use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context, Result};
use serde::Deserialize;
use walkdir::WalkDir;

use crate::config::{resolve_relative, Recipe, Stage, Step};

const ISO_TREE_DIR: &str = "iso_tree";
const ROOTFS_DIR: &str = "rootfs";
const CASPER_DIR: &str = "casper";
const INSTALL_SOURCES_FILE: &str = "install-sources.yaml";
const ROOT_MD5SUM_FILE: &str = "md5sum.txt";
const CASPER_SHA256SUMS_FILE: &str = "SHA256SUMS";

struct WorkspaceLayout {
    workspace_dir: PathBuf,
    iso_tree_dir: PathBuf,
    rootfs_dir: PathBuf,
}

#[derive(Debug, Deserialize)]
struct InstallSource {
    #[serde(default)]
    default: bool,
    path: String,
    #[serde(default)]
    variations: BTreeMap<String, InstallSourceVariation>,
}

#[derive(Debug, Deserialize)]
struct InstallSourceVariation {
    path: String,
}

struct MountGuard {
    targets: Vec<PathBuf>,
}

struct TempDir {
    path: PathBuf,
}

pub fn build(recipe_path: &Path, recipe: &Recipe) -> Result<()> {
    ensure_command_available("xorriso")?;
    ensure_command_available("unsquashfs")?;
    ensure_command_available("mksquashfs")?;
    ensure_command_available("bash")?;
    ensure_command_available("mount")?;
    ensure_command_available("umount")?;
    ensure_command_available("rsync")?;
    ensure_command_available("unmkinitramfs")?;
    ensure_command_available("md5sum")?;
    ensure_command_available("sha256sum")?;
    ensure_effective_root(
        "build command requires root for layered squashfs regeneration and verification",
    )?;
    let needs_rootfs_run = recipe
        .steps
        .iter()
        .any(|step| matches!(step.stage, Stage::Rootfs) && step.run.is_some());
    if needs_rootfs_run {
        ensure_command_available("chroot")?;
    }

    let layout = prepare_workspace(recipe_path, recipe, true)?;
    let build_result = (|| -> Result<()> {
        let squashfs_path = find_squashfs(&layout.iso_tree_dir)?;
        extract_rootfs(&squashfs_path, &layout.rootfs_dir)?;
        execute_steps(recipe_path, recipe, &layout)?;
        repack_rootfs(&layout.rootfs_dir, &squashfs_path)?;
        let layer_jobs = effective_layer_jobs(recipe.execution.layer_jobs);
        eprintln!("==> layer parallel jobs: {layer_jobs}");
        let layer_endpoints = regenerate_layers(&layout.iso_tree_dir, &squashfs_path, layer_jobs)?;
        regenerate_checksums(&layout.iso_tree_dir)?;
        let output_iso = recipe.output_iso_path(recipe_path)?;
        repack_iso(
            &recipe.base_iso_path(recipe_path),
            &layout.iso_tree_dir,
            &output_iso,
        )?;
        verify_iso_layer_synthesis(&output_iso, &layer_endpoints, layer_jobs)?;
        Ok(())
    })();
    let cleanup_result = cleanup_workspace_dir(&layout.workspace_dir);
    if let Err(build_error) = build_result {
        if let Err(cleanup_error) = cleanup_result {
            eprintln!(
                "warning: {cleanup_error:#}. workspace may remain at {}",
                layout.workspace_dir.display()
            );
        }
        return Err(build_error);
    }
    cleanup_result
}

pub fn shell(recipe_path: &Path, recipe: &Recipe) -> Result<()> {
    ensure_command_available("xorriso")?;
    ensure_command_available("unsquashfs")?;
    ensure_command_available("chroot")?;
    ensure_command_available("mount")?;
    ensure_command_available("umount")?;
    ensure_command_available("bash")?;
    ensure_effective_root("shell command requires chroot execution as root")?;

    let layout = prepare_workspace(recipe_path, recipe, false)?;
    if !dir_has_entries(&layout.iso_tree_dir)? {
        extract_iso(&recipe.base_iso_path(recipe_path), &layout.iso_tree_dir)?;
    }
    if !dir_has_entries(&layout.rootfs_dir)? {
        let squashfs_path = find_squashfs(&layout.iso_tree_dir)?;
        extract_rootfs(&squashfs_path, &layout.rootfs_dir)?;
    }

    let shell_path = find_chroot_shell(&layout.rootfs_dir)?;
    let _mounts = prepare_chroot_mounts(&layout.rootfs_dir)?;
    let status = Command::new("chroot")
        .arg(&layout.rootfs_dir)
        .arg(shell_path)
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()
        .context("failed to start chroot shell")?;
    if !status.success() {
        bail!("chroot shell exited with status {status}");
    }
    Ok(())
}

fn effective_layer_jobs(configured: usize) -> usize {
    if configured > 0 {
        return configured;
    }
    let cpu_count = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    auto_layer_jobs_for_cpus(cpu_count)
}

fn auto_layer_jobs_for_cpus(cpu_count: usize) -> usize {
    cpu_count.clamp(1, 16)
}

fn prepare_workspace(recipe_path: &Path, recipe: &Recipe, clean: bool) -> Result<WorkspaceLayout> {
    let workspace_dir = recipe.workspace_path(recipe_path);
    let iso_tree_dir = workspace_dir.join(ISO_TREE_DIR);
    let rootfs_dir = workspace_dir.join(ROOTFS_DIR);
    if clean {
        cleanup_workspace_dir(&workspace_dir)
            .with_context(|| format!("failed to reset {}", workspace_dir.display()))?;
    }
    fs::create_dir_all(&workspace_dir)
        .with_context(|| format!("failed to create {}", workspace_dir.display()))?;
    fs::create_dir_all(&iso_tree_dir)
        .with_context(|| format!("failed to create {}", iso_tree_dir.display()))?;
    fs::create_dir_all(&rootfs_dir)
        .with_context(|| format!("failed to create {}", rootfs_dir.display()))?;

    if clean {
        extract_iso(&recipe.base_iso_path(recipe_path), &iso_tree_dir)?;
    }

    Ok(WorkspaceLayout {
        workspace_dir,
        iso_tree_dir,
        rootfs_dir,
    })
}

fn cleanup_workspace_dir(workspace_dir: &Path) -> Result<()> {
    if !workspace_dir.exists() {
        return Ok(());
    }
    unmount_workspace_mounts(workspace_dir)?;
    fs::remove_dir_all(workspace_dir)
        .with_context(|| format!("failed to clean workspace {}", workspace_dir.display()))
}

fn unmount_workspace_mounts(workspace_dir: &Path) -> Result<()> {
    let mount_targets = workspace_mount_targets(workspace_dir)?;
    if mount_targets.is_empty() {
        return Ok(());
    }
    eprintln!(
        "==> unmount stale workspace mount(s): {}",
        mount_targets.len()
    );
    for target in mount_targets {
        let mut cmd = Command::new("umount");
        cmd.arg("-lf").arg(&target);
        run_command(
            cmd,
            &format!("unmount stale workspace mount {}", target.display()),
        )?;
    }
    Ok(())
}

fn workspace_mount_targets(workspace_dir: &Path) -> Result<Vec<PathBuf>> {
    let mount_root = workspace_mount_root(workspace_dir)?;
    let mountinfo = fs::read_to_string("/proc/self/mountinfo")
        .context("failed to read /proc/self/mountinfo for stale mount cleanup")?;
    Ok(workspace_mount_targets_from_mountinfo(
        &mount_root,
        &mountinfo,
    ))
}

fn workspace_mount_root(workspace_dir: &Path) -> Result<PathBuf> {
    if let Ok(canonical) = workspace_dir.canonicalize() {
        return Ok(canonical);
    }
    if workspace_dir.is_absolute() {
        return Ok(workspace_dir.to_path_buf());
    }
    let cwd = std::env::current_dir().context("failed to resolve current directory")?;
    Ok(cwd.join(workspace_dir))
}

fn workspace_mount_targets_from_mountinfo(mount_root: &Path, mountinfo: &str) -> Vec<PathBuf> {
    let mut targets = Vec::new();
    for line in mountinfo.lines() {
        let Some(raw_mount_point) = line.split_whitespace().nth(4) else {
            continue;
        };
        let mount_point = PathBuf::from(decode_mountinfo_path(raw_mount_point));
        if mount_point != mount_root && mount_point.starts_with(mount_root) {
            targets.push(mount_point);
        }
    }
    targets.sort_by(|a, b| {
        b.components()
            .count()
            .cmp(&a.components().count())
            .then(a.cmp(b))
    });
    targets.dedup();
    targets
}

fn decode_mountinfo_path(raw_mount_point: &str) -> String {
    let bytes = raw_mount_point.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0usize;
    while index < bytes.len() {
        if bytes[index] == b'\\'
            && index + 3 < bytes.len()
            && is_octal_digit(bytes[index + 1])
            && is_octal_digit(bytes[index + 2])
            && is_octal_digit(bytes[index + 3])
        {
            let value = (bytes[index + 1] - b'0') * 64
                + (bytes[index + 2] - b'0') * 8
                + (bytes[index + 3] - b'0');
            decoded.push(value);
            index += 4;
            continue;
        }
        decoded.push(bytes[index]);
        index += 1;
    }
    String::from_utf8_lossy(&decoded).into_owned()
}

fn is_octal_digit(value: u8) -> bool {
    (b'0'..=b'7').contains(&value)
}

fn extract_iso(base_iso: &Path, iso_tree_dir: &Path) -> Result<()> {
    let mut cmd = Command::new("xorriso");
    cmd.arg("-osirrox")
        .arg("on")
        .arg("-indev")
        .arg(base_iso)
        .arg("-extract")
        .arg("/")
        .arg(iso_tree_dir);
    run_command(cmd, "extract base ISO with xorriso")
}

fn find_squashfs(iso_tree_dir: &Path) -> Result<PathBuf> {
    if let Some(preferred) = squashfs_from_install_sources(iso_tree_dir)? {
        return Ok(preferred);
    }

    let mut candidates: Vec<(PathBuf, u8, u64)> = Vec::new();
    for entry in WalkDir::new(iso_tree_dir) {
        let entry = entry.with_context(|| {
            format!(
                "failed while scanning ISO tree {}",
                iso_tree_dir.to_string_lossy()
            )
        })?;
        if entry.file_type().is_file()
            && entry
                .path()
                .extension()
                .and_then(|e| e.to_str())
                .map(|e| e.eq_ignore_ascii_case("squashfs"))
                .unwrap_or(false)
        {
            let path = entry.path().to_path_buf();
            let lower = path.to_string_lossy().to_ascii_lowercase();
            let in_casper = lower.contains("/casper/");
            let is_live = lower.contains(".live.squashfs");
            let is_legacy_rootfs = lower.ends_with("filesystem.squashfs");
            let has_minimal_prefix = lower.contains("/casper/minimal");
            let priority = match (in_casper, is_live, is_legacy_rootfs) {
                (true, false, true) => 0,
                (true, false, _) if has_minimal_prefix => 1,
                (true, false, _) => 2,
                (true, true, _) => 3,
                (false, false, _) => 4,
                _ => 5,
            };
            let size = fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
            candidates.push((path, priority, size));
        }
    }

    if candidates.is_empty() {
        bail!("no *.squashfs file found under {}", iso_tree_dir.display());
    }

    candidates.sort_by(|a, b| a.1.cmp(&b.1).then_with(|| b.2.cmp(&a.2)));
    Ok(candidates.remove(0).0)
}

fn squashfs_from_install_sources(iso_tree_dir: &Path) -> Result<Option<PathBuf>> {
    let casper_dir = iso_tree_dir.join(CASPER_DIR);
    let sources = read_install_sources(&casper_dir)?;
    if sources.is_empty() {
        return Ok(None);
    }

    let selected = sources
        .iter()
        .find(|source| source.default)
        .or_else(|| sources.first())
        .map(|source| source.path.clone());

    let Some(selected) = selected else {
        return Ok(None);
    };
    let candidate = casper_dir.join(&selected);
    if candidate.is_file() {
        return Ok(Some(candidate));
    }

    let selected_name = Path::new(&selected)
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or(selected.as_str());
    for entry in WalkDir::new(&casper_dir) {
        let entry = entry.with_context(|| {
            format!(
                "failed while scanning install source candidates in {}",
                casper_dir.display()
            )
        })?;
        if entry.file_type().is_file()
            && entry
                .path()
                .file_name()
                .and_then(|n| n.to_str())
                .map(|n| n == selected_name)
                .unwrap_or(false)
        {
            return Ok(Some(entry.path().to_path_buf()));
        }
    }

    bail!(
        "install-sources.yaml selected {}, but matching squashfs was not found under {}",
        selected,
        casper_dir.display()
    )
}

fn read_install_sources(casper_dir: &Path) -> Result<Vec<InstallSource>> {
    let install_sources_path = casper_dir.join(INSTALL_SOURCES_FILE);
    if !install_sources_path.is_file() {
        return Ok(Vec::new());
    }
    let content = fs::read_to_string(&install_sources_path)
        .with_context(|| format!("failed to read {}", install_sources_path.display()))?;
    let sources: Vec<InstallSource> = serde_yaml_ng::from_str(&content).with_context(|| {
        format!(
            "failed to parse install sources YAML {}",
            install_sources_path.display()
        )
    })?;
    Ok(sources)
}

fn extract_rootfs(squashfs_path: &Path, rootfs_dir: &Path) -> Result<()> {
    let mut cmd = Command::new("unsquashfs");
    cmd.arg("-f").arg("-d").arg(rootfs_dir).arg(squashfs_path);
    run_command(cmd, "extract rootfs with unsquashfs")
}

fn repack_rootfs(rootfs_dir: &Path, squashfs_path: &Path) -> Result<()> {
    if squashfs_path.exists() {
        fs::remove_file(squashfs_path)
            .with_context(|| format!("failed to replace {}", squashfs_path.display()))?;
    }
    let mut cmd = Command::new("mksquashfs");
    cmd.arg(rootfs_dir)
        .arg(squashfs_path)
        .arg("-noappend")
        .arg("-comp")
        .arg("xz");
    run_command(cmd, "repack rootfs with mksquashfs")
}

fn repack_iso(base_iso: &Path, iso_tree_dir: &Path, output_iso: &Path) -> Result<()> {
    if let Some(parent) = output_iso.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    if output_iso.exists() {
        fs::remove_file(output_iso)
            .with_context(|| format!("failed to replace {}", output_iso.display()))?;
    }

    let mut cmd = Command::new("xorriso");
    cmd.arg("-indev")
        .arg(base_iso)
        .arg("-outdev")
        .arg(output_iso)
        .arg("-boot_image")
        .arg("any")
        .arg("replay")
        .arg("-update_r")
        .arg(iso_tree_dir)
        .arg("/")
        .arg("-padding")
        .arg("0")
        .arg("-commit")
        .arg("-end");
    run_command(cmd, "repack ISO with xorriso replay mode")
}

fn regenerate_layers(
    iso_tree_dir: &Path,
    selected_squashfs: &Path,
    layer_jobs: usize,
) -> Result<Vec<PathBuf>> {
    let endpoints = discover_layer_endpoints(iso_tree_dir, selected_squashfs)?;
    let casper_dir = iso_tree_dir.join(CASPER_DIR);
    let selected_rel = selected_squashfs
        .strip_prefix(&casper_dir)
        .with_context(|| {
            format!(
                "failed to derive casper relative path from {}",
                selected_squashfs.display()
            )
        })?
        .to_path_buf();

    let targets: Vec<PathBuf> = endpoints
        .iter()
        .filter(|endpoint| *endpoint != &selected_rel)
        .cloned()
        .collect();
    if targets.is_empty() {
        return Ok(endpoints);
    }

    let jobs = layer_jobs.max(1);
    let levels = layer_regeneration_levels(&targets)?;
    for level in levels {
        if level.len() == 1 || jobs == 1 {
            for endpoint in level {
                eprintln!("==> regenerate layer {}", endpoint.display());
                regenerate_layer_endpoint(&casper_dir, &endpoint)?;
            }
            continue;
        }
        eprintln!(
            "==> regenerate layer batch (parallel={}) {} item(s)",
            jobs,
            level.len()
        );
        regenerate_layers_parallel_level(&casper_dir, &level, jobs)?;
    }

    Ok(endpoints)
}

fn layer_regeneration_levels(targets: &[PathBuf]) -> Result<Vec<Vec<PathBuf>>> {
    let target_set: BTreeSet<PathBuf> = targets.iter().cloned().collect();
    let mut indegree = BTreeMap::<PathBuf, usize>::new();
    let mut dependents = BTreeMap::<PathBuf, Vec<PathBuf>>::new();

    for target in targets {
        let mut deps = BTreeSet::new();
        for layer in layer_chain_rel_paths(target)? {
            if layer != *target && target_set.contains(&layer) {
                deps.insert(layer.clone());
                dependents.entry(layer).or_default().push(target.clone());
            }
        }
        indegree.insert(target.clone(), deps.len());
    }

    let mut remaining = target_set;
    let mut levels = Vec::new();
    while !remaining.is_empty() {
        let mut ready: Vec<PathBuf> = remaining
            .iter()
            .filter(|layer| indegree.get(*layer).copied().unwrap_or(0) == 0)
            .cloned()
            .collect();
        if ready.is_empty() {
            bail!("failed to resolve layer regeneration order due to cyclic dependencies");
        }
        ready.sort();
        for layer in &ready {
            remaining.remove(layer);
            if let Some(children) = dependents.get(layer) {
                for child in children {
                    if let Some(count) = indegree.get_mut(child) {
                        *count = count.saturating_sub(1);
                    }
                }
            }
        }
        levels.push(ready);
    }

    Ok(levels)
}

fn regenerate_layers_parallel_level(
    casper_dir: &Path,
    level: &[PathBuf],
    jobs: usize,
) -> Result<()> {
    for chunk in level.chunks(jobs) {
        thread::scope(|scope| -> Result<()> {
            let mut handles = Vec::with_capacity(chunk.len());
            for endpoint in chunk {
                let endpoint = endpoint.clone();
                let casper_dir = casper_dir.to_path_buf();
                handles.push(scope.spawn(move || -> Result<()> {
                    eprintln!("==> regenerate layer {}", endpoint.display());
                    regenerate_layer_endpoint(&casper_dir, &endpoint)
                }));
            }
            for handle in handles {
                let outcome = handle
                    .join()
                    .map_err(|_| anyhow::anyhow!("layer regeneration worker thread panicked"))?;
                outcome?;
            }
            Ok(())
        })?;
    }
    Ok(())
}

fn discover_layer_endpoints(iso_tree_dir: &Path, selected_squashfs: &Path) -> Result<Vec<PathBuf>> {
    let casper_dir = iso_tree_dir.join(CASPER_DIR);
    let selected_rel = selected_squashfs
        .strip_prefix(&casper_dir)
        .with_context(|| {
            format!(
                "failed to derive casper relative path from {}",
                selected_squashfs.display()
            )
        })?
        .to_path_buf();

    let mut endpoints = BTreeSet::new();
    endpoints.insert(selected_rel.clone());

    for path in install_source_layer_paths(&casper_dir)? {
        if !casper_dir.join(&path).is_file() {
            continue;
        }
        if is_same_or_dependent_layer(&selected_rel, &path)? {
            endpoints.insert(path);
        }
    }

    if let Some(default_live_layer) = default_live_layer_path(iso_tree_dir)? {
        if casper_dir.join(&default_live_layer).is_file()
            && is_same_or_dependent_layer(&selected_rel, &default_live_layer)?
        {
            endpoints.insert(default_live_layer);
        }
    }

    let mut sorted: Vec<PathBuf> = endpoints.into_iter().collect();
    sorted.sort_by(|a, b| {
        layer_chain_rel_paths(a)
            .map(|c| c.len())
            .unwrap_or(usize::MAX)
            .cmp(
                &layer_chain_rel_paths(b)
                    .map(|c| c.len())
                    .unwrap_or(usize::MAX),
            )
            .then(a.cmp(b))
    });
    Ok(sorted)
}

fn install_source_layer_paths(casper_dir: &Path) -> Result<Vec<PathBuf>> {
    let sources = read_install_sources(casper_dir)?;
    let mut paths = BTreeSet::new();
    for source in sources {
        paths.insert(PathBuf::from(source.path));
        for variation in source.variations.values() {
            paths.insert(PathBuf::from(variation.path.clone()));
        }
    }
    Ok(paths.into_iter().collect())
}

fn default_live_layer_path(iso_tree_dir: &Path) -> Result<Option<PathBuf>> {
    let initrd_path = iso_tree_dir.join(CASPER_DIR).join("initrd");
    if !initrd_path.is_file() {
        return Ok(None);
    }

    let unpack_dir = create_temp_dir("initrd-inspect")?;
    let mut cmd = Command::new("unmkinitramfs");
    cmd.arg(&initrd_path).arg(unpack_dir.path());
    run_command(
        cmd,
        "extract initrd with unmkinitramfs for layered path discovery",
    )?;

    let default_layer_conf = unpack_dir
        .path()
        .join("main")
        .join("conf/conf.d/default-layer.conf");
    if !default_layer_conf.is_file() {
        return Ok(None);
    }

    let content = fs::read_to_string(&default_layer_conf)
        .with_context(|| format!("failed to read {}", default_layer_conf.display()))?;
    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some(raw_value) = line.strip_prefix("LAYERFS_PATH=") {
            let value = raw_value.trim().trim_matches('"').trim_matches('\'');
            if !value.is_empty() {
                return Ok(Some(PathBuf::from(value)));
            }
        }
    }
    Ok(None)
}

fn is_same_or_dependent_layer(base: &Path, target: &Path) -> Result<bool> {
    if base == target {
        return Ok(true);
    }
    Ok(layer_chain_rel_paths(target)?
        .iter()
        .any(|part| part == base))
}

fn layer_chain_rel_paths(layer: &Path) -> Result<Vec<PathBuf>> {
    let extension = layer
        .extension()
        .and_then(|s| s.to_str())
        .ok_or_else(|| anyhow::anyhow!("invalid layer path: {}", layer.display()))?;
    if !extension.eq_ignore_ascii_case("squashfs") {
        bail!("layer path is not squashfs: {}", layer.display());
    }

    let stem = layer
        .file_stem()
        .and_then(|s| s.to_str())
        .ok_or_else(|| anyhow::anyhow!("invalid layer filename: {}", layer.display()))?;
    let parts: Vec<&str> = stem.split('.').collect();
    if parts.is_empty() || parts.iter().any(|part| part.is_empty()) {
        bail!("invalid dotted layer filename: {}", layer.display());
    }

    let parent = layer.parent().unwrap_or_else(|| Path::new(""));
    let mut chain = Vec::with_capacity(parts.len());
    for index in 0..parts.len() {
        let prefix = parts[..=index].join(".");
        let name = format!("{prefix}.{extension}");
        if parent.as_os_str().is_empty() {
            chain.push(PathBuf::from(name));
        } else {
            chain.push(parent.join(name));
        }
    }
    Ok(chain)
}

fn regenerate_layer_endpoint(casper_dir: &Path, endpoint_rel: &Path) -> Result<()> {
    let chain_rel = layer_chain_rel_paths(endpoint_rel)?;
    if chain_rel.len() < 2 {
        return Ok(());
    }

    let chain_abs: Vec<PathBuf> = chain_rel.iter().map(|rel| casper_dir.join(rel)).collect();
    for layer in &chain_abs {
        if !layer.is_file() {
            bail!("required layer file missing: {}", layer.display());
        }
    }

    let lower_chain = &chain_abs[..chain_abs.len() - 1];
    let endpoint_abs = chain_abs.last().cloned().ok_or_else(|| {
        anyhow::anyhow!(
            "unexpected empty chain while regenerating {}",
            endpoint_rel.display()
        )
    })?;

    let work_dir = create_temp_dir("layer-regenerate")?;
    let lower_dir = work_dir.path().join("lower");
    let desired_dir = work_dir.path().join("desired");
    let upper_dir = work_dir.path().join("upper");
    let overlay_work_dir = work_dir.path().join("overlay-work");
    let merged_dir = work_dir.path().join("merged");

    synthesize_layer_chain_to_dir(lower_chain, &lower_dir)?;
    synthesize_layer_chain_to_dir(&chain_abs, &desired_dir)?;
    generate_overlay_delta(
        &lower_dir,
        &desired_dir,
        &upper_dir,
        &overlay_work_dir,
        &merged_dir,
    )?;
    repack_rootfs(&upper_dir, &endpoint_abs)
}

fn synthesize_layer_chain_to_dir(layer_chain: &[PathBuf], output_dir: &Path) -> Result<()> {
    if layer_chain.is_empty() {
        bail!("cannot synthesize an empty layer chain");
    }
    if output_dir.exists() {
        fs::remove_dir_all(output_dir)
            .with_context(|| format!("failed to reset {}", output_dir.display()))?;
    }
    fs::create_dir_all(output_dir)
        .with_context(|| format!("failed to create {}", output_dir.display()))?;

    with_synthesized_layer_mount(layer_chain, |merged_mount| {
        rsync_tree(merged_mount, output_dir, true)
    })
}

fn with_synthesized_layer_mount<F>(layer_chain: &[PathBuf], action: F) -> Result<()>
where
    F: FnOnce(&Path) -> Result<()>,
{
    if layer_chain.is_empty() {
        bail!("cannot synthesize an empty layer chain");
    }
    let compose_dir = create_temp_dir("layer-compose")?;
    let layers_mount_dir = compose_dir.path().join("layers");
    fs::create_dir_all(&layers_mount_dir)
        .with_context(|| format!("failed to create {}", layers_mount_dir.display()))?;

    let mut mount_guard = MountGuard {
        targets: Vec::new(),
    };
    let mut mounted_layers = Vec::with_capacity(layer_chain.len());
    for (index, layer_file) in layer_chain.iter().enumerate() {
        let mount_target = layers_mount_dir.join(index.to_string());
        fs::create_dir_all(&mount_target)
            .with_context(|| format!("failed to create {}", mount_target.display()))?;
        let mut cmd = Command::new("mount");
        cmd.arg("-t")
            .arg("squashfs")
            .arg("-o")
            .arg("loop,ro")
            .arg(layer_file)
            .arg(&mount_target);
        run_command(
            cmd,
            &format!("mount squashfs layer {}", layer_file.display()),
        )?;
        mount_guard.targets.push(mount_target.clone());
        mounted_layers.push(mount_target);
    }

    let merged_mount = compose_dir.path().join("merged");
    fs::create_dir_all(&merged_mount)
        .with_context(|| format!("failed to create {}", merged_mount.display()))?;
    let overlay_upper = compose_dir.path().join("overlay-upper");
    let overlay_work = compose_dir.path().join("overlay-work");
    fs::create_dir_all(&overlay_upper)
        .with_context(|| format!("failed to create {}", overlay_upper.display()))?;
    fs::create_dir_all(&overlay_work)
        .with_context(|| format!("failed to create {}", overlay_work.display()))?;
    let lowerdir = mounted_layers
        .iter()
        .rev()
        .map(|path| path.to_string_lossy().to_string())
        .collect::<Vec<_>>()
        .join(":");
    let mut overlay_cmd = Command::new("mount");
    overlay_cmd
        .arg("-t")
        .arg("overlay")
        .arg("overlay")
        .arg("-o")
        .arg(format!(
            "lowerdir={lowerdir},upperdir={},workdir={}",
            overlay_upper.display(),
            overlay_work.display()
        ))
        .arg(&merged_mount);
    run_command(overlay_cmd, "mount overlay for layer synthesis")?;
    mount_guard.targets.push(merged_mount.clone());

    action(&merged_mount)
}

fn generate_overlay_delta(
    lower_dir: &Path,
    desired_dir: &Path,
    upper_dir: &Path,
    overlay_work_dir: &Path,
    merged_dir: &Path,
) -> Result<()> {
    if upper_dir.exists() {
        fs::remove_dir_all(upper_dir)
            .with_context(|| format!("failed to reset {}", upper_dir.display()))?;
    }
    if overlay_work_dir.exists() {
        fs::remove_dir_all(overlay_work_dir)
            .with_context(|| format!("failed to reset {}", overlay_work_dir.display()))?;
    }
    if merged_dir.exists() {
        fs::remove_dir_all(merged_dir)
            .with_context(|| format!("failed to reset {}", merged_dir.display()))?;
    }

    fs::create_dir_all(upper_dir)
        .with_context(|| format!("failed to create {}", upper_dir.display()))?;
    fs::create_dir_all(overlay_work_dir)
        .with_context(|| format!("failed to create {}", overlay_work_dir.display()))?;
    fs::create_dir_all(merged_dir)
        .with_context(|| format!("failed to create {}", merged_dir.display()))?;

    let mut mount_guard = MountGuard {
        targets: Vec::new(),
    };
    let mut overlay_cmd = Command::new("mount");
    overlay_cmd
        .arg("-t")
        .arg("overlay")
        .arg("overlay")
        .arg("-o")
        .arg(format!(
            "lowerdir={},upperdir={},workdir={}",
            lower_dir.display(),
            upper_dir.display(),
            overlay_work_dir.display()
        ))
        .arg(merged_dir);
    run_command(overlay_cmd, "mount writable overlay for delta generation")?;
    mount_guard.targets.push(merged_dir.to_path_buf());

    rsync_tree(desired_dir, merged_dir, true)
}

fn rsync_tree(source_dir: &Path, target_dir: &Path, delete: bool) -> Result<()> {
    fs::create_dir_all(target_dir)
        .with_context(|| format!("failed to create {}", target_dir.display()))?;
    let mut cmd = Command::new("rsync");
    cmd.arg("-aHAX").arg("--numeric-ids");
    if delete {
        cmd.arg("--delete");
    }
    cmd.arg(format!("{}/", source_dir.display()))
        .arg(format!("{}/", target_dir.display()));
    run_command(
        cmd,
        &format!(
            "rsync directory tree {} -> {}",
            source_dir.display(),
            target_dir.display()
        ),
    )
}

fn regenerate_checksums(iso_tree_dir: &Path) -> Result<()> {
    eprintln!("==> regenerate checksums");
    regenerate_casper_sha256sums(&iso_tree_dir.join(CASPER_DIR))?;
    regenerate_root_md5sum(iso_tree_dir)
}

fn regenerate_root_md5sum(iso_tree_dir: &Path) -> Result<()> {
    let mut files = Vec::new();
    for entry in WalkDir::new(iso_tree_dir) {
        let entry = entry.with_context(|| {
            format!(
                "failed while scanning files under {}",
                iso_tree_dir.display()
            )
        })?;
        if !entry.file_type().is_file() {
            continue;
        }
        let rel = entry.path().strip_prefix(iso_tree_dir).with_context(|| {
            format!(
                "failed to compute relative path for {}",
                entry.path().display()
            )
        })?;
        if rel == Path::new(ROOT_MD5SUM_FILE) {
            continue;
        }
        files.push(rel.to_path_buf());
    }
    files.sort();

    let mut content = String::new();
    for rel in files {
        let digest = digest_file_with_command("md5sum", &iso_tree_dir.join(&rel))?;
        content.push_str(&format!("{digest}  ./{}\n", to_iso_path(&rel)));
    }
    let md5sum_path = iso_tree_dir.join(ROOT_MD5SUM_FILE);
    fs::write(&md5sum_path, content)
        .with_context(|| format!("failed to write {}", md5sum_path.display()))
}

fn regenerate_casper_sha256sums(casper_dir: &Path) -> Result<()> {
    let mut squashfs_files = Vec::new();
    for entry in fs::read_dir(casper_dir)
        .with_context(|| format!("failed to read {}", casper_dir.display()))?
    {
        let entry = entry.with_context(|| {
            format!(
                "failed while scanning casper directory {}",
                casper_dir.display()
            )
        })?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let is_squashfs = path
            .extension()
            .and_then(|s| s.to_str())
            .map(|ext| ext.eq_ignore_ascii_case("squashfs"))
            .unwrap_or(false);
        if is_squashfs {
            squashfs_files.push(path);
        }
    }
    squashfs_files.sort();

    let mut content = String::new();
    for path in squashfs_files {
        let digest = digest_file_with_command("sha256sum", &path)?;
        let file_name = path
            .file_name()
            .and_then(|s| s.to_str())
            .ok_or_else(|| anyhow::anyhow!("invalid squashfs filename {}", path.display()))?;
        content.push_str(&format!("{digest} *{file_name}\n"));
    }
    let sha_path = casper_dir.join(CASPER_SHA256SUMS_FILE);
    fs::write(&sha_path, content).with_context(|| format!("failed to write {}", sha_path.display()))
}

fn digest_file_with_command(command: &str, file: &Path) -> Result<String> {
    let output = Command::new(command)
        .arg(file)
        .output()
        .with_context(|| format!("failed to start {command} for {}", file.display()))?;
    if !output.status.success() {
        bail!("{command} failed for {}", file.display());
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    let digest = stdout
        .split_whitespace()
        .next()
        .ok_or_else(|| anyhow::anyhow!("unexpected {command} output for {}", file.display()))?;
    Ok(digest.to_string())
}

fn verify_iso_layer_synthesis(
    output_iso: &Path,
    endpoints: &[PathBuf],
    layer_jobs: usize,
) -> Result<()> {
    if endpoints.is_empty() {
        return Ok(());
    }
    eprintln!("==> verify layered synthesis in output ISO");

    let verify_dir = create_temp_dir("layer-verify")?;
    let casper_extract_dir = verify_dir.path().join(CASPER_DIR);
    fs::create_dir_all(&casper_extract_dir)
        .with_context(|| format!("failed to create {}", casper_extract_dir.display()))?;

    let mut required_layers = BTreeSet::new();
    for endpoint in endpoints {
        for layer in layer_chain_rel_paths(endpoint)? {
            required_layers.insert(layer);
        }
    }

    if !required_layers.is_empty() {
        let mut extract_cmd = Command::new("xorriso");
        extract_cmd
            .arg("-osirrox")
            .arg("on")
            .arg("-indev")
            .arg(output_iso);
        for layer_rel in &required_layers {
            let source = format!("/casper/{}", to_iso_path(layer_rel));
            let destination = casper_extract_dir.join(layer_rel);
            if let Some(parent) = destination.parent() {
                fs::create_dir_all(parent)
                    .with_context(|| format!("failed to create {}", parent.display()))?;
            }
            extract_cmd.arg("-extract").arg(source).arg(destination);
        }
        run_command(
            extract_cmd,
            "extract layered squashfs files from output ISO",
        )?;
    }

    let jobs = layer_jobs.max(1);
    let levels = layer_regeneration_levels(endpoints)?;
    for level in levels {
        if level.len() == 1 || jobs == 1 {
            for endpoint in level {
                verify_layer_endpoint(&casper_extract_dir, &endpoint)?;
            }
            continue;
        }
        eprintln!(
            "==> verify layer synthesis batch (parallel={}) {} item(s)",
            jobs,
            level.len()
        );
        verify_layers_parallel_level(&casper_extract_dir, &level, jobs)?;
    }
    Ok(())
}

fn verify_layer_endpoint(casper_extract_dir: &Path, endpoint: &Path) -> Result<()> {
    eprintln!("==> verify layer {}", endpoint.display());
    let chain_rel = layer_chain_rel_paths(endpoint)?;
    let chain_abs: Vec<PathBuf> = chain_rel
        .iter()
        .map(|layer_rel| casper_extract_dir.join(layer_rel))
        .collect();
    for layer in &chain_abs {
        if !layer.is_file() {
            bail!(
                "layer synthesis verification failed: missing extracted file {}",
                layer.display()
            );
        }
    }
    with_synthesized_layer_mount(&chain_abs, |merged_mount| {
        if !merged_mount.join("etc/os-release").is_file() {
            bail!(
                "layer synthesis verification failed for {}: etc/os-release not found",
                endpoint.display()
            );
        }
        Ok(())
    })
}

fn verify_layers_parallel_level(
    casper_extract_dir: &Path,
    level: &[PathBuf],
    jobs: usize,
) -> Result<()> {
    for chunk in level.chunks(jobs) {
        thread::scope(|scope| -> Result<()> {
            let mut handles = Vec::with_capacity(chunk.len());
            for endpoint in chunk {
                let endpoint = endpoint.clone();
                let casper_extract_dir = casper_extract_dir.to_path_buf();
                handles.push(scope.spawn(move || -> Result<()> {
                    verify_layer_endpoint(&casper_extract_dir, &endpoint)
                }));
            }
            for handle in handles {
                let outcome = handle
                    .join()
                    .map_err(|_| anyhow::anyhow!("layer verification worker thread panicked"))?;
                outcome?;
            }
            Ok(())
        })?;
    }
    Ok(())
}

fn to_iso_path(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

fn execute_steps(recipe_path: &Path, recipe: &Recipe, layout: &WorkspaceLayout) -> Result<()> {
    let recipe_dir = Recipe::recipe_dir(recipe_path);
    for (index, step) in recipe.steps.iter().enumerate() {
        let label = step
            .name
            .as_deref()
            .map(str::to_string)
            .unwrap_or_else(|| format!("step {}", index + 1));
        eprintln!("==> {}", label);

        match step.stage {
            Stage::Rootfs => execute_rootfs_step(recipe_dir, layout, step)?,
            Stage::Iso => execute_iso_step(recipe_dir, layout, step)?,
            Stage::Host => execute_host_step(recipe_dir, step)?,
        }
    }
    Ok(())
}

fn execute_rootfs_step(recipe_dir: &Path, layout: &WorkspaceLayout, step: &Step) -> Result<()> {
    if let Some(copy) = &step.copy {
        let from = resolve_relative(recipe_dir, &copy.from);
        let to = copy.to.strip_prefix("/").unwrap_or(&copy.to);
        copy_path(&from, &layout.rootfs_dir.join(to))?;
    }
    if let Some(run) = &step.run {
        let shell_path = find_chroot_shell(&layout.rootfs_dir)?;
        let _mounts = prepare_chroot_mounts(&layout.rootfs_dir)?;
        let command = wrap_with_workdir(run, step.workdir.as_deref(), true);
        let strict_script = strict_shell_script(shell_path, &command);
        let mut cmd = Command::new("chroot");
        cmd.arg(&layout.rootfs_dir)
            .arg(shell_path)
            .arg("-lc")
            .arg(strict_script);
        cmd.envs(&step.env);
        run_command(cmd, "execute rootfs run step")?;
    }
    Ok(())
}

fn execute_iso_step(recipe_dir: &Path, layout: &WorkspaceLayout, step: &Step) -> Result<()> {
    if let Some(copy) = &step.copy {
        let from = resolve_relative(recipe_dir, &copy.from);
        let to = copy.to.strip_prefix("/").unwrap_or(&copy.to);
        copy_path(&from, &layout.iso_tree_dir.join(to))?;
    }
    if let Some(run) = &step.run {
        let mut cmd = Command::new("bash");
        let strict_script = strict_shell_script("/bin/bash", run);
        cmd.arg("-lc").arg(strict_script);
        if let Some(workdir) = &step.workdir {
            let cwd = if workdir.is_absolute() {
                workdir.clone()
            } else {
                layout.iso_tree_dir.join(workdir)
            };
            cmd.current_dir(cwd);
        } else {
            cmd.current_dir(&layout.iso_tree_dir);
        }
        cmd.envs(&step.env);
        run_command(cmd, "execute iso run step")?;
    }
    Ok(())
}

fn execute_host_step(recipe_dir: &Path, step: &Step) -> Result<()> {
    if let Some(copy) = &step.copy {
        let from = resolve_relative(recipe_dir, &copy.from);
        let to = if copy.to.is_absolute() {
            copy.to.clone()
        } else {
            recipe_dir.join(&copy.to)
        };
        copy_path(&from, &to)?;
    }
    if let Some(run) = &step.run {
        let mut cmd = Command::new("bash");
        let strict_script = strict_shell_script("/bin/bash", run);
        cmd.arg("-lc").arg(strict_script);
        if let Some(workdir) = &step.workdir {
            let cwd = if workdir.is_absolute() {
                workdir.clone()
            } else {
                recipe_dir.join(workdir)
            };
            cmd.current_dir(cwd);
        } else {
            cmd.current_dir(recipe_dir);
        }
        cmd.envs(&step.env);
        run_command(cmd, "execute host run step")?;
    }
    Ok(())
}

fn wrap_with_workdir(command: &str, workdir: Option<&Path>, rootfs: bool) -> String {
    let Some(workdir) = workdir else {
        return command.to_string();
    };
    let workdir = if rootfs {
        if workdir.is_absolute() {
            workdir.to_path_buf()
        } else {
            Path::new("/").join(workdir)
        }
    } else {
        workdir.to_path_buf()
    };
    format!(
        "cd '{}' && {}",
        shell_single_quote_escape(&workdir.to_string_lossy()),
        command
    )
}

fn shell_single_quote_escape(input: &str) -> String {
    input.replace('\'', "'\"'\"'")
}

fn strict_shell_script(shell_path: &str, command: &str) -> String {
    if shell_path.contains("bash") {
        format!("set -euo pipefail\n{command}")
    } else {
        format!("set -eu\n{command}")
    }
}

fn copy_path(from: &Path, to: &Path) -> Result<()> {
    if from.is_file() {
        let final_target = if to.is_dir() {
            let filename = from
                .file_name()
                .ok_or_else(|| anyhow::anyhow!("invalid source filename for {}", from.display()))?;
            to.join(filename)
        } else {
            to.to_path_buf()
        };
        if let Some(parent) = final_target.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
        fs::copy(from, &final_target).with_context(|| {
            format!(
                "failed to copy file {} -> {}",
                from.display(),
                final_target.display()
            )
        })?;
        return Ok(());
    }

    if from.is_dir() {
        fs::create_dir_all(to).with_context(|| format!("failed to create {}", to.display()))?;
        for entry in WalkDir::new(from) {
            let entry = entry.with_context(|| format!("failed to walk {}", from.display()))?;
            let source = entry.path();
            let rel = source.strip_prefix(from).with_context(|| {
                format!("failed to compute relative path for {}", source.display())
            })?;
            if rel.as_os_str().is_empty() {
                continue;
            }
            let target = to.join(rel);
            if entry.file_type().is_dir() {
                fs::create_dir_all(&target)
                    .with_context(|| format!("failed to create {}", target.display()))?;
            } else if entry.file_type().is_file() {
                if let Some(parent) = target.parent() {
                    fs::create_dir_all(parent)
                        .with_context(|| format!("failed to create {}", parent.display()))?;
                }
                fs::copy(source, &target).with_context(|| {
                    format!(
                        "failed to copy file {} -> {}",
                        source.display(),
                        target.display()
                    )
                })?;
            } else if entry.file_type().is_symlink() {
                let link_target = fs::read_link(source)
                    .with_context(|| format!("failed to read symlink {}", source.display()))?;
                create_symlink(&link_target, &target)?;
            }
        }
        return Ok(());
    }

    bail!("copy source does not exist: {}", from.display())
}

#[cfg(unix)]
fn create_symlink(target: &Path, link: &Path) -> Result<()> {
    use std::os::unix::fs::symlink;
    if let Some(parent) = link.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    if link.exists() || link.is_symlink() {
        fs::remove_file(link).with_context(|| format!("failed to replace {}", link.display()))?;
    }
    symlink(target, link).with_context(|| {
        format!(
            "failed to create symlink {} -> {}",
            link.display(),
            target.display()
        )
    })?;
    Ok(())
}

#[cfg(not(unix))]
fn create_symlink(target: &Path, link: &Path) -> Result<()> {
    let _ = target;
    let _ = link;
    bail!("copying symlinks is not supported on this platform")
}

impl TempDir {
    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

fn create_temp_dir(prefix: &str) -> Result<TempDir> {
    let base = std::env::temp_dir();
    let seed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    for attempt in 0..1000u32 {
        let candidate = base.join(format!(
            "frozenos-builder-{prefix}-{}-{seed}-{attempt}",
            std::process::id()
        ));
        match fs::create_dir(&candidate) {
            Ok(_) => return Ok(TempDir { path: candidate }),
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(err) => {
                return Err(err).with_context(|| {
                    format!(
                        "failed to create temporary directory {}",
                        candidate.display()
                    )
                });
            }
        }
    }
    bail!("failed to allocate temporary directory after many attempts")
}

impl Drop for MountGuard {
    fn drop(&mut self) {
        for target in self.targets.iter().rev() {
            let _ = Command::new("umount")
                .arg("-lf")
                .arg(target)
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
        }
    }
}

fn prepare_chroot_mounts(rootfs_dir: &Path) -> Result<MountGuard> {
    let mount_pairs = [
        ("/dev", "dev"),
        ("/dev/pts", "dev/pts"),
        ("/proc", "proc"),
        ("/sys", "sys"),
        ("/run", "run"),
    ];
    let mut mounted_targets = Vec::with_capacity(mount_pairs.len());
    for (source, rel_target) in mount_pairs {
        let target = rootfs_dir.join(rel_target);
        fs::create_dir_all(&target)
            .with_context(|| format!("failed to create {}", target.display()))?;
        let mut cmd = Command::new("mount");
        cmd.arg("--bind").arg(source).arg(&target);
        run_command(cmd, &format!("bind mount {source} into chroot"))?;
        mounted_targets.push(target);
    }
    Ok(MountGuard {
        targets: mounted_targets,
    })
}

fn find_chroot_shell(rootfs_dir: &Path) -> Result<&'static str> {
    let shells = ["/bin/bash", "/usr/bin/bash", "/bin/sh", "/usr/bin/sh"];
    for shell in shells {
        let relative = shell.strip_prefix('/').unwrap_or(shell);
        if rootfs_dir.join(relative).exists() {
            return Ok(shell);
        }
    }
    bail!(
        "no usable shell found in extracted rootfs under {}",
        rootfs_dir.display()
    )
}

fn run_command(mut cmd: Command, description: &str) -> Result<()> {
    let status = cmd
        .status()
        .with_context(|| format!("failed to start command for {description}"))?;
    if !status.success() {
        bail!("{description} failed with status {status}");
    }
    Ok(())
}

fn ensure_command_available(command: &str) -> Result<()> {
    if is_command_available(command) {
        return Ok(());
    }
    bail!("required command not found in PATH: {command}")
}

fn is_command_available(command: &str) -> bool {
    if command.contains('/') {
        return Path::new(command).is_file();
    }
    let Some(path) = std::env::var_os("PATH") else {
        return false;
    };
    for dir in std::env::split_paths(&path) {
        let candidate = dir.join(command);
        if candidate.is_file() {
            return true;
        }
    }
    false
}

fn ensure_effective_root(reason: &str) -> Result<()> {
    if is_effective_root()? {
        return Ok(());
    }
    bail!("{reason}. Run this command with sudo.")
}

fn is_effective_root() -> Result<bool> {
    let output = Command::new("id")
        .arg("-u")
        .output()
        .context("failed to check effective uid with id -u")?;
    if !output.status.success() {
        bail!("failed to check effective uid with id -u");
    }
    let uid = String::from_utf8_lossy(&output.stdout).trim().to_string();
    Ok(uid == "0")
}

fn dir_has_entries(path: &Path) -> Result<bool> {
    let mut iter =
        fs::read_dir(path).with_context(|| format!("failed to read {}", path.display()))?;
    Ok(iter.next().is_some())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn find_squashfs_prefers_default_install_source() {
        let dir = tempdir().unwrap();
        let casper = dir.path().join(CASPER_DIR);
        fs::create_dir_all(&casper).unwrap();
        fs::write(
            casper.join(INSTALL_SOURCES_FILE),
            r#"
- default: true
  path: minimal.squashfs
- default: false
  path: minimal.standard.squashfs
"#,
        )
        .unwrap();
        fs::write(casper.join("minimal.squashfs"), b"a").unwrap();
        fs::write(casper.join("minimal.standard.squashfs"), b"b").unwrap();

        let selected = find_squashfs(dir.path()).unwrap();
        assert_eq!(selected, casper.join("minimal.squashfs"));
    }

    #[test]
    fn find_chroot_shell_falls_back_to_usr_bin_bash() {
        let dir = tempdir().unwrap();
        let usr_bin = dir.path().join("usr/bin");
        fs::create_dir_all(&usr_bin).unwrap();
        fs::write(usr_bin.join("bash"), b"").unwrap();

        let shell = find_chroot_shell(dir.path()).unwrap();
        assert_eq!(shell, "/usr/bin/bash");
    }

    #[test]
    fn layer_chain_rel_paths_builds_dotted_stack() {
        let chain = layer_chain_rel_paths(Path::new("minimal.standard.live.squashfs")).unwrap();
        assert_eq!(
            chain,
            vec![
                PathBuf::from("minimal.squashfs"),
                PathBuf::from("minimal.standard.squashfs"),
                PathBuf::from("minimal.standard.live.squashfs"),
            ]
        );
    }

    #[test]
    fn is_same_or_dependent_layer_matches_parent_layer() {
        let base = Path::new("minimal.squashfs");
        let target = Path::new("minimal.standard.live.squashfs");
        assert!(is_same_or_dependent_layer(base, target).unwrap());
    }

    #[test]
    fn layer_regeneration_levels_group_independent_layers() {
        let targets = vec![
            PathBuf::from("minimal.standard.live.squashfs"),
            PathBuf::from("minimal.standard.squashfs"),
            PathBuf::from("minimal.enhanced-secureboot.squashfs"),
        ];
        let levels = layer_regeneration_levels(&targets).unwrap();
        assert_eq!(levels.len(), 2);
        assert_eq!(
            levels[0],
            vec![
                PathBuf::from("minimal.enhanced-secureboot.squashfs"),
                PathBuf::from("minimal.standard.squashfs"),
            ]
        );
        assert_eq!(
            levels[1],
            vec![PathBuf::from("minimal.standard.live.squashfs")]
        );
    }

    #[test]
    fn auto_layer_jobs_for_cpus_is_bounded() {
        assert_eq!(auto_layer_jobs_for_cpus(1), 1);
        assert_eq!(auto_layer_jobs_for_cpus(4), 4);
        assert_eq!(auto_layer_jobs_for_cpus(32), 16);
    }

    #[test]
    fn decode_mountinfo_path_unescapes_octal_sequences() {
        let raw = "/tmp/frozenos\\040builder/tab\\011dir/newline\\012end";
        let decoded = decode_mountinfo_path(raw);
        assert_eq!(decoded, "/tmp/frozenos builder/tab\tdir/newline\nend");
    }

    #[test]
    fn workspace_mount_targets_from_mountinfo_filters_and_sorts_descendants() {
        let mount_root = Path::new("/work/.work");
        let mountinfo = r#"
20 1 8:1 / / rw,relatime - ext4 /dev/sda1 rw
21 20 0:30 / /work/.work/rootfs/dev rw,nosuid - devtmpfs udev rw,size=1024
22 20 0:31 / /work/.work/rootfs/proc rw,nosuid - proc proc rw
23 20 0:32 / /work/.work rw,relatime - ext4 /dev/sda1 rw
24 20 0:33 / /work/.work/rootfs/sys rw,nosuid - sysfs sysfs rw
25 20 0:34 / /work/other rw,relatime - ext4 /dev/sda1 rw
"#;
        let targets = workspace_mount_targets_from_mountinfo(mount_root, mountinfo);
        assert_eq!(
            targets,
            vec![
                PathBuf::from("/work/.work/rootfs/dev"),
                PathBuf::from("/work/.work/rootfs/proc"),
                PathBuf::from("/work/.work/rootfs/sys"),
            ]
        );
    }
}
