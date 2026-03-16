use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use anyhow::{bail, Context, Result};
use serde::Deserialize;
use walkdir::WalkDir;

use crate::config::{resolve_relative, Recipe, Stage, Step};

const ISO_TREE_DIR: &str = "iso_tree";
const ROOTFS_DIR: &str = "rootfs";
const CASPER_DIR: &str = "casper";
const INSTALL_SOURCES_FILE: &str = "install-sources.yaml";

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
}

struct MountGuard {
    targets: Vec<PathBuf>,
}

pub fn build(recipe_path: &Path, recipe: &Recipe) -> Result<()> {
    ensure_command_available("xorriso")?;
    ensure_command_available("unsquashfs")?;
    ensure_command_available("mksquashfs")?;
    ensure_command_available("bash")?;
    let needs_rootfs_run = recipe
        .steps
        .iter()
        .any(|step| matches!(step.stage, Stage::Rootfs) && step.run.is_some());
    if needs_rootfs_run {
        ensure_command_available("chroot")?;
        ensure_command_available("mount")?;
        ensure_command_available("umount")?;
        ensure_effective_root("rootfs run steps require chroot execution as root")?;
    }

    let layout = prepare_workspace(recipe_path, recipe, true)?;
    let squashfs_path = find_squashfs(&layout.iso_tree_dir)?;
    extract_rootfs(&squashfs_path, &layout.rootfs_dir)?;
    execute_steps(recipe_path, recipe, &layout)?;
    repack_rootfs(&layout.rootfs_dir, &squashfs_path)?;
    let output_iso = recipe.output_iso_path(recipe_path)?;
    repack_iso(
        &recipe.base_iso_path(recipe_path),
        &layout.iso_tree_dir,
        &output_iso,
    )?;

    if !recipe.build.keep_workdir {
        fs::remove_dir_all(&layout.workspace_dir).with_context(|| {
            format!(
                "failed to clean workspace {}",
                layout.workspace_dir.display()
            )
        })?;
    }
    Ok(())
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

fn prepare_workspace(recipe_path: &Path, recipe: &Recipe, clean: bool) -> Result<WorkspaceLayout> {
    let workspace_dir = recipe.workspace_path(recipe_path);
    let iso_tree_dir = workspace_dir.join(ISO_TREE_DIR);
    let rootfs_dir = workspace_dir.join(ROOTFS_DIR);
    fs::create_dir_all(&workspace_dir)
        .with_context(|| format!("failed to create {}", workspace_dir.display()))?;
    if clean {
        if iso_tree_dir.exists() {
            fs::remove_dir_all(&iso_tree_dir)
                .with_context(|| format!("failed to reset {}", iso_tree_dir.display()))?;
        }
        if rootfs_dir.exists() {
            fs::remove_dir_all(&rootfs_dir)
                .with_context(|| format!("failed to reset {}", rootfs_dir.display()))?;
        }
    }
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
    let install_sources_path = casper_dir.join(INSTALL_SOURCES_FILE);
    if !install_sources_path.is_file() {
        return Ok(None);
    }

    let content = fs::read_to_string(&install_sources_path)
        .with_context(|| format!("failed to read {}", install_sources_path.display()))?;
    let sources: Vec<InstallSource> = serde_yaml_ng::from_str(&content).with_context(|| {
        format!(
            "failed to parse install sources YAML {}",
            install_sources_path.display()
        )
    })?;
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
}
