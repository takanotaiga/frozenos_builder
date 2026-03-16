use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, bail, Context, Result};
use serde::Deserialize;

pub const SUPPORTED_KIND: &str = "iso.build/v1";

#[derive(Debug, Clone, Deserialize)]
pub struct Recipe {
    pub kind: String,
    #[serde(default)]
    pub name: Option<String>,
    pub base: BaseConfig,
    pub build: BuildConfig,
    #[serde(default)]
    pub execution: ExecutionConfig,
    #[serde(default)]
    pub steps: Vec<Step>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BaseConfig {
    pub iso: PathBuf,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BuildConfig {
    pub workspace: PathBuf,
    #[serde(default)]
    pub output: Option<PathBuf>,
    #[serde(default)]
    pub keep_workdir: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ExecutionConfig {
    #[serde(default)]
    pub sandbox: SandboxMode,
    #[serde(default)]
    pub network: NetworkMode,
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            sandbox: SandboxMode::Namespace,
            network: NetworkMode::Host,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum SandboxMode {
    Chroot,
    #[default]
    Namespace,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub enum NetworkMode {
    #[default]
    Host,
    Off,
    ProxyOnly,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Step {
    #[serde(default)]
    pub stage: Stage,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    #[serde(default)]
    pub workdir: Option<PathBuf>,
    #[serde(default)]
    pub run: Option<String>,
    #[serde(default)]
    pub copy: Option<CopySpec>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CopySpec {
    pub from: PathBuf,
    pub to: PathBuf,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum Stage {
    Host,
    #[default]
    Rootfs,
    Iso,
}

impl Recipe {
    pub fn load(path: &Path) -> Result<Self> {
        let content = fs::read_to_string(path)
            .with_context(|| format!("failed to read recipe file {}", path.display()))?;
        let recipe: Recipe = serde_yaml_ng::from_str(&content)
            .with_context(|| format!("failed to parse YAML in {}", path.display()))?;
        Ok(recipe)
    }

    pub fn validate(&self, recipe_path: &Path) -> Result<()> {
        if self.kind != SUPPORTED_KIND {
            bail!(
                "unsupported kind {:?}; expected {:?}",
                self.kind,
                SUPPORTED_KIND
            );
        }

        let recipe_dir = recipe_path.parent().unwrap_or_else(|| Path::new("."));
        let base_iso = resolve_relative(recipe_dir, &self.base.iso);
        if !base_iso.is_file() {
            bail!("base ISO not found: {}", base_iso.display());
        }

        if self.steps.is_empty() {
            bail!("recipe must contain at least one step");
        }

        for (index, step) in self.steps.iter().enumerate() {
            let id = format!("step {}", index + 1);
            let has_run = step
                .run
                .as_ref()
                .map(|s| !s.trim().is_empty())
                .unwrap_or(false);
            let has_copy = step.copy.is_some();
            match (has_run, has_copy) {
                (true, false) | (false, true) => {}
                _ => bail!("{id} must define exactly one action: run or copy"),
            }

            if let Some(run) = &step.run {
                if run.trim().is_empty() {
                    bail!("{id} run command must not be empty");
                }
            }

            if let Some(copy) = &step.copy {
                let from = resolve_relative(recipe_dir, &copy.from);
                if !from.exists() {
                    bail!("{id} copy.from does not exist: {}", from.display());
                }
                if step.stage != Stage::Host && !copy.to.is_absolute() {
                    bail!(
                        "{id} copy.to must be absolute for {:?} stage: {}",
                        step.stage,
                        copy.to.display()
                    );
                }
            }
        }

        Ok(())
    }

    pub fn recipe_dir(recipe_path: &Path) -> &Path {
        recipe_path.parent().unwrap_or_else(|| Path::new("."))
    }

    pub fn base_iso_path(&self, recipe_path: &Path) -> PathBuf {
        resolve_relative(Self::recipe_dir(recipe_path), &self.base.iso)
    }

    pub fn workspace_path(&self, recipe_path: &Path) -> PathBuf {
        resolve_relative(Self::recipe_dir(recipe_path), &self.build.workspace)
    }

    pub fn output_iso_path(&self, recipe_path: &Path) -> Result<PathBuf> {
        let output = self
            .build
            .output
            .as_ref()
            .ok_or_else(|| anyhow!("build.output is required for build command"))?;
        Ok(resolve_relative(Self::recipe_dir(recipe_path), output))
    }
}

pub fn resolve_relative(base: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        base.join(path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn validate_requires_single_action() {
        let dir = tempdir().unwrap();
        let iso = dir.path().join("base.iso");
        let overlay = dir.path().join("overlay");
        fs::write(iso, b"iso").unwrap();
        fs::create_dir_all(overlay).unwrap();

        let recipe = Recipe {
            kind: SUPPORTED_KIND.to_string(),
            name: None,
            base: BaseConfig {
                iso: PathBuf::from("base.iso"),
            },
            build: BuildConfig {
                workspace: PathBuf::from(".work"),
                output: Some(PathBuf::from("out.iso")),
                keep_workdir: false,
            },
            execution: ExecutionConfig::default(),
            steps: vec![Step {
                stage: Stage::Rootfs,
                name: None,
                env: BTreeMap::new(),
                workdir: None,
                run: Some("echo hi".to_string()),
                copy: Some(CopySpec {
                    from: PathBuf::from("overlay"),
                    to: PathBuf::from("/"),
                }),
            }],
        };

        let recipe_path = dir.path().join("recipe.yml");
        let err = recipe.validate(&recipe_path).unwrap_err();
        assert!(err
            .to_string()
            .contains("must define exactly one action: run or copy"));
    }
}
