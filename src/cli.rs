use std::path::{Path, PathBuf};

use anyhow::{bail, Result};
use clap::{Parser, Subcommand};

use crate::builder;
use crate::config::Recipe;
use crate::inspector::{inspect_iso, IsoReport};

#[derive(Debug, Parser)]
#[command(name = "frozenos-builder")]
#[command(about = "Ubuntu ISO customization orchestrator (phase 1)", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    Validate {
        #[arg(short, long, default_value = "recipe.yml")]
        file: PathBuf,
    },
    Inspect {
        iso: PathBuf,
    },
    Build {
        #[arg(short, long, default_value = "recipe.yml")]
        file: PathBuf,
    },
    Shell {
        #[arg(short, long, default_value = "recipe.yml")]
        file: PathBuf,
    },
}

pub fn run() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Validate { file } => cmd_validate(file),
        Commands::Inspect { iso } => cmd_inspect(iso),
        Commands::Build { file } => cmd_build(file),
        Commands::Shell { file } => cmd_shell(file),
    }
}

fn cmd_validate(file: PathBuf) -> Result<()> {
    let recipe = Recipe::load(&file)?;
    recipe.validate(&file)?;
    let iso_report = inspect_iso(&recipe.base_iso_path(&file))?;
    print_validation_ok(&file, &recipe, &iso_report);
    Ok(())
}

fn cmd_inspect(iso: PathBuf) -> Result<()> {
    let report = inspect_iso(&iso)?;
    print_inspection(&iso, &report);
    Ok(())
}

fn cmd_build(file: PathBuf) -> Result<()> {
    let recipe = Recipe::load(&file)?;
    recipe.validate(&file)?;
    let report = inspect_iso(&recipe.base_iso_path(&file))?;
    ensure_supported_iso(&report)?;
    builder::build(&file, &recipe)?;
    println!(
        "build completed: {}",
        recipe.output_iso_path(&file)?.to_string_lossy()
    );
    Ok(())
}

fn cmd_shell(file: PathBuf) -> Result<()> {
    let recipe = Recipe::load(&file)?;
    recipe.validate(&file)?;
    let report = inspect_iso(&recipe.base_iso_path(&file))?;
    ensure_supported_iso(&report)?;
    builder::shell(&file, &recipe)
}

fn ensure_supported_iso(report: &IsoReport) -> Result<()> {
    if report.supported {
        return Ok(());
    }
    bail!(
        "unsupported base ISO for phase 1: {}",
        report.support_reason
    )
}

fn print_validation_ok(recipe_file: &Path, recipe: &Recipe, report: &IsoReport) {
    println!("recipe: {}", recipe_file.display());
    println!("kind: {}", recipe.kind);
    if let Some(name) = &recipe.name {
        println!("name: {}", name);
    }
    println!("steps: {}", recipe.steps.len());
    println!(
        "base support: {} ({})",
        if report.supported { "ok" } else { "warning" },
        report.support_reason
    );
}

fn print_inspection(iso: &Path, report: &IsoReport) {
    println!("iso: {}", iso.display());
    println!("size_bytes: {}", report.file_size_bytes);
    println!(
        "volume_id: {}",
        report
            .volume_id
            .as_deref()
            .unwrap_or("<not detected from PVD>")
    );
    println!(
        "release: {}",
        report.detected_release.as_deref().unwrap_or("<unknown>")
    );
    println!(
        "architecture: {}",
        report.architecture.as_deref().unwrap_or("<unknown>")
    );
    if report.boot_modes.is_empty() {
        println!("boot_modes: <unknown>");
    } else {
        println!("boot_modes: {}", report.boot_modes.join(", "));
    }
    if report.squashfs_candidates.is_empty() {
        println!("squashfs_candidates: <none>");
    } else {
        println!("squashfs_candidates:");
        for candidate in &report.squashfs_candidates {
            println!("  - {}", candidate);
        }
    }
    println!(
        "phase1_support: {} ({})",
        if report.supported { "yes" } else { "no" },
        report.support_reason
    );
}
