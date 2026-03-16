## FrozenOS Builder

**FrozenOS Builder** is a CLI tool for customizing Ubuntu installation ISOs.

## Requirements

* Rust toolchain (Cargo)
* `xorriso`
  Install with:

```bash
sudo apt install xorriso
```

## Quick Start

```bash
cargo build
sudo ./target/debug/frozenos_builder build -f examples/minimal.yml
```

Example output:

```text
dist/frozenos-minimal.iso
```

## Recipe

See `examples/minimal.yml` for a minimal recipe example.

Build runs always start from a clean workspace (`build.workspace` is deleted first) and remove the workspace again after a successful build.
