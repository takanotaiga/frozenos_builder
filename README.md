## FrozenOS Builder

**FrozenOS Builder** is a CLI tool for customizing Ubuntu installation ISOs.
Japanese README: [`README.ja.md`](./README.ja.md)

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
See `examples/ros2.yml` for a ROS 2 Jazzy customization example.
`examples/overlay/autoinstall.yaml` is an Ubuntu Desktop autoinstall sample and is copied by recipe copy steps.
