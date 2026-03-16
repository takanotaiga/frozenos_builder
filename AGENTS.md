# Repository Guidelines

## Project Structure & Module Organization
This repository is a Rust crate with a minimal layout:
- `Cargo.toml`: crate metadata and dependencies.
- `src/main.rs`: current binary entrypoint.

When the project grows, keep code organized by domain under `src/` (for example, `src/builder.rs`, `src/config.rs`) and wire modules through `mod` declarations. Use `tests/` for integration tests and keep reusable test data in `tests/fixtures/`. Do not commit build artifacts from `target/`.

## Build, Test, and Development Commands
Use Cargo for all local workflows:
- `cargo check`: fast compile checks without producing binaries.
- `cargo run`: build and run the local binary.
- `cargo build --release`: production-style optimized build.
- `cargo test`: run unit and integration tests.
- `cargo fmt --all`: format code with `rustfmt`.
- `cargo clippy --all-targets --all-features -- -D warnings`: lint and fail on warnings.

## Coding Style & Naming Conventions
Follow idiomatic Rust (edition 2021):
- Indentation: 4 spaces, no tabs.
- Naming: `snake_case` for functions/modules/variables, `CamelCase` for types/traits, `UPPER_SNAKE_CASE` for constants.
- Keep modules focused and small; prefer composition over large files.
- Avoid `unwrap()`/`expect()` in production paths; return `Result` with clear error context.

Run `cargo fmt` and `cargo clippy` before opening a PR.

## Testing Guidelines
- Place unit tests next to implementation using `#[cfg(test)]`.
- Place integration tests in `tests/*.rs`.
- Use descriptive test names like `builds_image_from_valid_spec`.
- Add or update tests for every behavior change and bug fix.

Run `cargo test` locally before pushing changes.

## Commit & Pull Request Guidelines
There is no existing commit history yet; use Conventional Commits going forward:
- `feat: add image manifest parser`
- `fix: handle missing config file`
- `chore: update dependencies`

PRs should include:
- A short summary and motivation.
- Linked issue(s), if applicable.
- Test evidence (commands run and results).
- Example CLI output when behavior changes are user-visible.
