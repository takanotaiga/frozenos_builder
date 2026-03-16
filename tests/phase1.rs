use std::path::PathBuf;

use frozenos_builder::inspector::inspect_iso;

#[test]
fn inspect_detects_local_test_iso_when_present() {
    let iso = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("ubuntu-24.04.4-desktop-amd64.iso");
    if !iso.exists() {
        eprintln!("skipping: {} not found", iso.display());
        return;
    }

    let report = inspect_iso(&iso).expect("inspect should succeed");
    assert_eq!(report.architecture.as_deref(), Some("amd64"));
    assert_eq!(report.detected_release.as_deref(), Some("Ubuntu 24.04"));
    assert!(!report.squashfs_candidates.is_empty());
    assert!(report.supported);
}
