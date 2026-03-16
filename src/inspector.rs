use std::collections::BTreeSet;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use aho_corasick::{AhoCorasick, AhoCorasickBuilder};
use anyhow::{Context, Result};

const ISO_SECTOR_SIZE: u64 = 2048;
const PRIMARY_VOLUME_DESCRIPTOR_SECTOR: u64 = 16;
const MAX_SCAN_BYTES: u64 = 512 * 1024 * 1024;
const CHUNK_SIZE: usize = 4 * 1024 * 1024;

#[derive(Debug, Clone)]
pub struct IsoReport {
    pub file_size_bytes: u64,
    pub volume_id: Option<String>,
    pub detected_release: Option<String>,
    pub architecture: Option<String>,
    pub boot_modes: Vec<String>,
    pub squashfs_candidates: Vec<String>,
    pub supported: bool,
    pub support_reason: String,
}

pub fn inspect_iso(path: &Path) -> Result<IsoReport> {
    let metadata =
        std::fs::metadata(path).with_context(|| format!("failed to stat {}", path.display()))?;
    let file_size_bytes = metadata.len();
    let volume_id = read_volume_id(path)?;

    let scanner = IsoPatternScanner::new()?;
    let scan = scanner.scan(path)?;

    let architecture = detect_architecture(volume_id.as_deref(), &scan);
    let detected_release = detect_release(volume_id.as_deref(), &scan);
    let boot_modes = detect_boot_modes(&scan);

    let supported = matches!(detected_release.as_deref(), Some("Ubuntu 24.04"))
        && matches!(architecture.as_deref(), Some("amd64"))
        && !scan.squashfs_candidates.is_empty();
    let support_reason = if supported {
        "supported (ubuntu 24.04 amd64 candidate)".to_string()
    } else {
        "unsupported or not confidently detectable as Ubuntu 24.04 amd64".to_string()
    };

    Ok(IsoReport {
        file_size_bytes,
        volume_id,
        detected_release,
        architecture,
        boot_modes,
        squashfs_candidates: scan.squashfs_candidates.into_iter().collect(),
        supported,
        support_reason,
    })
}

#[derive(Debug, Default)]
struct ScanResult {
    contains_ubuntu_24_04: bool,
    contains_amd64: bool,
    has_boot_catalog: bool,
    has_efi_boot: bool,
    has_isolinux: bool,
    squashfs_candidates: BTreeSet<String>,
}

struct IsoPatternScanner {
    ac: AhoCorasick,
    max_pattern_len: usize,
}

impl IsoPatternScanner {
    fn new() -> Result<Self> {
        let patterns = [
            "ubuntu 24.04",
            "amd64",
            "boot.catalog",
            "efi/boot/bootx64.efi",
            "bootx64.efi",
            "isolinux/isolinux.bin",
            ".squashfs",
        ];
        let ac = AhoCorasickBuilder::new()
            .ascii_case_insensitive(true)
            .build(patterns)
            .context("failed to build ISO pattern scanner")?;
        let max_pattern_len = patterns.iter().map(|p| p.len()).max().unwrap_or(64).max(64);
        Ok(Self {
            ac,
            max_pattern_len,
        })
    }

    fn scan(&self, path: &Path) -> Result<ScanResult> {
        let mut file =
            File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
        let mut result = ScanResult::default();
        let mut processed: u64 = 0;
        let mut overlap: Vec<u8> = Vec::new();
        let overlap_keep = self.max_pattern_len.saturating_sub(1).max(64);

        while processed < MAX_SCAN_BYTES {
            let remaining = (MAX_SCAN_BYTES - processed) as usize;
            if remaining == 0 {
                break;
            }
            let read_len = CHUNK_SIZE.min(remaining);
            let mut chunk = vec![0u8; read_len];
            let n = file
                .read(&mut chunk)
                .with_context(|| format!("failed to read {}", path.display()))?;
            if n == 0 {
                break;
            }
            chunk.truncate(n);

            let mut data = overlap;
            data.extend_from_slice(&chunk);

            for mat in self.ac.find_iter(&data) {
                let hit = &data[mat.start()..mat.end()];
                if contains_ascii_nocase(hit, b"ubuntu 24.04") {
                    result.contains_ubuntu_24_04 = true;
                }
                if contains_ascii_nocase(hit, b"amd64") {
                    result.contains_amd64 = true;
                }
                if contains_ascii_nocase(hit, b"boot.catalog") {
                    result.has_boot_catalog = true;
                }
                if contains_ascii_nocase(hit, b"efi/boot/bootx64.efi") {
                    result.has_efi_boot = true;
                }
                if contains_ascii_nocase(hit, b"bootx64.efi") {
                    result.has_efi_boot = true;
                }
                if contains_ascii_nocase(hit, b"isolinux/isolinux.bin") {
                    result.has_isolinux = true;
                }
                if contains_ascii_nocase(hit, b".squashfs") {
                    if let Some(candidate) = extract_path_like_token(&data, mat.start(), mat.end())
                    {
                        result.squashfs_candidates.insert(candidate);
                    }
                }
            }

            processed += n as u64;
            let keep = overlap_keep.min(data.len());
            overlap = data[data.len() - keep..].to_vec();
            if n < read_len {
                break;
            }
        }

        Ok(result)
    }
}

fn contains_ascii_nocase(haystack: &[u8], needle: &[u8]) -> bool {
    if haystack.len() < needle.len() {
        return false;
    }
    haystack
        .windows(needle.len())
        .any(|w| w.eq_ignore_ascii_case(needle))
}

fn extract_path_like_token(data: &[u8], start: usize, end: usize) -> Option<String> {
    let mut left = start;
    while left > 0 && is_path_char(data[left - 1]) {
        left -= 1;
    }

    let mut right = end;
    while right < data.len() && is_path_char(data[right]) {
        right += 1;
    }

    if right <= left {
        return None;
    }

    let token = String::from_utf8_lossy(&data[left..right]);
    let token = token.trim_matches(|c: char| c == '\0' || c.is_ascii_whitespace());
    if token.ends_with(".squashfs") && token.len() > ".squashfs".len() {
        Some(token.to_ascii_lowercase())
    } else {
        None
    }
}

fn is_path_char(b: u8) -> bool {
    b.is_ascii_alphanumeric() || matches!(b, b'/' | b'.' | b'_' | b'-' | b'+' | b':')
}

fn read_volume_id(path: &Path) -> Result<Option<String>> {
    let mut file =
        File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
    let offset = PRIMARY_VOLUME_DESCRIPTOR_SECTOR * ISO_SECTOR_SIZE;
    file.seek(SeekFrom::Start(offset))
        .with_context(|| format!("failed to seek {}", path.display()))?;

    let mut sector = [0u8; ISO_SECTOR_SIZE as usize];
    let n = file
        .read(&mut sector)
        .with_context(|| format!("failed to read volume descriptor from {}", path.display()))?;
    if n < sector.len() {
        return Ok(None);
    }

    let valid_pvd = sector[0] == 1 && &sector[1..6] == b"CD001";
    if !valid_pvd {
        return Ok(None);
    }

    let raw = &sector[40..72];
    let volume = String::from_utf8_lossy(raw)
        .trim_matches(char::from(0))
        .trim()
        .to_string();
    if volume.is_empty() {
        Ok(None)
    } else {
        Ok(Some(volume))
    }
}

fn detect_release(volume_id: Option<&str>, scan: &ScanResult) -> Option<String> {
    if volume_id
        .map(|v| v.to_ascii_lowercase().contains("ubuntu 24.04"))
        .unwrap_or(false)
        || scan.contains_ubuntu_24_04
    {
        Some("Ubuntu 24.04".to_string())
    } else {
        None
    }
}

fn detect_architecture(volume_id: Option<&str>, scan: &ScanResult) -> Option<String> {
    if volume_id
        .map(|v| v.to_ascii_lowercase().contains("amd64"))
        .unwrap_or(false)
        || scan.contains_amd64
    {
        Some("amd64".to_string())
    } else {
        None
    }
}

fn detect_boot_modes(scan: &ScanResult) -> Vec<String> {
    let mut modes = Vec::new();
    if scan.has_boot_catalog || scan.has_isolinux {
        modes.push("BIOS".to_string());
    }
    if scan.has_efi_boot {
        modes.push("UEFI".to_string());
    }
    modes
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Seek, Write};
    use tempfile::NamedTempFile;

    #[test]
    fn reads_iso_volume_id_from_primary_descriptor() {
        let mut file = NamedTempFile::new().unwrap();
        let mut pvd = [0u8; ISO_SECTOR_SIZE as usize];
        pvd[0] = 1;
        pvd[1..6].copy_from_slice(b"CD001");
        let volume = b"Ubuntu 24.04.4 LTS amd64";
        pvd[40..40 + volume.len()].copy_from_slice(volume);
        file.as_file_mut()
            .seek(SeekFrom::Start(
                PRIMARY_VOLUME_DESCRIPTOR_SECTOR * ISO_SECTOR_SIZE,
            ))
            .unwrap();
        file.write_all(&pvd).unwrap();
        file.flush().unwrap();

        let id = read_volume_id(file.path()).unwrap();
        assert_eq!(id.as_deref(), Some("Ubuntu 24.04.4 LTS amd64"));
    }
}
