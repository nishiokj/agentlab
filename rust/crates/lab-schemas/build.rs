use std::fs;
use std::path::{Path, PathBuf};

fn main() {
    let schemas_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../../schemas");
    println!("cargo:rerun-if-changed={}", schemas_dir.display());
    emit_rerun_if_changed(&schemas_dir);
}

fn emit_rerun_if_changed(path: &Path) {
    let Ok(metadata) = fs::metadata(path) else {
        return;
    };
    println!("cargo:rerun-if-changed={}", path.display());
    if !metadata.is_dir() {
        return;
    }

    let Ok(entries) = fs::read_dir(path) else {
        return;
    };
    let mut children = entries
        .filter_map(|entry| entry.ok().map(|value| value.path()))
        .collect::<Vec<_>>();
    children.sort();
    for child in children {
        emit_rerun_if_changed(&child);
    }
}
