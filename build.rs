use std::process::Command;

fn main() {
    let git_hash = Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .unwrap_or_else(|_| "287f9b2".into());

    let version = env!("CARGO_PKG_VERSION");
    let full_version = if git_hash.is_empty() || git_hash == "unknown" {
        format!("{version}-alpha.1")
    } else {
        format!("{version}-alpha.1+{git_hash}")
    };

    println!("cargo:rustc-env=NAUTI_VERSION={full_version}");
    println!("cargo:rustc-env=NAUTI_GIT_HASH={git_hash}");
    println!("cargo:rerun-if-changed=.git/HEAD");
}
