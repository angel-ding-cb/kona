//! Main entrypoint for the host binary.

#![warn(missing_debug_implementations, missing_docs, unreachable_pub, rustdoc::all)]
#![deny(unused_must_use, rust_2018_idioms)]
#![cfg_attr(docsrs, feature(doc_cfg, doc_auto_cfg))]

use anyhow::Result;
use clap::{ArgAction, Parser, Subcommand};
use kona_cli::{cli_styles, init_tracing_subscriber};
use serde::Serialize;
use tracing::info;
use tracing_subscriber::EnvFilter;

const ABOUT: &str = "
kona-host is a CLI application that runs the Kona pre-image server and client program. The host
can run in two modes: server mode and native mode. In server mode, the host runs the pre-image
server and waits for the client program in the parent process to request pre-images. In native
mode, the host runs the client program in a separate thread with the pre-image server in the
primary thread.
";

/// The host binary CLI application arguments.
#[derive(Parser, Serialize, Clone, Debug)]
#[command(about = ABOUT, version, styles = cli_styles())]
pub struct HostCli {
    /// Verbosity level (0-5)
    /// If set to 0, no logs are printed.
    /// By default, the verbosity level is set to 3 (info level).
    #[arg(long, short, default_value = "3", action = ArgAction::Count)]
    pub v: u8,
    /// Host mode
    #[command(subcommand)]
    pub mode: HostMode,
}

/// Operation modes for the host binary.
#[derive(Subcommand, Serialize, Clone, Debug)]
#[allow(clippy::large_enum_variant)]
pub enum HostMode {
    /// Run the host in single-chain mode.
    #[cfg(feature = "single")]
    Single(kona_host::single::SingleChainHost),
    /// Run the host in super-chain (interop) mode.
    #[cfg(feature = "interop")]
    Super(kona_host::interop::InteropHost),
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    // Workaround for op-challenger compatibility: if --server or --native is passed
    // without a subcommand, automatically inject 'single' subcommand
    let args: Vec<String> = std::env::args().collect();
    
    let has_server_or_native = args.iter().any(|arg| arg == "--server" || arg == "--native");
    let has_subcommand = args.iter().any(|arg| arg == "single" || arg == "super");
    
    let modified_args = if args.len() > 1 && has_server_or_native && !has_subcommand {
        // Insert 'single' after the program name
        let mut new_args = vec![args[0].clone(), "single".to_string()];
        new_args.extend_from_slice(&args[1..]);
        new_args
    } else {
        args
    };

    let cfg = HostCli::try_parse_from(modified_args)?;
    init_tracing_subscriber(cfg.v, None::<EnvFilter>)?;

    match cfg.mode {
        #[cfg(feature = "single")]
        HostMode::Single(cfg) => {
            cfg.start().await?;
        }
        #[cfg(feature = "interop")]
        HostMode::Super(cfg) => {
            cfg.start().await?;
        }
    }

    info!("Exiting host program.");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_server_flag_workaround() {
        // Test that --server without subcommand gets transformed correctly
        let original_args = vec![
            "kona-host".to_string(),
            "--server".to_string(),
            "--l1.beacon".to_string(),
            "https://beacon-url.com".to_string(),
        ];

        let has_server_or_native = original_args.iter().any(|arg| arg == "--server" || arg == "--native");
        let has_subcommand = original_args.iter().any(|arg| arg == "single" || arg == "super");

        assert!(has_server_or_native, "Should detect --server flag");
        assert!(!has_subcommand, "Should not detect subcommand initially");

        let modified_args = if original_args.len() > 1 && has_server_or_native && !has_subcommand {
            let mut new_args = vec![original_args[0].clone(), "single".to_string()];
            new_args.extend_from_slice(&original_args[1..]);
            new_args
        } else {
            original_args
        };

        let expected = vec![
            "kona-host".to_string(),
            "single".to_string(),
            "--server".to_string(),
            "--l1.beacon".to_string(),
            "https://beacon-url.com".to_string(),
        ];

        assert_eq!(modified_args, expected, "Should inject 'single' subcommand");
    }

    #[test]
    fn test_native_flag_workaround() {
        // Test that --native also triggers the workaround
        let original_args = vec![
            "kona-host".to_string(),
            "--native".to_string(),
            "--l1".to_string(),
            "https://l1-url.com".to_string(),
        ];

        let has_server_or_native = original_args.iter().any(|arg| arg == "--server" || arg == "--native");
        let has_subcommand = original_args.iter().any(|arg| arg == "single" || arg == "super");

        assert!(has_server_or_native, "Should detect --native flag");
        assert!(!has_subcommand, "Should not detect subcommand initially");
    }

    #[test]
    fn test_no_workaround_when_subcommand_present() {
        // Test that existing subcommands are not affected
        let original_args = vec![
            "kona-host".to_string(),
            "single".to_string(),
            "--server".to_string(),
            "--l1.beacon".to_string(),
            "https://beacon-url.com".to_string(),
        ];

        let has_server_or_native = original_args.iter().any(|arg| arg == "--server" || arg == "--native");
        let has_subcommand = original_args.iter().any(|arg| arg == "single" || arg == "super");

        assert!(has_server_or_native, "Should detect --server flag");
        assert!(has_subcommand, "Should detect existing subcommand");

        // In this case, no modification should happen
        let should_modify = original_args.len() > 1 && has_server_or_native && !has_subcommand;
        assert!(!should_modify, "Should not modify when subcommand already present");
    }
}
