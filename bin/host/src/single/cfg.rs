//! This module contains all CLI-specific code for the single chain entrypoint.

use super::{SingleChainHintHandler, SingleChainLocalInputs};
use crate::{
    DiskKeyValueStore, MemoryKeyValueStore, OfflineHostBackend, OnlineHostBackend,
    OnlineHostBackendCfg, PreimageServer, SharedKeyValueStore, SplitKeyValueStore,
    eth::http_provider, server::PreimageServerError,
};
use alloy_primitives::B256;
use alloy_provider::RootProvider;
use clap::Parser;
use kona_cli::cli_styles;
use kona_genesis::RollupConfig;
use kona_preimage::{
    BidirectionalChannel, Channel, HintReader, HintWriter, OracleReader, OracleServer,
};
use kona_proof::HintType;
use kona_providers_alloy::{OnlineBeaconClient, OnlineBlobProvider};
use kona_std_fpvm::{FileChannel, FileDescriptor};
use op_alloy_network::Optimism;
use serde::Serialize;
use std::{path::PathBuf, sync::Arc};
use tokio::{
    sync::RwLock,
    task::{self, JoinHandle},
};

/// The host binary CLI application arguments.
#[derive(Default, Parser, Serialize, Clone, Debug)]
#[command(styles = cli_styles())]
pub struct SingleChainHost {
    /// Hash of the L1 head block. Derivation stops after this block is processed.
    #[arg(long, visible_alias = "l1.head", env)]
    pub l1_head: Option<B256>,
    /// Hash of the agreed upon safe L2 block committed to by `--agreed-l2-output-root`.
    #[arg(long, visible_alias = "l2-head", visible_alias = "l2.head", env)]
    pub agreed_l2_head_hash: Option<B256>,
    /// Agreed safe L2 Output Root to start derivation from.
    #[arg(long, visible_alias = "l2-output-root", visible_alias = "l2.outputroot", env)]
    pub agreed_l2_output_root: Option<B256>,
    /// Claimed L2 output root at block # `--claimed-l2-block-number` to validate.
    #[arg(long, visible_alias = "l2-claim", visible_alias = "l2.claim", env)]
    pub claimed_l2_output_root: Option<B256>,
    /// Number of the L2 block that the claimed output root commits to.
    #[arg(long, visible_alias = "l2-block-number", visible_alias = "l2.blocknumber", env)]
    pub claimed_l2_block_number: Option<u64>,
    /// Address of L2 JSON-RPC endpoint to use (eth and debug namespace required).
    #[arg(
        long,
        visible_alias = "l2",
        visible_alias = "l2.node",
        env
    )]
    pub l2_node_address: Option<String>,
    /// Address of L1 JSON-RPC endpoint to use (eth and debug namespace required)
    #[arg(
        long,
        visible_alias = "l1",
        visible_alias = "l1.node",
        env
    )]
    pub l1_node_address: Option<String>,
    /// Address of the L1 Beacon API endpoint to use.
    #[arg(
        long,
        visible_alias = "beacon",
        visible_alias = "l1.beacon",
        env
    )]
    pub l1_beacon_address: Option<String>,
    /// The Data Directory for preimage data storage. Optional if running in online mode,
    /// required if running in offline mode.
    #[arg(
        long,
        visible_alias = "db",
        visible_alias = "datadir",
        env
    )]
    pub data_dir: Option<PathBuf>,
    /// Run the client program natively.
    #[arg(long, conflicts_with = "server")]
    pub native: bool,
    /// Run in pre-image server mode without executing any client program. If not provided, the
    /// host will run the client program in the host process.
    #[arg(long, conflicts_with = "native")]
    pub server: bool,
    /// The L2 chain ID of a supported chain. If provided, the host will look for the corresponding
    /// rollup config in the superchain registry.
    #[arg(
        long,
        env
    )]
    pub l2_chain_id: Option<u64>,
    /// Path to rollup config. If provided, the host will use this config instead of attempting to
    /// look up the config in the superchain registry.
    #[arg(
        long,
        alias = "rollup-cfg",
        visible_alias = "rollup.config",
        env
    )]
    pub rollup_config_path: Option<PathBuf>,
    /// Optionally enables the use of `debug_executePayload` to collect the execution witness from
    /// the execution layer.
    #[arg(long, env)]
    pub enable_experimental_witness_endpoint: bool,
    /// L2 agreed prestate (compatibility with OpProgramServerExecutor, currently unused)
    #[arg(long, visible_alias = "l2.agreed-prestate", env)]
    pub l2_agreed_prestate: Option<String>,
    /// Dependency set configuration (compatibility with OpProgramServerExecutor, currently unused)
    #[arg(long, visible_alias = "depset.config", env)]
    pub depset_config: Option<String>,
    /// Network configuration (compatibility with OpProgramServerExecutor, currently unused)
    #[arg(long, env)]
    pub network: Option<String>,
    /// L2 genesis configuration (compatibility with OpProgramServerExecutor, currently unused)
    #[arg(long, visible_alias = "l2.genesis", env)]
    pub l2_genesis: Option<String>,
    /// L2 experimental configuration (compatibility with OpProgramServerExecutor, currently unused)
    #[arg(long, visible_alias = "l2.experimental", env)]
    pub l2_experimental: Option<String>,
    /// Log level (compatibility with OpProgramServerExecutor, currently unused)
    #[arg(long, visible_alias = "log.level", env)]
    pub log_level: Option<String>,
    /// L2 custom configuration (compatibility with OpProgramServerExecutor, currently unused)
    #[arg(long, visible_alias = "l2.custom", env)]
    pub l2_custom: Option<String>,
}

/// An error that can occur when handling single chain hosts
#[derive(Debug, thiserror::Error)]
pub enum SingleChainHostError {
    /// An error when handling preimage requests.
    #[error("Error handling preimage request: {0}")]
    PreimageServerError(#[from] PreimageServerError),
    /// An IO error.
    #[error("IO error: {0}")]
    IOError(#[from] std::io::Error),
    /// A JSON parse error.
    #[error("Failed deserializing RollupConfig: {0}")]
    ParseError(#[from] serde_json::Error),
    /// Task failed to execute to completion.
    #[error("Join error: {0}")]
    ExecutionError(#[from] tokio::task::JoinError),
    /// Any other error.
    #[error("Error: {0}")]
    Other(&'static str),
}

impl SingleChainHost {
    /// Starts the [SingleChainHost] application.
    pub async fn start(self) -> Result<(), SingleChainHostError> {
        if self.server {
            let hint = FileChannel::new(FileDescriptor::HintRead, FileDescriptor::HintWrite);
            let preimage =
                FileChannel::new(FileDescriptor::PreimageRead, FileDescriptor::PreimageWrite);

            self.start_server(hint, preimage).await?.await?
        } else {
            self.start_native().await
        }
    }

    /// Starts the preimage server, communicating with the client over the provided channels.
    pub async fn start_server<C>(
        &self,
        hint: C,
        preimage: C,
    ) -> Result<JoinHandle<Result<(), SingleChainHostError>>, SingleChainHostError>
    where
        C: Channel + Send + Sync + 'static,
    {
        let kv_store = self.create_key_value_store()?;

        let task_handle = if self.is_offline() {
            task::spawn(async {
                PreimageServer::new(
                    OracleServer::new(preimage),
                    HintReader::new(hint),
                    Arc::new(OfflineHostBackend::new(kv_store)),
                )
                .start()
                .await
                .map_err(SingleChainHostError::from)
            })
        } else {
            let providers = self.create_providers().await?;
            let backend = OnlineHostBackend::new(
                self.clone(),
                kv_store.clone(),
                providers,
                SingleChainHintHandler,
            )
            .with_proactive_hint(HintType::L2PayloadWitness);

            task::spawn(async {
                PreimageServer::new(
                    OracleServer::new(preimage),
                    HintReader::new(hint),
                    Arc::new(backend),
                )
                .start()
                .await
                .map_err(SingleChainHostError::from)
            })
        };

        Ok(task_handle)
    }

    /// Starts the host in native mode, running both the client and preimage server in the same
    /// process.
    async fn start_native(&self) -> Result<(), SingleChainHostError> {
        let hint = BidirectionalChannel::new()?;
        let preimage = BidirectionalChannel::new()?;

        let server_task = self.start_server(hint.host, preimage.host).await?;
        let client_task = task::spawn(kona_client::single::run(
            OracleReader::new(preimage.client),
            HintWriter::new(hint.client),
        ));

        let (_, client_result) = tokio::try_join!(server_task, client_task)?;

        // Bubble up the exit status of the client program if execution completes.
        std::process::exit(client_result.is_err() as i32)
    }

    /// Returns `true` if the host is running in offline mode.
    pub const fn is_offline(&self) -> bool {
        self.l1_node_address.is_none() &&
            self.l2_node_address.is_none() &&
            self.l1_beacon_address.is_none() &&
            self.data_dir.is_some()
    }

    /// Reads the [RollupConfig] from the file system and returns it as a string.
    pub fn read_rollup_config(&self) -> Result<RollupConfig, SingleChainHostError> {
        let path = self.rollup_config_path.as_ref().ok_or_else(|| {
            SingleChainHostError::Other(
                "No rollup config path provided. Please provide a path to the rollup config.",
            )
        })?;

        // Read the serialized config from the file system.
        let ser_config = std::fs::read_to_string(path)?;

        // Deserialize the config and return it.
        serde_json::from_str(&ser_config).map_err(SingleChainHostError::ParseError)
    }

    /// Creates the key-value store for the host backend.
    pub fn create_key_value_store(&self) -> Result<SharedKeyValueStore, SingleChainHostError> {
        let local_kv_store = SingleChainLocalInputs::new(self.clone());

        let kv_store: SharedKeyValueStore = if let Some(ref data_dir) = self.data_dir {
            let disk_kv_store = DiskKeyValueStore::new(data_dir.clone());
            let split_kv_store = SplitKeyValueStore::new(local_kv_store, disk_kv_store);
            Arc::new(RwLock::new(split_kv_store))
        } else {
            let mem_kv_store = MemoryKeyValueStore::new();
            let split_kv_store = SplitKeyValueStore::new(local_kv_store, mem_kv_store);
            Arc::new(RwLock::new(split_kv_store))
        };

        Ok(kv_store)
    }

    /// Creates the providers required for the host backend.
    pub async fn create_providers(&self) -> Result<SingleChainProviders, SingleChainHostError> {
        let l1_provider = http_provider(
            self.l1_node_address
                .as_ref()
                .ok_or(SingleChainHostError::Other("Provider must be set"))?,
        );
        let blob_provider = OnlineBlobProvider::init(OnlineBeaconClient::new_http(
            self.l1_beacon_address
                .clone()
                .ok_or(SingleChainHostError::Other("Beacon API URL must be set"))?,
        ))
        .await;
        let l2_provider = http_provider::<Optimism>(
            self.l2_node_address
                .as_ref()
                .ok_or(SingleChainHostError::Other("L2 node address must be set"))?,
        );

        Ok(SingleChainProviders { l1: l1_provider, blobs: blob_provider, l2: l2_provider })
    }
}

impl OnlineHostBackendCfg for SingleChainHost {
    type HintType = HintType;
    type Providers = SingleChainProviders;
}

/// The providers required for the single chain host.
#[derive(Debug, Clone)]
pub struct SingleChainProviders {
    /// The L1 EL provider.
    pub l1: RootProvider,
    /// The L1 beacon node provider.
    pub blobs: OnlineBlobProvider<OnlineBeaconClient>,
    /// The L2 EL provider.
    pub l2: RootProvider<Optimism>,
}

#[cfg(test)]
mod test {
    use crate::single::SingleChainHost;
    use alloy_primitives::B256;
    use clap::Parser;

    #[test]
    fn test_op_challenger_compatibility() {
        // Test that OpProgramServerExecutor-style arguments work
        let op_challenger_args = [
            "single",
            "--server",
            "--l1.head", "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
            "--l2.head", "0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
            "--l2.claim", "0xfedcba0987654321fedcba0987654321fedcba0987654321fedcba0987654321",
            "--l2.blocknumber", "12345678",
            "--l2.outputroot", "0x9876543210fedcba9876543210fedcba9876543210fedcba9876543210fedcba",
            "--l1.beacon", "https://beacon-url.com",
            "--l1", "https://l1-url.com", 
            "--l2", "https://l2-url.com",
            "--datadir", "/tmp/test-data",
            "--network", "mainnet",
            "--log.level", "INFO",
            "--l2.experimental", "https://experimental-url.com",
        ];

        let parsed = SingleChainHost::try_parse_from(op_challenger_args);
        assert!(parsed.is_ok(), "OpProgramServerExecutor-style arguments should parse successfully: {:?}", parsed.err());
        
        let config = parsed.unwrap();
        assert!(config.server, "Server mode should be enabled");
        assert_eq!(config.l1_node_address, Some("https://l1-url.com".to_string()));
        assert_eq!(config.l2_node_address, Some("https://l2-url.com".to_string()));
        assert_eq!(config.l1_beacon_address, Some("https://beacon-url.com".to_string()));
        assert_eq!(config.data_dir, Some(std::path::PathBuf::from("/tmp/test-data")));
        assert_eq!(config.network, Some("mainnet".to_string()));
        assert_eq!(config.log_level, Some("INFO".to_string()));
    }

    #[test]
    fn test_minimal_op_challenger_args() {
        // Test with minimal required arguments that op-challenger might provide
        let minimal_args = [
            "single",
            "--server",
            "--l1", "https://l1-url.com",
            "--l1.beacon", "https://beacon-url.com", 
            "--l2", "https://l2-url.com",
            "--datadir", "/tmp/test-data",
        ];

        let parsed = SingleChainHost::try_parse_from(minimal_args);
        assert!(parsed.is_ok(), "Minimal OpProgramServerExecutor arguments should parse: {:?}", parsed.err());
    }

    #[test]
    fn test_flags() {
        let zero_hash_str = &B256::ZERO.to_string();
        let default_flags = [
            "single",
            "--l1-head",
            zero_hash_str,
            "--l2-head",
            zero_hash_str,
            "--l2-output-root",
            zero_hash_str,
            "--l2-claim",
            zero_hash_str,
            "--l2-block-number",
            "0",
        ];

        let cases = [
            // valid - these should all pass with the new permissive configuration
            (["--server", "--l2-chain-id", "0", "--data-dir", "dummy"].as_slice(), true),
            (["--server", "--rollup-config-path", "dummy", "--data-dir", "dummy"].as_slice(), true),
            (["--native", "--l2-chain-id", "0", "--data-dir", "dummy"].as_slice(), true),
            (["--native", "--rollup-config-path", "dummy", "--data-dir", "dummy"].as_slice(), true),
            (
                [
                    "--l1-node-address",
                    "dummy",
                    "--l2-node-address",
                    "dummy",
                    "--l1-beacon-address",
                    "dummy",
                    "--server",
                    "--l2-chain-id",
                    "0",
                ]
                .as_slice(),
                true,
            ),
            (
                [
                    "--server",
                    "--l2-chain-id",
                    "0",
                    "--data-dir",
                    "dummy",
                    "--enable-experimental-witness-endpoint",
                ]
                .as_slice(),
                true,
            ),
            // These are now valid due to op-challenger compatibility changes
            (["--server"].as_slice(), true),
            (["--native"].as_slice(), true),
            (["--rollup-config-path", "dummy"].as_slice(), true),
            (["--l2-chain-id", "0"].as_slice(), true),
            (["--l1-node-address", "dummy", "--server", "--l2-chain-id", "0"].as_slice(), true),
            (["--l2-node-address", "dummy", "--server", "--l2-chain-id", "0"].as_slice(), true),
            (["--l1-beacon-address", "dummy", "--server", "--l2-chain-id", "0"].as_slice(), true),
            // invalid - conflicts still apply
            (["--server", "--native", "--l2-chain-id", "0"].as_slice(), false),
            // empty args now valid due to op-challenger compatibility
            ([].as_slice(), true),
        ];

        for (args_ext, valid) in cases.into_iter() {
            let args = default_flags.iter().chain(args_ext.iter()).cloned().collect::<Vec<_>>();

            let parsed = SingleChainHost::try_parse_from(args);
            assert_eq!(parsed.is_ok(), valid);
        }
    }
}
