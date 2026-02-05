use std::{
    fs,
    sync::{Arc, Mutex, MutexGuard, OnceLock},
};

use pumpfun::{
    common::types::{Cluster, PriorityFee},
    PumpFun,
};
use solana_commitment_config::CommitmentConfig;
use solana_sdk::signature::{read_keypair_file, Keypair};

// Load the default keypair with error handling
fn load_default_keypair() -> Result<Keypair, String> {
    // Get home directory in a cross-platform way
    let home_path = dirs::home_dir().ok_or("Could not determine home directory")?;
    let default_keypair_path = home_path.join(".config/solana/id.json");

    // Check if the keypair file exists
    if !default_keypair_path.exists() {
        // Generate a new keypair
        let keypair = Keypair::new();

        // Create directory if it doesn't exist
        if let Some(parent) = default_keypair_path.parent() {
            fs::create_dir_all(parent)
                .map_err(|e| format!("Failed to create directory {}: {}", parent.display(), e))?;
        }

        // Write the keypair to file
        let keypair_bytes = keypair.to_bytes();
        fs::write(&default_keypair_path, keypair_bytes).map_err(|e| {
            format!(
                "Failed to write keypair to {}: {}",
                default_keypair_path.display(),
                e
            )
        })?;

        return Ok(keypair);
    }

    // Read existing keypair
    read_keypair_file(&default_keypair_path).map_err(|e| {
        format!(
            "Failed to read keypair from {}: {}",
            default_keypair_path.display(),
            e
        )
    })
}

// Global state with OnceLock
static DEFAULT_KEYPAIR: OnceLock<Keypair> = OnceLock::new();
static PAYER: OnceLock<Arc<Keypair>> = OnceLock::new();
static MINT: OnceLock<Arc<Keypair>> = OnceLock::new();
static CLIENT: OnceLock<Mutex<PumpFun>> = OnceLock::new();

// Initialize global state
fn initialize_globals() {
    // Load and set the default keypair
    let keypair = load_default_keypair().expect("Failed to load default keypair");
    DEFAULT_KEYPAIR
        .set(keypair)
        .expect("DEFAULT_KEYPAIR already set");

    // Set payer and mint keypairs
    PAYER
        .set(Arc::new(DEFAULT_KEYPAIR.get().unwrap().insecure_clone()))
        .expect("PAYER already set");
    MINT.set(Arc::new(Keypair::from_base58_string(
        "2Cc5p7aNW8jsTbDxmPMfguDxwrDFWUu1s93gABJYTUJ2xGLF9w2EpPCW1CGFvKYAWzuHXh5fLhrmroHjd8LwBQxj",
    )))
    .expect("MINT already set");

    // Initialize the PumpFun client with configurable cluster URLs
    CLIENT
        .set(Mutex::new(PumpFun::new(
            PAYER.get().unwrap().clone(),
            if std::env::var("SKIP_EXPENSIVE_TESTS").is_ok() {
                Cluster::mainnet(CommitmentConfig::finalized(), PriorityFee::default())
            } else {
                Cluster::localnet(CommitmentConfig::finalized(), PriorityFee::default())
            },
        )))
        .unwrap_or_else(|_| panic!("CLIENT already set"));
}

pub struct TestContext {
    pub payer: Arc<Keypair>,
    pub mint: Arc<Keypair>,
    pub client: MutexGuard<'static, PumpFun>,
}

impl Default for TestContext {
    fn default() -> Self {
        // Ensure globals are initialized
        if DEFAULT_KEYPAIR.get().is_none() {
            initialize_globals();
        }

        Self {
            payer: PAYER.get().unwrap().clone(),
            mint: MINT.get().unwrap().clone(),
            client: CLIENT
                .get()
                .unwrap()
                .lock()
                .expect("Failed to lock CLIENT mutex"),
        }
    }
}
