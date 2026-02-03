pub mod utils;

use pumpfun::constants::accounts::TOKEN_PROGRAM;
use pumpfun::utils::CreateTokenMetadata;
use serial_test::serial;
use solana_sdk::{native_token::sol_str_to_lamports, signer::Signer};
use tempfile::TempDir;
use utils::TestContext;

#[tokio::test]
#[serial]
async fn test_01_get_global_account() {
    let ctx = TestContext::default();
    let global_acct = ctx
        .client
        .get_global_account()
        .await
        .expect("Failed to get global account");

    assert!(
        global_acct.initialized,
        "Global account should be initialized"
    );
}

#[cfg(not(skip_expensive_tests))]
#[tokio::test]
#[serial]
async fn test_02_create_token() {
    if std::env::var("SKIP_EXPENSIVE_TESTS").is_ok() {
        return;
    }

    let ctx = TestContext::default();

    // Mint keypair
    let mint = ctx.mint.insecure_clone();

    // Check if the mint account already exists (optional, depending on your setup)
    if ctx.client.rpc.get_account(&mint.pubkey()).await.is_err() {
        // Use TempDir for temporary file management
        let temp_dir = TempDir::new().expect("Failed to create temporary directory");
        let file_path = temp_dir.path().join("test_image.png");
        std::fs::write(&file_path, b"fake image data").expect("Failed to write temporary file");

        let metadata = CreateTokenMetadata {
            name: "Cat On Horse".to_string(),
            symbol: "COH".to_string(),
            description: "Lorem ipsum dolor, sit amet consectetur adipisicing elit.".to_string(),
            file: file_path.to_str().unwrap().to_string(),
            twitter: None,
            telegram: None,
            website: Some("https://example.com".to_string()),
        };

        let signature = ctx
            .client
            .create(
                mint.insecure_clone(),
                metadata.clone(),
                None,
                &TOKEN_PROGRAM,
            )
            .await
            .expect("Failed to create token");
        println!("Signature: {}", signature);
        println!("{} Mint: {}", metadata.symbol, mint.pubkey());

        let curve = ctx
            .client
            .get_bonding_curve_account(&mint.pubkey())
            .await
            .expect("Failed to get bonding curve");
        println!("{} Bonding Curve: {:#?}", metadata.symbol, curve);
    } else {
        println!("Mint already exists: {}", mint.pubkey());
    }
}

#[cfg(not(skip_expensive_tests))]
#[tokio::test]
#[serial]
async fn test_03_buy_token() {
    if std::env::var("SKIP_EXPENSIVE_TESTS").is_ok() {
        return;
    }

    let ctx = TestContext::default();
    let mint = ctx.mint.pubkey();
    let track_volume = Some(true);

    let signature = ctx
        .client
        .buy(
            mint,
            sol_str_to_lamports("1.0").unwrap(),
            track_volume,
            None,
            None,
            &TOKEN_PROGRAM,
        )
        .await
        .expect("Failed to buy tokens");
    println!("Signature: {}", signature);
}

#[cfg(not(skip_expensive_tests))]
#[tokio::test]
#[serial]
async fn test_04_sell_token() {
    if std::env::var("SKIP_EXPENSIVE_TESTS").is_ok() {
        return;
    }

    let ctx = TestContext::default();
    let mint = ctx.mint.pubkey();

    let signature = ctx
        .client
        .sell(mint, None, None, None, &TOKEN_PROGRAM)
        .await
        .expect("Failed to sell tokens");
    println!("Signature: {}", signature);
}
