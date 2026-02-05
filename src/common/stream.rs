use base64::Engine;
use borsh::{BorshDeserialize, BorshSerialize};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use solana_client::{
    nonblocking::pubsub_client::PubsubClient,
    rpc_config::{RpcTransactionLogsConfig, RpcTransactionLogsFilter},
};
use solana_commitment_config::CommitmentConfig;
use solana_sdk::pubkey::Pubkey;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use super::types::Cluster;
use crate::{constants, error};

/// Error type for event parsing failures
#[derive(Debug)]
pub enum ParseEventError {
    Base64(base64::DecodeError),
    DataTooShort,
    Deserialize(std::io::Error),
}

impl std::fmt::Display for ParseEventError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Base64(e) => write!(f, "base64 decode error: {}", e),
            Self::DataTooShort => write!(f, "data too short for discriminator"),
            Self::Deserialize(e) => write!(f, "deserialize error: {}", e),
        }
    }
}

/// Event emitted when a new token is created
///
/// This event contains information about a newly created token, including its
/// metadata, mint address, bonding curve address, and the accounts involved.
#[derive(BorshSerialize, BorshDeserialize, Debug, Serialize, Deserialize)]
pub struct CreateEvent {
    pub name: String,
    pub symbol: String,
    pub uri: String,
    pub mint: Pubkey,
    pub bonding_curve: Pubkey,
    pub user: Pubkey,
    pub creator: Pubkey,
    pub timestamp: i64,
    pub virtual_token_reserves: u64,
    pub virtual_sol_reserves: u64,
    pub real_token_reserves: u64,
    pub token_total_supply: u64,
    pub token_program: Pubkey,
    pub mayhem_mode: bool,
}

/// Event emitted when a token is bought or sold
///
/// This event contains details about a trade transaction, including the amounts
/// exchanged, the type of trade (buy/sell), and the updated bonding curve state.
#[derive(BorshSerialize, BorshDeserialize, Debug, Serialize, Deserialize)]
pub struct TradeEvent {
    pub mint: Pubkey,
    pub sol_amount: u64,
    pub token_amount: u64,
    pub is_buy: bool,
    pub user: Pubkey,
    pub timestamp: i64,
    pub virtual_sol_reserves: u64,
    pub virtual_token_reserves: u64,
    pub real_sol_reserves: u64,
    pub real_token_reserves: u64,
    pub fee_recipient: Pubkey,
    pub fee_basis_points: u64,
    pub fee: u64,
    pub creator: Pubkey,
    pub creator_fee_basis_points: u64,
    pub creator_fee: u64,
    pub track_volume: bool,
    pub total_unclaimed_tokens: u64,
    pub total_claimed_tokens: u64,
    pub current_sol_volume: u64,
    pub last_update_timestamp: i64,
    pub operation: String,
    pub mayhem_mode: bool,
}

/// Event emitted when a bonding curve operation completes
///
/// This event signals the completion of a bonding curve operation,
/// providing information about the involved accounts.
#[derive(BorshSerialize, BorshDeserialize, Debug, Serialize, Deserialize)]
pub struct CompleteEvent {
    pub user: Pubkey,
    pub mint: Pubkey,
    pub bonding_curve: Pubkey,
    pub timestamp: i64,
}

/// Event emitted when global parameters are updated
///
/// This event contains information about updates to the global program parameters,
/// including fee settings and initial bonding curve configuration values.
#[derive(BorshSerialize, BorshDeserialize, Debug, Serialize, Deserialize)]
pub struct SetParamsEvent {
    pub initial_virtual_token_reserves: u64,
    pub initial_virtual_sol_reserves: u64,
    pub initial_real_token_reserves: u64,
    pub final_real_sol_reserves: u64,
    pub token_total_supply: u64,
    pub fee_basis_points: u64,
    pub withdraw_authority: Pubkey,
    pub enable_migrate: bool,
    pub pool_migration_fee: u64,
    pub creator_fee_basis_points: u64,
    pub fee_recipients: [Pubkey; 8],
    pub timestamp: i64,
    pub set_creator_authority: Pubkey,
    pub admin_set_creator_authority: Pubkey,
}

/// Enum representing all possible event types emitted by the Pump.fun program
///
/// This enum acts as a container for the different event types that can be
/// emitted by the program. It's used to provide a unified type for event handlers.
#[derive(Debug, Serialize, Deserialize)]
pub enum PumpFunEvent {
    Create(CreateEvent),
    Trade(TradeEvent),
    Complete(CompleteEvent),
    SetParams(SetParamsEvent),
    Unknown(Vec<u8>),
}

/// Represents an active WebSocket subscription to Pump.fun events
///
/// This struct manages the lifecycle of an event subscription, automatically
/// unsubscribing when dropped to ensure proper cleanup of resources.
pub struct Subscription {
    pub task: JoinHandle<()>,
    pub unsubscribe: Box<dyn Fn() + Send>,
}

impl Subscription {
    pub fn new(task: JoinHandle<()>, unsubscribe: Box<dyn Fn() + Send>) -> Self {
        Subscription { task, unsubscribe }
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        (self.unsubscribe)();
        self.task.abort();
    }
}

/// Parses base64-encoded program log data into a structured PumpFunEvent
///
/// This function decodes the base64 data from program logs, identifies the event type
/// using the discriminator (first 8 bytes), and deserializes the remaining data into
/// the appropriate event structure.
///
/// # Arguments
///
/// * `data` - Base64-encoded event data from program logs
///
/// # Returns
///
/// Returns a parsed PumpFunEvent if successful, or an error if parsing fails
pub fn parse_event(data: &str) -> Result<PumpFunEvent, ParseEventError> {
    // Decode base64
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(data)
        .map_err(ParseEventError::Base64)?;

    // Get event type from first 8 bytes as u64
    if decoded.len() < 8 {
        return Err(ParseEventError::DataTooShort);
    }

    // SAFETY: we just verified len >= 8
    let discriminator = u64::from_le_bytes(unsafe {
        decoded.get_unchecked(..8).try_into().unwrap_unchecked()
    });
    let payload = unsafe { decoded.get_unchecked(8..) };

    match discriminator {
        0x7663ebde4da9721b => Ok(PumpFunEvent::Create(
            CreateEvent::try_from_slice(payload).map_err(ParseEventError::Deserialize)?,
        )),
        0xee61e64ed37fdbbd => Ok(PumpFunEvent::Trade(
            TradeEvent::try_from_slice(payload).map_err(ParseEventError::Deserialize)?,
        )),
        0x08982ed49c61725f => Ok(PumpFunEvent::Complete(
            CompleteEvent::try_from_slice(payload).map_err(ParseEventError::Deserialize)?,
        )),
        0x838f303ef69fc3df => Ok(PumpFunEvent::SetParams(
            SetParamsEvent::try_from_slice(payload).map_err(ParseEventError::Deserialize)?,
        )),
        _ => Ok(PumpFunEvent::Unknown(decoded)),
    }
}

/// Subscribes to Pump.fun program events emitted on-chain
///
/// This function establishes a WebSocket connection to the Solana cluster and
/// subscribes to all transaction logs that mention the Pump.fun program. It parses
/// the program data from these logs into strongly-typed event structures.
///
/// Events are delivered through the provided callback function as they occur. The
/// subscription continues until the returned `Subscription` object is dropped.
///
/// # Arguments
///
/// * `cluster` - Solana cluster configuration containing RPC endpoints
/// * `mentioned` - Optional public key to filter events by mentions. If None, subscribes to all Pump.fun events
/// * `commitment` - Optional commitment level for the subscription. If None, uses the
///   default from the cluster configuration
/// * `callback` - A function that will be called for each event with the parsed result
///
/// # Returns
///
/// Returns a `Subscription` object that manages the lifecycle of the subscription.
/// When this object is dropped, the subscription is automatically terminated. If
/// the subscription cannot be established, returns a ClientError.
///
/// # Errors
///
/// Returns an error if:
/// - The WebSocket connection cannot be established
/// - The subscription request fails
///
/// # Examples
///
/// ```no_run
/// use pumpfun::{PumpFun, common::types::{Cluster, PriorityFee}};
/// use solana_commitment_config::CommitmentConfig;
/// use std::{sync::Arc, error::Error};
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn Error>> {
///     // Create cluster configuration
///     let cluster = Cluster::mainnet(
///         CommitmentConfig::confirmed(),
///         PriorityFee::default()
///     );
///
///     // Define callback to process events
///     let callback = |result| {
///         match result {
///             Ok(event) => println!("Event received: {:#?}", event),
///             Err(err) => eprintln!("Error parsing event: {}", err),
///         }
///     };
///
///     // Subscribe to events
///     let subscription = pumpfun::common::stream::subscribe(cluster, None, None, callback).await?;
///
///     // Keep subscription alive until program terminates
///     tokio::signal::ctrl_c().await?;
///     Ok(())
/// }
/// ```
pub async fn subscribe<F>(
    cluster: Cluster,
    mentioned: Option<String>,
    commitment: Option<CommitmentConfig>,
    callback: F,
) -> Result<Subscription, error::ClientError>
where
    F: Fn(Result<PumpFunEvent, ParseEventError>) + Send + Sync + 'static,
{
    // Initialize PubsubClient
    let ws_url = &cluster.rpc.ws;
    let pubsub_client = PubsubClient::new(ws_url)
        .await
        .map_err(error::ClientError::PubsubClientError)?;

    let (tx, _) = mpsc::channel(1);
    let (cb_tx, mut cb_rx) = mpsc::channel::<Result<PumpFunEvent, ParseEventError>>(1000);

    tokio::spawn(async move {
        while let Some(result) = cb_rx.recv().await {
            callback(result);
        }
    });

    let task = tokio::spawn(async move {
        // Subscribe to logs for the program
        let (mut stream, _unsubscribe) = pubsub_client
            .logs_subscribe(
                RpcTransactionLogsFilter::Mentions(vec![
                    mentioned.unwrap_or(constants::accounts::PUMPFUN.to_string())
                ]),
                RpcTransactionLogsConfig {
                    commitment: Some(commitment.unwrap_or(cluster.commitment)),
                },
            )
            .await
            .unwrap();

        const PREFIX: &[u8] = b"Program data: ";
        const PREFIX_LEN: usize = PREFIX.len();

        // Process incoming logs
        while let Some(log) = stream.next().await {
            log.value
                .logs
                .iter()
                // SAFETY: filter_map verifies string starts with PREFIX before slicing
                .filter_map(|line| {
                    line.as_bytes()
                        .starts_with(PREFIX)
                        .then(|| unsafe { line.get_unchecked(PREFIX_LEN..) })
                })
                .for_each(|data| {
                    let _ = cb_tx.try_send(parse_event(data));
                });
        }
    });

    Ok(Subscription::new(
        task,
        Box::new(move || {
            let _ = tx.try_send(());
        }),
    ))
}

#[cfg(test)]
mod tests {
    use crate::common::types::PriorityFee;

    use super::*;
    use std::sync::Arc;
    use tokio::sync::Mutex;
    use tokio::time::{timeout, Duration};

    #[cfg(not(skip_expensive_tests))]
    #[tokio::test]
    async fn test_subscribe() {
        if std::env::var("SKIP_EXPENSIVE_TESTS").is_ok() {
            return;
        }

        // Define the cluster
        let cluster = Cluster::mainnet(CommitmentConfig::processed(), PriorityFee::default());

        // Shared vector to collect events
        let events: Arc<Mutex<Vec<PumpFunEvent>>> = Arc::new(Mutex::new(Vec::new()));

        // Define the callback to store events
        let callback = {
            let events = Arc::clone(&events);
            move |result: Result<PumpFunEvent, ParseEventError>| {
                match result {
                    Ok(event) => {
                        let events = Arc::clone(&events);
                        tokio::spawn(async move {
                            let mut events = events.lock().await;
                            events.push(event);
                        });
                    }
                    Err(e) => {
                        eprintln!("Error in subscription: {}", e);
                    }
                }
            }
        };

        // Start the subscription
        let subscription = subscribe(cluster, None, None, callback)
            .await
            .expect("Failed to start subscription");

        // Wait for 30 seconds to collect events
        let wait_duration = Duration::from_secs(30);
        timeout(wait_duration, async {
            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        })
        .await
        .unwrap_err(); // Expect a timeout error to end the waiting period

        // Clean up the subscription
        drop(subscription);

        // Verify that at least one event was received
        let events = events.lock().await;
        assert!(
            !events.is_empty(),
            "No events received within {} seconds",
            wait_duration.as_secs()
        );

        println!("Received {} events", events.len());
    }
}
