// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2024 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

use std::{
    collections::HashMap,
    str::FromStr, 
    sync::Arc, 
    time::Duration
};

use bytes::Bytes;
use nautilus_common::{
    cache::{database::CacheDatabaseAdapter, CacheConfig},
    custom::CustomData,
    enums::SerializationEncoding,
    runtime::get_runtime,
    signal::Signal,
    msgbus::database::DatabaseConfig, 
};
use nautilus_core::{correctness::check_slice_not_empty, nanos::UnixNanos, uuid::UUID4};
use nautilus_cryptography::providers::install_cryptographic_provider;
use nautilus_model::{
    accounts::any::AccountAny,
    data::{bar::Bar, quote::QuoteTick, trade::TradeTick, DataType},
    events::{order::OrderEventAny, position::snapshot::PositionSnapshot},
    identifiers::{
        AccountId, ClientId, ClientOrderId, ComponentId, InstrumentId, PositionId, StrategyId,
        TraderId, VenueOrderId,
    },
    instruments::{any::InstrumentAny, synthetic::SyntheticInstrument},
    orderbook::book::OrderBook,
    orders::any::OrderAny,
    position::Position,
    types::currency::Currency,
};
use redis::{Commands, Connection, Pipeline, RedisError};
use tokio::sync::Notify;
use ustr::Ustr;

use super::{REDIS_DELIMITER, REDIS_FLUSHDB};
use crate::redis::create_redis_connection;

use surrealkv::{Durability, Options, Store, Transaction};
use anyhow::Result;
use std::path::PathBuf;
use serde::Serialize;
use serde::Deserialize;

use once_cell::sync::Lazy;
use tokio::runtime::Runtime;
use std::sync::Mutex;

// Task and connection names
const CACHE_READ: &str = "cache-read";
const CACHE_WRITE: &str = "cache-write";

// Collection keys
const INDEX: &str = "index";
const GENERAL: &str = "general";
const CURRENCIES: &str = "currencies";
const INSTRUMENTS: &str = "instruments";
const SYNTHETICS: &str = "synthetics";
const ACCOUNTS: &str = "accounts";
const ORDERS: &str = "orders";
const POSITIONS: &str = "positions";
const ACTORS: &str = "actors";
const STRATEGIES: &str = "strategies";
const SNAPSHOTS: &str = "snapshots";
const HEALTH: &str = "health";

// Index keys
const INDEX_ORDER_IDS: &str = "index:order_ids";
const INDEX_ORDER_POSITION: &str = "index:order_position";
const INDEX_ORDER_CLIENT: &str = "index:order_client";
const INDEX_ORDERS: &str = "index:orders";
const INDEX_ORDERS_OPEN: &str = "index:orders_open";
const INDEX_ORDERS_CLOSED: &str = "index:orders_closed";
const INDEX_ORDERS_EMULATED: &str = "index:orders_emulated";
const INDEX_ORDERS_INFLIGHT: &str = "index:orders_inflight";
const INDEX_POSITIONS: &str = "index:positions";
const INDEX_POSITIONS_OPEN: &str = "index:positions_open";
const INDEX_POSITIONS_CLOSED: &str = "index:positions_closed";
const SURREAL_KV_DIR: &str = "./nautilus_embedded_storage";
const CHANNEL_BUFFER_SIZE: usize = 1;
const RETRY_DELAY_MS: u64 = 2000; // Define delay between retries in milliseconds
const REDIS_SCAN_BATCH_SIZE: usize = 100;

static TOKIO_RUNTIME: Lazy<Runtime> = Lazy::new(|| {
    Runtime::new().expect("Failed to create global Tokio runtime")
});

static GLOBAL_DB_CONFIG: Lazy<Mutex<Option<DatabaseConfig>>> = Lazy::new(|| Mutex::new(None));
lazy_static::lazy_static! {
    static ref IS_SURREAL_DIRTY: Mutex<bool> = Mutex::new(true); // Initially set to true
}

fn set_global_surreal_dirty_flag(value: bool) {
    let mut flag = IS_SURREAL_DIRTY.lock().unwrap();
    *flag = value; // Set the flag to the given value (true or false)
}

fn get_global_surreal_dirty_flag() -> bool {
    let flag = IS_SURREAL_DIRTY.lock().unwrap();
    *flag // Return the current value of the flag
}

enum RedisWALKey {
    NextOperationSequence,          // Key for the next sequence ID to be used for a new operation
    LastCommittedOperationSequence, // Key for the last sequence that has been committed
    GetKeyForOperationSequence(u64), // Generates the key for the operation at the given sequence
}

impl RedisWALKey {
    fn as_bytes(&self) -> Vec<u8> {
        match self {
            RedisWALKey::NextOperationSequence => b"RedisWal/NextOperationSequence".to_vec(),
            RedisWALKey::LastCommittedOperationSequence => b"RedisWal/LastCommittedOperationSequence".to_vec(),
            RedisWALKey::GetKeyForOperationSequence(sequence) => {
                format!("RedisWal/OperationSequence/{}", sequence).into_bytes()
            }
        }
    }
}

/// A type of database operation.
#[derive(Clone, Debug)]
pub enum DatabaseOperation {
    Insert,
    Update,
    Delete,
    Close,
}

#[derive(Clone, Debug)]
pub struct DatabaseCommand {
    /// The database operation type.
    pub op_type: DatabaseOperation,
    /// The primary key for the operation.
    pub key: Option<String>,
    /// The data payload for the operation.
    pub payload: Option<Vec<Bytes>>,
    /// Indicates whether the command has been committed to Redis.
    pub committed_to_redis: bool, // New field
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SerializableDatabaseCommand {
    op_type: String,
    key: Option<String>,
    payload: Option<Vec<Bytes>>, // Changed from Vec<String> to Vec<Bytes>
    committed_to_redis: bool,    // New field
}

// Conversion from `DatabaseCommand` to `SerializableDatabaseCommand`
impl From<DatabaseCommand> for SerializableDatabaseCommand {
    fn from(cmd: DatabaseCommand) -> Self {
        SerializableDatabaseCommand {
            op_type: format!("{:?}", cmd.op_type),
            key: cmd.key,
            payload: cmd.payload, // Directly assign `Bytes` payload
            committed_to_redis: cmd.committed_to_redis, // Copy the flag
        }
    }
}

// Conversion from `SerializableDatabaseCommand` to `DatabaseCommand`
impl TryFrom<SerializableDatabaseCommand> for DatabaseCommand {
    type Error = String;

    fn try_from(serializable: SerializableDatabaseCommand) -> Result<Self, Self::Error> {
        let op_type = match serializable.op_type.as_str() {
            "Insert" => DatabaseOperation::Insert,
            "Update" => DatabaseOperation::Update,
            "Delete" => DatabaseOperation::Delete,
            "Close" => DatabaseOperation::Close,
            _ => {
                return Err(format!("Unknown operation type: {}", serializable.op_type));
            }
        };

        Ok(DatabaseCommand {
            op_type,
            key: serializable.key,
            payload: serializable.payload, // Directly assign `Bytes` payload
            committed_to_redis: serializable.committed_to_redis, // Copy the flag
        })
    }
}

impl DatabaseCommand {
    /// Creates a new [`DatabaseCommand`] instance.
    #[must_use]
    pub fn new(op_type: DatabaseOperation, key: String, payload: Option<Vec<Bytes>>) -> Self {
        Self {
            op_type,
            key: Some(key),
            payload,
            committed_to_redis: false, // Default to `false` when creating a new command
        }
    }

    /// Initialize a `Close` database command, this is meant to close the database cache channel.
    #[must_use]
    pub fn close() -> Self {
        Self {
            op_type: DatabaseOperation::Close,
            key: None,
            payload: None,
            committed_to_redis: false, // Default to `false`
        }
    }
}

#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.core.nautilus_pyo3.infrastructure")
)]
pub struct RedisCacheDatabase {
    pub trader_id: TraderId,
    trader_key: String,
    con: Connection,
    tx: tokio::sync::mpsc::Sender<DatabaseCommand>, // Updated to use a bounded sender
    handle: tokio::task::JoinHandle<()>,
}

fn create_surrealkv_store() -> Result<Store> {
    let config = Options {
        dir: PathBuf::from(SURREAL_KV_DIR), // Persistent storage path
        disk_persistence: true,
        isolation_level: surrealkv::IsolationLevel::SerializableSnapshotIsolation,                        // Enable disk persistence
        ..Default::default()
    };

    Store::new(config).map_err(|e| anyhow::anyhow!("Failed to create SurrealKV store: {e}"))
}

fn new_surrealkv_transaction(store: &Store, durability: Durability) -> Result<Transaction> {
    let mut txn = store.begin()?;
    txn.set_durability(durability);
    Ok(txn)
}

async fn write_to_wal_store(store: &Store, message: DatabaseCommand) -> Result<()> {
    let mut txn = new_surrealkv_transaction(&store, Durability::Immediate)?;
    let next_sequence_key = RedisWALKey::NextOperationSequence.as_bytes();

    // Fetch the next sequence value
    let next_sequence: u64 = if let Some(sequence_bytes) = txn.get(&next_sequence_key)? {
        serde_json::from_slice(&sequence_bytes)?
    } else {
        0 // Default to 0 if sequence doesn't exist
    };

    // Create the new key for the operation
    let operation_key = RedisWALKey::GetKeyForOperationSequence(next_sequence).as_bytes();
    let serializable_command: SerializableDatabaseCommand = message.into();

    // Write the operation to the new key
    txn.set(&operation_key, &serde_json::to_vec(&serializable_command)?)?;

    // Increment the sequence and update it
    txn.set(&next_sequence_key, &serde_json::to_vec(&(next_sequence + 1))?)?;

    // Commit the transaction
    if let Err(e) = txn.commit().await {
        tracing::error!("Transaction commit failed: {e}. Rolling back transaction.");
        txn.rollback();
        return Err(anyhow::anyhow!("Failed to commit transaction: {e}"));
    }
    set_global_surreal_dirty_flag(true);
    Ok(())
}

async fn read_next_uncommitted_wal_operation(store: &Store) -> Result<Option<(u64, DatabaseCommand)>> {
    let mut txn = new_surrealkv_transaction(&store, Durability::Immediate)?;
    let last_committed_sequence_key = RedisWALKey::LastCommittedOperationSequence.as_bytes();

    // Fetch the last committed sequence
    let last_sequence: u64 = if let Some(sequence_bytes) = txn.get(&last_committed_sequence_key)? {
        serde_json::from_slice(&sequence_bytes)?
    } else {
        0 // Default to 0 if not present
    };

    // Get the next operation key
    let next_operation_key = RedisWALKey::GetKeyForOperationSequence(last_sequence + 1).as_bytes();

    // Retrieve the operation
    if let Some(operation_bytes) = txn.get(&next_operation_key)? {
        let message: SerializableDatabaseCommand = serde_json::from_slice(&operation_bytes)?;

        // Only return uncommitted operations
        if !message.committed_to_redis {
            return Ok(Some((
                last_sequence + 1,
                message.try_into().map_err(|e: String| anyhow::anyhow!(e))?,
            )));
        }
    }

    Ok(None) // No uncommitted operations found
}

async fn commit_message(store: &Store, sequence: u64) -> Result<()> {
    let mut txn = new_surrealkv_transaction(&store, Durability::Immediate)?;
    let last_committed_sequence_key = RedisWALKey::LastCommittedOperationSequence.as_bytes();
    let operation_key = RedisWALKey::GetKeyForOperationSequence(sequence).as_bytes();

    // Fetch and update the operation
    if let Some(existing_bytes) = txn.get(&operation_key)? {
        let mut operation: SerializableDatabaseCommand = serde_json::from_slice(&existing_bytes)?;

        // Check if the operation is already committed to Redis
        if operation.committed_to_redis {
            tracing::debug!(
                "Operation with sequence {} is already committed to Redis. Possible duplicate processing.",
                sequence
            );
            return Err(anyhow::anyhow!(
                "Operation with sequence {} is already committed to Redis. Aborting.",
                sequence
            ));
        }

        // Update the commit flag
        operation.committed_to_redis = true;

        // Write the updated operation back to the store
        txn.set(&operation_key, &serde_json::to_vec(&operation)?)?;
    } else {
        return Err(anyhow::anyhow!("Operation with sequence {} not found", sequence));
    }

    // Update the last committed sequence
    txn.set(&last_committed_sequence_key, &serde_json::to_vec(&sequence)?)?;

    // Commit the transaction with rollback on failure
    if let Err(e) = txn.commit().await {
        tracing::error!("Transaction commit failed for sequence {}: {e}. Rolling back transaction.", sequence);
        txn.rollback();
        return Err(anyhow::anyhow!("Failed to commit operation with sequence {}: {e}", sequence));
    }

    Ok(())
}

fn read_next_uncommitted_wal_operation_sync(store: &Store) -> anyhow::Result<Option<(u64, DatabaseCommand)>> {
    let mut txn = new_surrealkv_transaction(&store, Durability::Immediate)?;
    let last_committed_sequence_key = RedisWALKey::LastCommittedOperationSequence.as_bytes();

    // Fetch the last committed sequence
    let last_sequence: u64 = if let Some(last_bytes) = txn.get(&last_committed_sequence_key)? {
        serde_json::from_slice(&last_bytes)?
    } else {
        0 // Default to 0 if not present
    };

    // Get the next operation key
    let next_operation_key = RedisWALKey::GetKeyForOperationSequence(last_sequence + 1).as_bytes();

    // Retrieve the operation
    if let Some(operation_bytes) = txn.get(&next_operation_key)? {
        let operation: SerializableDatabaseCommand = serde_json::from_slice(&operation_bytes)?;

        // Only return uncommitted operations
        if !operation.committed_to_redis {
            return Ok(Some((
                last_sequence + 1,
                operation.try_into().map_err(|e: String| anyhow::anyhow!(e))?,
            )));
        }
    }

    Ok(None) // No uncommitted operations found
}

fn commit_message_sync(store: &Store, sequence: u64) -> anyhow::Result<()> {
    let mut txn = new_surrealkv_transaction(&store, Durability::Immediate)?;
    let last_committed_sequence_key = RedisWALKey::LastCommittedOperationSequence.as_bytes();
    let operation_key = RedisWALKey::GetKeyForOperationSequence(sequence).as_bytes();

    // Fetch and update the operation
    if let Some(existing_bytes) = txn.get(&operation_key)? {
        let mut operation: SerializableDatabaseCommand = serde_json::from_slice(&existing_bytes)?;

        // Check if the operation is already committed to Redis
        if operation.committed_to_redis {
            tracing::debug!(
                "Operation with sequence {} is already committed to Redis. Possible duplicate processing.",
                sequence
            );
            return Err(anyhow::anyhow!(
                "Operation with sequence {} is already committed to Redis. Aborting.",
                sequence
            ));
        }

        // Update the commit flag
        operation.committed_to_redis = true;

        // Write the updated operation back to the store
        txn.set(&operation_key, &serde_json::to_vec(&operation)?)?;
    } else {
        return Err(anyhow::anyhow!("Operation with sequence {} not found", sequence));
    }

    // Update the last committed sequence
    txn.set(&last_committed_sequence_key, &serde_json::to_vec(&sequence)?)?;

    // Commit the transaction synchronously with rollback on failure
    let result = tokio::task::block_in_place(|| {
        tokio::runtime::Handle::current().block_on(async { txn.commit().await })
    });

    if let Err(e) = result {
        tracing::error!("Transaction commit failed for sequence {}: {e}. Rolling back transaction.", sequence);
        txn.rollback();
        return Err(anyhow::anyhow!("Failed to commit operation with sequence {}: {e}", sequence));
    }

    Ok(())
}

fn is_redis_connection_alive(conn: &mut Connection) -> bool {
    match redis::cmd("PING").query::<String>(conn) {
        Ok(response) if response == "PONG" => true,
        _ => false,
    }
}

async fn shutdown_signal() {
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("Failed to register SIGTERM handler");
    let mut sigint = tokio::signal::ctrl_c();

    tokio::select! {
        _ = sigterm.recv() => {
            tracing::info!("SIGTERM received. Initiating graceful shutdown...");
        }
        _ = sigint => {
            tracing::info!("SIGINT (Ctrl+C) received. Initiating graceful shutdown...");
        }
    }
}

impl RedisCacheDatabase {
    /// Creates a new [`RedisCacheDatabase`] instance.
    pub fn new(
        trader_id: TraderId,
        instance_id: UUID4,
        config: CacheConfig,
    ) -> anyhow::Result<RedisCacheDatabase> {
        install_cryptographic_provider();

        let db_config = config
            .database
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("No database config"))?;
        let con = create_redis_connection(CACHE_READ, db_config.clone())?;

        // Store db_config in the global variable
        {
            let mut global_config = GLOBAL_DB_CONFIG.lock().unwrap();
            *global_config = Some(db_config.clone());
        }
    
        let (tx, rx) = tokio::sync::mpsc::channel::<DatabaseCommand>(CHANNEL_BUFFER_SIZE);
        let trader_key = get_trader_key(trader_id, instance_id, &config);
        let trader_key_clone = trader_key.clone();

        let handle = get_runtime().spawn(async move {
            process_commands(rx, trader_key_clone, config.clone())
                .await
                .expect("Error spawning task '{CACHE_WRITE}'")
        });

        Ok(RedisCacheDatabase {
            trader_id,
            trader_key,
            con,
            tx,
            handle,
        })
    }

    pub fn close(&mut self) {
        log::debug!("Closing");

        tokio::task::block_in_place(|| {
            let close_command = DatabaseCommand::close();
            if let Err(e) = get_runtime().block_on(self.tx.send(close_command)) {
                log::debug!("Error sending close message: {e:?}");
            }
        });

        log::debug!("Awaiting task '{CACHE_WRITE}'");
        tokio::task::block_in_place(|| {
            if let Err(e) = get_runtime().block_on(&mut self.handle) {
                log::error!("Error awaiting task '{CACHE_WRITE}': {e:?}");
            }
        });

        log::debug!("Closed");
    }

    pub fn flushdb(&mut self) {
        if let Err(e) = redis::cmd(REDIS_FLUSHDB).query::<()>(&mut self.con) {
            log::error!("Failed to flush database: {e:?}");
        }
    }

    pub fn keys(&mut self, pattern: &str) -> anyhow::Result<Vec<String>> {
        self.drain_messages_from_surrealkv()?; // Process pending messages from SurrealKV

        let pattern = format!("{}{REDIS_DELIMITER}{pattern}", self.trader_key);
        tracing::debug!("Querying keys: {pattern}");
        Ok(scan_keys(&mut self.con, pattern)?)
    }

    pub fn read(&mut self, key: &str) -> anyhow::Result<Vec<Bytes>> {
        if get_global_surreal_dirty_flag() {
            tracing::debug!("draining as surreal is dirty");
            self.drain_messages_from_surrealkv()?; // Process pending messages from SurrealKV
        } else {
            tracing::debug!("surreal is not dirty");
        }
        tracing::debug!("Reading keys");

        let collection = get_collection_key(key)?;
        let key = format!("{}{REDIS_DELIMITER}{}", self.trader_key, key);

        match collection {
            INDEX => read_index(&mut self.con, &key),
            GENERAL => read_string(&mut self.con, &key),
            CURRENCIES => read_string(&mut self.con, &key),
            INSTRUMENTS => read_string(&mut self.con, &key),
            SYNTHETICS => read_string(&mut self.con, &key),
            ACCOUNTS => read_list(&mut self.con, &key),
            ORDERS => read_list(&mut self.con, &key),
            POSITIONS => read_list(&mut self.con, &key),
            ACTORS => read_string(&mut self.con, &key),
            STRATEGIES => read_string(&mut self.con, &key),
            _ => anyhow::bail!("Unsupported operation: `read` for collection '{collection}'"),
        }
    }

    fn drain_messages_from_surrealkv(&mut self) -> anyhow::Result<()> {
        tracing::info!("Entering drain_messages_from_surrealkv for trader_key: {}", self.trader_key);
    
        let db_config = {
            let global_config = GLOBAL_DB_CONFIG.lock().unwrap();
            global_config.clone().ok_or_else(|| anyhow::anyhow!("Global db_config is not set"))?
        };
    
        tracing::info!("Using db_config: {:?}", db_config);
    
        TOKIO_RUNTIME.block_on(async {
            let store = create_surrealkv_store()?;
            loop {
                // Attempt to read the next message
                match read_next_uncommitted_wal_operation_sync(&store)? {
                    Some((counter, message)) => {
                        loop {
                            // Ensure Redis connection is alive
                            if !is_redis_connection_alive(&mut self.con) {
                                match create_redis_connection(CACHE_WRITE, db_config.clone()) {
                                    Ok(new_con) => {
                                        self.con = new_con;
                                        tracing::info!("Redis connection reestablished.");
                                    }
                                    Err(e) => {
                                        // tracing::error!("Failed to reconnect to Redis: {}. Retrying...", e);
                                        tokio::time::sleep(std::time::Duration::from_millis(RETRY_DELAY_MS)).await;
                                        continue;
                                    }
                                }
                            }
    
                            // Attempt to drain the message to Redis
                            match drain_single_message(&mut self.con, &self.trader_key, message.clone()) {
                                Ok(_) => {
                                    tracing::info!("Successfully drained message at counter {}", counter);
                                    break;
                                }
                                Err(e) => {
                                    tracing::warn!("Failed to drain message at counter {}: {}. Retrying...", counter, e);
                                    tokio::time::sleep(std::time::Duration::from_millis(RETRY_DELAY_MS)).await;
                                }
                            }
                        }
    
                        // Commit the message after successful draining
                        if let Err(e) = commit_message_sync(&store, counter) {
                            tracing::error!("Failed to commit message at counter {}: {}", counter, e);
                            break;
                        }
                    }
                    None => {
                        set_global_surreal_dirty_flag(false);
                        tracing::info!("No more messages to process. Exiting...");
                        break; // Exit the loop if there are no more messages
                    }
                }
            }
    
            tracing::info!("Exiting drain_messages_from_surrealkv for trader_key: {}", self.trader_key);
            Ok(())
        })
    }
    
    
    

    pub fn insert(&self, key: String, payload: Option<Vec<Bytes>>) -> anyhow::Result<()> {
        self.send_command(DatabaseOperation::Insert, key, payload)
    }

    pub fn update(&self, key: String, payload: Option<Vec<Bytes>>) -> anyhow::Result<()> {
        self.send_command(DatabaseOperation::Update, key, payload)
    }

    pub fn delete(&self, key: String, payload: Option<Vec<Bytes>>) -> anyhow::Result<()> {
        self.send_command(DatabaseOperation::Delete, key, payload)
    }

    fn send_command(
        &self,
        operation: DatabaseOperation,
        key: String,
        payload: Option<Vec<Bytes>>,
    ) -> anyhow::Result<()> {
        // Convert payload
        // Construct the DatabaseCommand
        let command = DatabaseCommand::new(operation, key, payload);

        // Ensure a runtime context
        let handle = tokio::runtime::Handle::try_current().unwrap_or_else(|_| {
            tokio::runtime::Runtime::new()
                .expect("Failed to create Tokio runtime")
                .handle()
                .clone()
        });

        // Use runtime to send the command
        let result = tokio::task::block_in_place(|| {
            handle.block_on(async {
                self.tx.send(command).await.map_err(|e| {
                    anyhow::anyhow!(format!("Failed to send command: {}", e))
                })
            })
        });

        // Handle result
        result.map_err(|e| anyhow::anyhow!(format!("Failed to process command: {}", e)))
    }
}

async fn process_commands(
    mut rx: tokio::sync::mpsc::Receiver<DatabaseCommand>, // Bounded channel
    trader_key: String,
    config: CacheConfig,
) -> anyhow::Result<()> {
    tracing::debug!("Starting cache processing");

    let db_config = config.database.as_ref().ok_or_else(|| anyhow::anyhow!("No database config"))?;
    let mut con = create_redis_connection(CACHE_WRITE, db_config.clone())?;
    let store = Arc::new(create_surrealkv_store()?);

    let buffer_interval = Duration::from_millis(config.buffer_interval_ms.unwrap_or(0) as u64);
    let notify = Arc::new(Notify::new());
    let shutdown_notify = notify.clone();

    // Spawn a task to listen for shutdown signals
    tokio::spawn({
        let notify = notify.clone();
        async move {
            shutdown_signal().await;
            notify.notify_one();
        }
    });

    // Main loop to handle writing to SurrealKV and periodic Redis operations
    loop {
        tokio::select! {
            // Handle incoming messages and write to SurrealKV
            Some(msg) = rx.recv() => {
                // Skip storing `DatabaseOperation::Close`
                if let DatabaseOperation::Close = msg.op_type {
                    tracing::info!("Received DatabaseOperation::Close. Skipping SurrealKV write.");
                    continue;
                }

                if let Err(e) = write_to_wal_store(&store, msg).await {
                    tracing::error!("Failed to write to SurrealKV: {}", e);
                }
            }

            // Periodically drain the buffer
            _ = tokio::time::sleep(buffer_interval) => {
                if is_redis_connection_alive(&mut con) {
                    while let Some((counter, message)) = read_next_uncommitted_wal_operation(&store).await? {
                        // Drain each message to Redis
                        if let Err(e) = drain_single_message(&mut con, &trader_key, message) {
                            tracing::error!("Failed to drain message at counter {}: {}", counter, e);
                            break;
                        }
                    
                        // Commit the message and update the counter atomically
                        if let Err(e) = commit_message(&store, counter).await {
                            tracing::error!("Failed to commit message at counter {}: {}", counter, e);
                            break;
                        }
                    }
                } else {
                    if let Ok(new_con) = create_redis_connection(CACHE_WRITE, db_config.clone()) {
                        con = new_con;
                        tracing::info!("Redis connection reestablished.");
                    } else {
                        // tracing::error!("Failed to reconnect to Redis. Retrying on the next interval.");
                        continue; // Skip draining on this iteration
                    }
                }
            }

            // Handle shutdown signal
            _ = shutdown_notify.notified() => {
                tracing::info!("Shutdown notifier triggered. Cleaning up...");
                break;
            }
        }
    }

    tracing::debug!("Stopped cache processing");
    Ok(())
}

fn drain_single_message(conn: &mut Connection, trader_key: &str, msg: DatabaseCommand) -> Result<()> {
    let mut pipe = redis::pipe();
    pipe.atomic();

    let key = msg.key.expect("Null command key");
    let collection = get_collection_key(&key)?;
    let redis_key = format!("{trader_key}{REDIS_DELIMITER}{}", &key);

    match msg.op_type {
        DatabaseOperation::Insert => {
            if let Some(payload) = msg.payload {
                insert(&mut pipe, collection, &redis_key, payload)?;
            }
        }
        DatabaseOperation::Update => {
            if let Some(payload) = msg.payload {
                update(&mut pipe, collection, &redis_key, payload)?;
            }
        }
        DatabaseOperation::Delete => {
            delete(&mut pipe, collection, &redis_key, msg.payload)?;
        }
        _ => {
            tracing::warn!("Unsupported operation in drain: {:?}", msg.op_type);
        }
    }

    pipe.query::<()>(conn).map_err(|e| anyhow::anyhow!("Redis pipeline query failed: {e}"))?;
    tracing::info!("Successfully processed command for key: {}", redis_key);
    Ok(())
}

// fn drain_buffer(conn: &mut Connection, trader_key: &str, buffer: &mut VecDeque<DatabaseCommand>) {
//     if !is_redis_connection_alive(conn) {
//         tracing::error!("Redis connection is not alive. Buffer remains in SurrealKV.");
//         return;
//     }

//     if !buffer.is_empty() {
//         tracing::info!("Processing combined buffer with {} commands.", buffer.len());
//         let mut pipe = redis::pipe();
//         pipe.atomic();

//         for (i, msg) in buffer.drain(..).enumerate() {
//             if msg.key.is_none() || msg.payload.is_none() {
//                 tracing::error!("Malformed command #{}", i + 1);
//                 continue;
//             }

//             let key = msg.key.expect("Null command key");
//             let collection = match get_collection_key(&key) {
//                 Ok(collection) => collection,
//                 Err(e) => {
//                     tracing::error!("Failed to get collection key for command #{}: {}", i + 1, e);
//                     continue;
//                 }
//             };

//             let key = format!("{trader_key}{REDIS_DELIMITER}{}", &key);

//             match msg.op_type {
//                 DatabaseOperation::Insert => {
//                     if let Some(payload) = msg.payload {
//                         if let Err(e) = insert(&mut pipe, collection, &key, payload) {
//                             tracing::error!("Failed to insert data for command #{}: {}", i + 1, e);
//                         }
//                     } else {
//                         tracing::error!("Null payload for insert in command #{}", i + 1);
//                     }
//                 }
//                 DatabaseOperation::Update => {
//                     if let Some(payload) = msg.payload {
//                         if let Err(e) = update(&mut pipe, collection, &key, payload) {
//                             tracing::error!("Failed to update data for command #{}: {}", i + 1, e);
//                         }
//                     } else {
//                         tracing::error!("Null payload for update in command #{}", i + 1);
//                     };
//                 }
//                 DatabaseOperation::Delete => {
//                     if let Err(e) = delete(&mut pipe, collection, &key, msg.payload) {
//                         tracing::error!("Failed to delete data for command #{}: {}", i + 1, e);
//                     }
//                 }
//                 DatabaseOperation::Close => {
//                     tracing::error!("Close command should not be drained. Command #{}", i + 1);
//                     continue;
//                 }
//             }
//         }

//         if let Err(e) = pipe.query::<()>(conn) {
//             tracing::error!("Redis pipeline query failed: {e}");
//         } else {
//             tracing::info!("Successfully processed all commands in the combined buffer.");
//         }
//     } else {
//         tracing::info!("No commands to process. Buffer is empty.");
//     }
// }

fn scan_keys(con: &mut Connection, pattern: String) -> Result<Vec<String>, RedisError> {
    tracing::info!("Entering scan_keys");

    let mut keys = Vec::new();
    let mut cursor: u64 = 0;

    loop {
        let (new_cursor, batch): (u64, Vec<String>) = redis
            ::cmd("SCAN")
            .cursor_arg(cursor)
            .arg("MATCH")
            .arg(&pattern) // Borrowing pattern
            .arg("COUNT")
            .arg(REDIS_SCAN_BATCH_SIZE) // Default batch size
            .query(con)?;

        keys.extend(batch);
        cursor = new_cursor;

        if cursor == 0 {
            break;
        }
    }

    tracing::info!("Exiting scan_keys with {} keys found", keys.len());
    Ok(keys)
}

fn read_index(conn: &mut Connection, key: &str) -> anyhow::Result<Vec<Bytes>> {
    let index_key = get_index_key(key)?;
    match index_key {
        INDEX_ORDER_IDS => read_set(conn, key),
        INDEX_ORDER_POSITION => read_hset(conn, key),
        INDEX_ORDER_CLIENT => read_hset(conn, key),
        INDEX_ORDERS => read_set(conn, key),
        INDEX_ORDERS_OPEN => read_set(conn, key),
        INDEX_ORDERS_CLOSED => read_set(conn, key),
        INDEX_ORDERS_EMULATED => read_set(conn, key),
        INDEX_ORDERS_INFLIGHT => read_set(conn, key),
        INDEX_POSITIONS => read_set(conn, key),
        INDEX_POSITIONS_OPEN => read_set(conn, key),
        INDEX_POSITIONS_CLOSED => read_set(conn, key),
        _ => anyhow::bail!("Index unknown '{index_key}' on read"),
    }
}

fn read_string(conn: &mut Connection, key: &str) -> anyhow::Result<Vec<Bytes>> {
    let result: Vec<u8> = conn.get(key)?;

    if result.is_empty() {
        Ok(vec![])
    } else {
        Ok(vec![Bytes::from(result)])
    }
}

fn read_set(conn: &mut Connection, key: &str) -> anyhow::Result<Vec<Bytes>> {
    let result: Vec<Bytes> = conn.smembers(key)?;
    Ok(result)
}

fn read_hset(conn: &mut Connection, key: &str) -> anyhow::Result<Vec<Bytes>> {
    let result: HashMap<String, String> = conn.hgetall(key)?;
    let json = serde_json::to_string(&result)?;
    Ok(vec![Bytes::from(json.into_bytes())])
}

fn read_list(conn: &mut Connection, key: &str) -> anyhow::Result<Vec<Bytes>> {
    let result: Vec<Bytes> = conn.lrange(key, 0, -1)?;
    Ok(result)
}

fn insert(
    pipe: &mut Pipeline,
    collection: &str,
    key: &str,
    value: Vec<Bytes>,
) -> anyhow::Result<()> {
    check_slice_not_empty(value.as_slice(), stringify!(value))?;

    match collection {
        INDEX => insert_index(pipe, key, &value),
        GENERAL => {
            insert_string(pipe, key, value[0].as_ref());
            Ok(())
        }
        CURRENCIES => {
            insert_string(pipe, key, value[0].as_ref());
            Ok(())
        }
        INSTRUMENTS => {
            insert_string(pipe, key, value[0].as_ref());
            Ok(())
        }
        SYNTHETICS => {
            insert_string(pipe, key, value[0].as_ref());
            Ok(())
        }
        ACCOUNTS => {
            insert_list(pipe, key, value[0].as_ref());
            Ok(())
        }
        ORDERS => {
            insert_list(pipe, key, value[0].as_ref());
            Ok(())
        }
        POSITIONS => {
            insert_list(pipe, key, value[0].as_ref());
            Ok(())
        }
        ACTORS => {
            insert_string(pipe, key, value[0].as_ref());
            Ok(())
        }
        STRATEGIES => {
            insert_string(pipe, key, value[0].as_ref());
            Ok(())
        }
        SNAPSHOTS => {
            insert_list(pipe, key, value[0].as_ref());
            Ok(())
        }
        HEALTH => {
            insert_string(pipe, key, value[0].as_ref());
            Ok(())
        }
        _ => anyhow::bail!("Unsupported operation: `insert` for collection '{collection}'"),
    }
}

fn insert_index(pipe: &mut Pipeline, key: &str, value: &[Bytes]) -> anyhow::Result<()> {
    let index_key = get_index_key(key)?;
    match index_key {
        INDEX_ORDER_IDS => {
            insert_set(pipe, key, value[0].as_ref());
            Ok(())
        }
        INDEX_ORDER_POSITION => {
            insert_hset(pipe, key, value[0].as_ref(), value[1].as_ref());
            Ok(())
        }
        INDEX_ORDER_CLIENT => {
            insert_hset(pipe, key, value[0].as_ref(), value[1].as_ref());
            Ok(())
        }
        INDEX_ORDERS => {
            insert_set(pipe, key, value[0].as_ref());
            Ok(())
        }
        INDEX_ORDERS_OPEN => {
            insert_set(pipe, key, value[0].as_ref());
            Ok(())
        }
        INDEX_ORDERS_CLOSED => {
            insert_set(pipe, key, value[0].as_ref());
            Ok(())
        }
        INDEX_ORDERS_EMULATED => {
            insert_set(pipe, key, value[0].as_ref());
            Ok(())
        }
        INDEX_ORDERS_INFLIGHT => {
            insert_set(pipe, key, value[0].as_ref());
            Ok(())
        }
        INDEX_POSITIONS => {
            insert_set(pipe, key, value[0].as_ref());
            Ok(())
        }
        INDEX_POSITIONS_OPEN => {
            insert_set(pipe, key, value[0].as_ref());
            Ok(())
        }
        INDEX_POSITIONS_CLOSED => {
            insert_set(pipe, key, value[0].as_ref());
            Ok(())
        }
        _ => anyhow::bail!("Index unknown '{index_key}' on insert"),
    }
}

fn insert_string(pipe: &mut Pipeline, key: &str, value: &[u8]) {
    pipe.set(key, value);
}

fn insert_set(pipe: &mut Pipeline, key: &str, value: &[u8]) {
    pipe.sadd(key, value);
}

fn insert_hset(pipe: &mut Pipeline, key: &str, name: &[u8], value: &[u8]) {
    pipe.hset(key, name, value);
}

fn insert_list(pipe: &mut Pipeline, key: &str, value: &[u8]) {
    pipe.rpush(key, value);
}

fn update(
    pipe: &mut Pipeline,
    collection: &str,
    key: &str,
    value: Vec<Bytes>,
) -> anyhow::Result<()> {
    check_slice_not_empty(value.as_slice(), stringify!(value))?;

    match collection {
        ACCOUNTS => {
            update_list(pipe, key, value[0].as_ref());
            Ok(())
        }
        ORDERS => {
            update_list(pipe, key, value[0].as_ref());
            Ok(())
        }
        POSITIONS => {
            update_list(pipe, key, value[0].as_ref());
            Ok(())
        }
        _ => anyhow::bail!("Unsupported operation: `update` for collection '{collection}'"),
    }
}

fn update_list(pipe: &mut Pipeline, key: &str, value: &[u8]) {
    pipe.rpush_exists(key, value);
}

fn delete(
    pipe: &mut Pipeline,
    collection: &str,
    key: &str,
    value: Option<Vec<Bytes>>,
) -> anyhow::Result<()> {
    match collection {
        INDEX => remove_index(pipe, key, value),
        ACTORS => {
            delete_string(pipe, key);
            Ok(())
        }
        STRATEGIES => {
            delete_string(pipe, key);
            Ok(())
        }
        _ => anyhow::bail!("Unsupported operation: `delete` for collection '{collection}'"),
    }
}

fn remove_index(pipe: &mut Pipeline, key: &str, value: Option<Vec<Bytes>>) -> anyhow::Result<()> {
    let value = value.ok_or_else(|| anyhow::anyhow!("Empty `payload` for `delete` '{key}'"))?;
    let index_key = get_index_key(key)?;

    match index_key {
        INDEX_ORDERS_OPEN => {
            remove_from_set(pipe, key, value[0].as_ref());
            Ok(())
        }
        INDEX_ORDERS_CLOSED => {
            remove_from_set(pipe, key, value[0].as_ref());
            Ok(())
        }
        INDEX_ORDERS_EMULATED => {
            remove_from_set(pipe, key, value[0].as_ref());
            Ok(())
        }
        INDEX_ORDERS_INFLIGHT => {
            remove_from_set(pipe, key, value[0].as_ref());
            Ok(())
        }
        INDEX_POSITIONS_OPEN => {
            remove_from_set(pipe, key, value[0].as_ref());
            Ok(())
        }
        INDEX_POSITIONS_CLOSED => {
            remove_from_set(pipe, key, value[0].as_ref());
            Ok(())
        }
        _ => anyhow::bail!("Unsupported index operation: remove from '{index_key}'"),
    }
}

fn remove_from_set(pipe: &mut Pipeline, key: &str, member: &[u8]) {
    pipe.srem(key, member);
}

fn delete_string(pipe: &mut Pipeline, key: &str) {
    pipe.del(key);
}

fn get_trader_key(trader_id: TraderId, instance_id: UUID4, config: &CacheConfig) -> String {
    let mut key = String::new();

    if config.use_trader_prefix {
        key.push_str("trader-");
    }

    key.push_str(trader_id.as_str());

    if config.use_instance_id {
        key.push(REDIS_DELIMITER);
        key.push_str(&format!("{instance_id}"));
    }

    key
}

fn get_collection_key(key: &str) -> anyhow::Result<&str> {
    key.split_once(REDIS_DELIMITER)
        .map(|(collection, _)| collection)
        .ok_or_else(|| {
            anyhow::anyhow!("Invalid `key`, missing a '{REDIS_DELIMITER}' delimiter, was {key}")
        })
}

fn get_index_key(key: &str) -> anyhow::Result<&str> {
    key.split_once(REDIS_DELIMITER)
        .map(|(_, index_key)| index_key)
        .ok_or_else(|| {
            anyhow::anyhow!("Invalid `key`, missing a '{REDIS_DELIMITER}' delimiter, was {key}")
        })
}

// This function can be used when we handle cache serialization in Rust
#[allow(dead_code)]
fn get_encoding(config: &HashMap<String, serde_json::Value>) -> String {
    config
        .get("encoding")
        .and_then(|v| v.as_str())
        .unwrap_or("msgpack")
        .to_string()
}

// This function can be used when we handle cache serialization in Rust
#[allow(dead_code)]
fn deserialize_payload(
    encoding: &str,
    payload: &[u8],
) -> anyhow::Result<HashMap<String, serde_json::Value>> {
    match encoding {
        "msgpack" => rmp_serde::from_slice(payload)
            .map_err(|e| anyhow::anyhow!("Failed to deserialize msgpack `payload`: {e}")),
        "json" => serde_json::from_slice(payload)
            .map_err(|e| anyhow::anyhow!("Failed to deserialize json `payload`: {e}")),
        _ => Err(anyhow::anyhow!("Unsupported encoding: {encoding}")),
    }
}

#[allow(dead_code)] // Under development
pub struct RedisCacheDatabaseAdapter {
    pub encoding: SerializationEncoding,
    database: RedisCacheDatabase,
}

#[allow(dead_code)] // Under development
#[allow(unused)] // Under development
impl CacheDatabaseAdapter for RedisCacheDatabaseAdapter {
    fn close(&mut self) -> anyhow::Result<()> {
        self.database.close();
        Ok(())
    }

    fn flush(&mut self) -> anyhow::Result<()> {
        self.database.flushdb();
        Ok(())
    }

    fn load(&self) -> anyhow::Result<HashMap<String, Bytes>> {
        // self.database.load()
        Ok(HashMap::new()) // TODO
    }

    fn load_currencies(&mut self) -> anyhow::Result<HashMap<Ustr, Currency>> {
        let mut currencies = HashMap::new();
        let pattern = format!("{CURRENCIES}*");

        for key in scan_keys(&mut self.database.con, pattern)? {
            let parts: Vec<&str> = key.as_str().rsplitn(2, ':').collect();
            let currency_code = Ustr::from(parts.first().unwrap());
            let result = self.load_currency(&currency_code)?;
            match result {
                Some(currency) => {
                    currencies.insert(currency_code, currency);
                }
                None => {
                    log::error!("Currency not found: {currency_code}");
                }
            }
        }
        Ok(currencies)
    }

    fn load_instruments(&mut self) -> anyhow::Result<HashMap<InstrumentId, InstrumentAny>> {
        let mut instruments = HashMap::new();
        let pattern = format!("{INSTRUMENTS}*");

        for key in scan_keys(&mut self.database.con, pattern)? {
            let parts: Vec<&str> = key.as_str().rsplitn(2, ':').collect();
            let instrument_id = InstrumentId::from_str(parts.first().unwrap())?;
            let result = self.load_instrument(&instrument_id)?;
            match result {
                Some(instrument) => {
                    instruments.insert(instrument_id, instrument);
                }
                None => {
                    log::error!("Instrument not found: {instrument_id}");
                }
            }
        }

        Ok(instruments)
    }

    fn load_synthetics(&mut self) -> anyhow::Result<HashMap<InstrumentId, SyntheticInstrument>> {
        let mut synthetics = HashMap::new();
        let pattern = format!("{SYNTHETICS}*");

        for key in scan_keys(&mut self.database.con, pattern)? {
            let parts: Vec<&str> = key.as_str().rsplitn(2, ':').collect();
            let instrument_id = InstrumentId::from_str(parts.first().unwrap())?;
            let synthetic = self.load_synthetic(&instrument_id)?;
            synthetics.insert(instrument_id, synthetic);
        }

        Ok(synthetics)
    }

    fn load_accounts(&mut self) -> anyhow::Result<HashMap<AccountId, AccountAny>> {
        let mut accounts = HashMap::new();
        let pattern = format!("{ACCOUNTS}*");

        for key in scan_keys(&mut self.database.con, pattern)? {
            let parts: Vec<&str> = key.as_str().rsplitn(2, ':').collect();
            let account_id = AccountId::from(*parts.first().unwrap());
            let result = self.load_account(&account_id)?;
            match result {
                Some(account) => {
                    accounts.insert(account_id, account);
                }
                None => {
                    log::error!("Account not found: {account_id}");
                }
            }
        }

        Ok(accounts)
    }

    fn load_orders(&mut self) -> anyhow::Result<HashMap<ClientOrderId, OrderAny>> {
        let mut orders = HashMap::new();
        let pattern = format!("{ORDERS}*");

        for key in scan_keys(&mut self.database.con, pattern)? {
            let parts: Vec<&str> = key.as_str().rsplitn(2, ':').collect();
            let client_order_id = ClientOrderId::from(*parts.first().unwrap());
            let result = self.load_order(&client_order_id)?;
            match result {
                Some(order) => {
                    orders.insert(client_order_id, order);
                }
                None => {
                    log::error!("Order not found: {client_order_id}");
                }
            }
        }
        Ok(orders)
    }

    fn load_positions(&mut self) -> anyhow::Result<HashMap<PositionId, Position>> {
        self.database.drain_messages_from_surrealkv()?;
        let mut positions = HashMap::new();
        let pattern = format!("{POSITIONS}*");

        for key in scan_keys(&mut self.database.con, pattern)? {
            let parts: Vec<&str> = key.as_str().rsplitn(2, ':').collect();
            let position_id = PositionId::from(*parts.first().unwrap());
            let position = self.load_position(&position_id)?;
            positions.insert(position_id, position);
        }

        Ok(positions)
    }

    fn load_index_order_position(&self) -> anyhow::Result<HashMap<ClientOrderId, Position>> {
        todo!()
    }

    fn load_index_order_client(&self) -> anyhow::Result<HashMap<ClientOrderId, ClientId>> {
        todo!()
    }

    fn load_currency(&self, code: &Ustr) -> anyhow::Result<Option<Currency>> {
        todo!()
    }

    fn load_instrument(
        &self,
        instrument_id: &InstrumentId,
    ) -> anyhow::Result<Option<InstrumentAny>> {
        todo!()
    }

    fn load_synthetic(&self, instrument_id: &InstrumentId) -> anyhow::Result<SyntheticInstrument> {
        todo!()
    }

    fn load_account(&self, account_id: &AccountId) -> anyhow::Result<Option<AccountAny>> {
        todo!()
    }

    fn load_order(&self, client_order_id: &ClientOrderId) -> anyhow::Result<Option<OrderAny>> {
        todo!()
    }

    fn load_position(&self, position_id: &PositionId) -> anyhow::Result<Position> {
        todo!()
    }

    fn load_actor(&self, component_id: &ComponentId) -> anyhow::Result<HashMap<String, Bytes>> {
        todo!()
    }

    fn delete_actor(&self, component_id: &ComponentId) -> anyhow::Result<()> {
        todo!()
    }

    fn load_strategy(&self, strategy_id: &StrategyId) -> anyhow::Result<HashMap<String, Bytes>> {
        todo!()
    }

    fn delete_strategy(&self, component_id: &StrategyId) -> anyhow::Result<()> {
        todo!()
    }

    fn add(&self, key: String, value: Bytes) -> anyhow::Result<()> {
        todo!()
    }

    fn add_currency(&self, currency: &Currency) -> anyhow::Result<()> {
        todo!()
    }

    fn add_instrument(&self, instrument: &InstrumentAny) -> anyhow::Result<()> {
        todo!()
    }

    fn add_synthetic(&self, synthetic: &SyntheticInstrument) -> anyhow::Result<()> {
        todo!()
    }

    fn add_account(&self, account: &AccountAny) -> anyhow::Result<()> {
        todo!()
    }

    fn add_order(&self, order: &OrderAny, client_id: Option<ClientId>) -> anyhow::Result<()> {
        todo!()
    }

    fn add_position(&self, position: &Position) -> anyhow::Result<()> {
        todo!()
    }

    fn add_position_snapshot(&self, snapshot: &PositionSnapshot) -> anyhow::Result<()> {
        todo!()
    }

    fn add_order_book(&self, order_book: &OrderBook) -> anyhow::Result<()> {
        anyhow::bail!("Saving market data for Redis cache adapter not supported")
    }

    fn add_quote(&self, quote: &QuoteTick) -> anyhow::Result<()> {
        anyhow::bail!("Saving market data for Redis cache adapter not supported")
    }

    fn load_quotes(&self, instrument_id: &InstrumentId) -> anyhow::Result<Vec<QuoteTick>> {
        anyhow::bail!("Loading quote data for Redis cache adapter not supported")
    }

    fn add_trade(&self, trade: &TradeTick) -> anyhow::Result<()> {
        anyhow::bail!("Saving market data for Redis cache adapter not supported")
    }

    fn load_trades(&self, instrument_id: &InstrumentId) -> anyhow::Result<Vec<TradeTick>> {
        anyhow::bail!("Loading market data for Redis cache adapter not supported")
    }

    fn add_bar(&self, bar: &Bar) -> anyhow::Result<()> {
        anyhow::bail!("Saving market data for Redis cache adapter not supported")
    }

    fn load_bars(&self, instrument_id: &InstrumentId) -> anyhow::Result<Vec<Bar>> {
        anyhow::bail!("Loading market data for Redis cache adapter not supported")
    }

    fn add_signal(&self, signal: &Signal) -> anyhow::Result<()> {
        anyhow::bail!("Saving signals for Redis cache adapter not supported")
    }

    fn load_signals(&self, name: &str) -> anyhow::Result<Vec<Signal>> {
        anyhow::bail!("Loading signals from Redis cache adapter not supported")
    }

    fn add_custom_data(&self, data: &CustomData) -> anyhow::Result<()> {
        anyhow::bail!("Saving custom data for Redis cache adapter not supported")
    }

    fn load_custom_data(&self, data_type: &DataType) -> anyhow::Result<Vec<CustomData>> {
        anyhow::bail!("Loading custom data from Redis cache adapter not supported")
    }

    fn index_venue_order_id(
        &self,
        client_order_id: ClientOrderId,
        venue_order_id: VenueOrderId,
    ) -> anyhow::Result<()> {
        todo!()
    }

    fn index_order_position(
        &self,
        client_order_id: ClientOrderId,
        position_id: PositionId,
    ) -> anyhow::Result<()> {
        todo!()
    }

    fn update_actor(&self) -> anyhow::Result<()> {
        todo!()
    }

    fn update_strategy(&self) -> anyhow::Result<()> {
        todo!()
    }

    fn update_account(&self, account: &AccountAny) -> anyhow::Result<()> {
        todo!()
    }

    fn update_order(&self, order_event: &OrderEventAny) -> anyhow::Result<()> {
        todo!()
    }

    fn update_position(&self, position: &Position) -> anyhow::Result<()> {
        todo!()
    }

    fn snapshot_order_state(&self, order: &OrderAny) -> anyhow::Result<()> {
        todo!()
    }

    fn snapshot_position_state(&self, position: &Position) -> anyhow::Result<()> {
        todo!()
    }

    fn heartbeat(&self, timestamp: UnixNanos) -> anyhow::Result<()> {
        todo!()
    }
}

////////////////////////////////////////////////////////////////////////////////
// Tests
////////////////////////////////////////////////////////////////////////////////
#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn test_get_trader_key_with_prefix_and_instance_id() {
        let trader_id = TraderId::from("tester-123");
        let instance_id = UUID4::new();
        let mut config = CacheConfig::default();
        config.use_instance_id = true;

        let key = get_trader_key(trader_id, instance_id, &config);
        assert!(key.starts_with("trader-tester-123:"));
        assert!(key.ends_with(&instance_id.to_string()));
    }

    #[rstest]
    fn test_get_collection_key_valid() {
        let key = "collection:123";
        assert_eq!(get_collection_key(key).unwrap(), "collection");
    }

    #[rstest]
    fn test_get_collection_key_invalid() {
        let key = "no_delimiter";
        assert!(get_collection_key(key).is_err());
    }

    #[rstest]
    fn test_get_index_key_valid() {
        let key = "index:123";
        assert_eq!(get_index_key(key).unwrap(), "123");
    }

    #[rstest]
    fn test_get_index_key_invalid() {
        let key = "no_delimiter";
        assert!(get_index_key(key).is_err());
    }
}
