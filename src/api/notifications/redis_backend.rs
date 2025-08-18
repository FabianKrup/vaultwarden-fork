/*!
 * Redis WebSocket Backend Implementation
 * 
 * This provides a Redis-based implementation for WebSocket connections,
 * enabling horizontal scaling of Vaultwarden instances.
 * 
 * The implementation uses:
 * - Redis Pub/Sub for message broadcasting
 * - Redis Hash Maps for connection tracking
 * - Local MPSC channels for WebSocket message delivery
 */

#[cfg(feature = "redis-websockets")]
mod redis_impl {
    use std::{collections::HashMap, sync::Arc, time::{Duration, Instant}};
    use log::{error, info, warn};
    use rocket::futures::StreamExt;
    use tokio::{
        sync::{mpsc::Sender, RwLock},
        time::timeout,
    };
    use rocket_ws::Message;
    use redis::{AsyncCommands, Client, ConnectionManager, RedisResult, Script};
    use serde_json;
    use uuid::Uuid;
    use data_encoding::BASE64;

    use crate::{db::models::{UserId, AuthRequestId}, CONFIG};
    use super::backends::{WebSocketBackend, AnonymousWebSocketBackend};

    const USER_CONNECTIONS_PREFIX: &str = "vw:ws:users:";
    const ANONYMOUS_CONNECTIONS_PREFIX: &str = "vw:ws:anon:";
    const PUBSUB_CHANNEL_PREFIX: &str = "vw:ws:msg:";
    const ANONYMOUS_PUBSUB_CHANNEL_PREFIX: &str = "vw:ws:anon:msg:";

    /// Health status of Redis connection
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum RedisHealthStatus {
        Healthy,
        Unhealthy,
        Unknown,
    }

    /// Health information for Redis connection
    #[derive(Debug, Clone)]
    pub struct RedisHealthInfo {
        pub status: RedisHealthStatus,
        pub last_check: Instant,
        pub consecutive_failures: u32,
        pub last_error: Option<String>,
    }

    impl Default for RedisHealthInfo {
        fn default() -> Self {
            Self {
                status: RedisHealthStatus::Unknown,
                last_check: Instant::now(),
                consecutive_failures: 0,
                last_error: None,
            }
        }
    }

    type LocalConnections = Arc<RwLock<HashMap<Uuid, Sender<Message>>>>;

    #[derive(Clone)]
    pub struct RedisWebSocketBackend {
        redis_manager: Arc<ConnectionManager>,
        // Local storage for active WebSocket senders on this instance
        local_connections: LocalConnections,
        _pubsub_task: Arc<tokio::task::JoinHandle<()>>,
        redis_timeout: Duration,
        fallback_to_memory: bool,
        health_check_interval: Duration,
        health_info: Arc<RwLock<RedisHealthInfo>>,
        _health_check_task: Arc<tokio::task::JoinHandle<()>>,
    }

    impl RedisWebSocketBackend {
        pub async fn new(redis_url: &str) -> Result<Self, redis::RedisError> {
            let client = Client::open(redis_url)?;
            let manager = ConnectionManager::new(client.clone()).await?;
            let local_connections = Arc::new(RwLock::new(HashMap::new()));
            
            // Get config values once during initialization
            let redis_timeout = Duration::from_secs(CONFIG.redis_websocket_timeout());
            let fallback_to_memory = CONFIG.redis_websocket_fallback_memory();
            let health_check_interval = Duration::from_secs(CONFIG.redis_websocket_health_check_interval());
            
            // Initialize health info
            let health_info = Arc::new(RwLock::new(RedisHealthInfo::default()));
            
            // Start pubsub listener task
            let pubsub_connections = Arc::clone(&local_connections);
            let pubsub_client = client.clone();
            let pubsub_task = tokio::spawn(async move {
                Self::pubsub_listener(pubsub_client, pubsub_connections).await;
            });

            // Start health check task
            let health_check_manager = Arc::new(manager.clone());
            let health_check_info = Arc::clone(&health_info);
            let health_check_timeout = redis_timeout;
            let health_check_task = tokio::spawn(async move {
                Self::health_check_loop(
                    health_check_manager,
                    health_check_info,
                    health_check_interval,
                    health_check_timeout,
                ).await;
            });

            info!("Redis WebSocket backend initialized with {}s timeout, {}s health checks, fallback: {}", 
                  redis_timeout.as_secs(), health_check_interval.as_secs(), fallback_to_memory);
            
            Ok(Self {
                redis_manager: Arc::new(manager),
                local_connections,
                _pubsub_task: Arc::new(pubsub_task),
                redis_timeout,
                fallback_to_memory,
                health_check_interval,
                health_info,
                _health_check_task: Arc::new(health_check_task),
            })
        }

        async fn pubsub_listener(client: Client, local_connections: LocalConnections) {
            loop {
                match client.get_async_connection().await {
                    Ok(mut conn) => {
                        let mut pubsub = conn.into_pubsub();
                        
                        // Subscribe to all user message channels
                        if let Err(e) = pubsub.psubscribe(&format!("{}*", PUBSUB_CHANNEL_PREFIX)).await {
                            error!("Failed to subscribe to Redis pub/sub: {}", e);
                            tokio::time::sleep(Duration::from_secs(5)).await;
                            continue;
                        }

                        info!("Redis pub/sub listener started");

                        loop {
                            match pubsub.on_message().next().await {
                                Some(msg) => {
                                    if let Ok(channel) = msg.get_channel_name::<String>() {
                                        if let Ok(payload) = msg.get_payload::<Vec<u8>>() {
                                            Self::handle_pubsub_message(&channel, &payload, &local_connections).await;
                                        }
                                    }
                                }
                                None => {
                                    warn!("Redis pub/sub connection lost, reconnecting...");
                                    break;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        error!("Failed to connect to Redis for pub/sub: {}", e);
                        tokio::time::sleep(Duration::from_secs(5)).await;
                    }
                }
            }
        }

        async fn handle_pubsub_message(channel: &str, payload: &[u8], local_connections: &LocalConnections) {
            // Extract connection UUIDs for this user from the channel
            if let Some(user_connections_str) = channel.strip_prefix(PUBSUB_CHANNEL_PREFIX) {
                // Parse message metadata to get connection UUIDs
                if let Ok(message_data) = serde_json::from_slice::<serde_json::Value>(payload) {
                    if let Some(connection_uuids) = message_data.get("connection_uuids").and_then(|v| v.as_array()) {
                        let connections = local_connections.read().await;
                        
                        for uuid_val in connection_uuids {
                            if let Some(uuid_str) = uuid_val.as_str() {
                                if let Ok(uuid) = Uuid::parse_str(uuid_str) {
                                    if let Some(sender) = connections.get(&uuid) {
                                        if let Some(data) = message_data.get("data").and_then(|v| v.as_str()) {
                                            if let Ok(decoded_data) = BASE64.decode(data.as_bytes()) {
                                                if let Err(e) = sender.send(Message::binary(&decoded_data)).await {
                                                    error!("Error sending WS update via Redis: {}", e);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        /// Health check loop that runs periodically
        async fn health_check_loop(
            manager: Arc<ConnectionManager>,
            health_info: Arc<RwLock<RedisHealthInfo>>,
            interval: Duration,
            timeout: Duration,
        ) {
            let mut timer = tokio::time::interval(interval);
            
            loop {
                timer.tick().await;
                
                match Self::perform_health_check(&manager, timeout).await {
                    Ok(()) => {
                        let mut info = health_info.write().await;
                        if info.status != RedisHealthStatus::Healthy {
                            info!("Redis connection recovered after {} consecutive failures", info.consecutive_failures);
                        }
                        info.status = RedisHealthStatus::Healthy;
                        info.last_check = Instant::now();
                        info.consecutive_failures = 0;
                        info.last_error = None;
                    }
                    Err(e) => {
                        let mut info = health_info.write().await;
                        info.status = RedisHealthStatus::Unhealthy;
                        info.last_check = Instant::now();
                        info.consecutive_failures += 1;
                        info.last_error = Some(e.to_string());
                        
                        if info.consecutive_failures <= 3 {
                            warn!("Redis health check failed (attempt {}): {}", info.consecutive_failures, e);
                        } else {
                            error!("Redis health check failed {} consecutive times: {}", info.consecutive_failures, e);
                        }
                    }
                }
            }
        }

        /// Perform a single health check by sending PING command
        async fn perform_health_check(manager: &ConnectionManager, check_timeout: Duration) -> RedisResult<()> {
            let mut conn = timeout(check_timeout, manager.clone().get_async_connection()).await
                .map_err(|_| redis::RedisError::from((redis::ErrorKind::IoError, "Health check timeout")))??;
            
            // Send PING command
            let response: String = conn.ping().await?;
            
            if response != "PONG" {
                return Err(redis::RedisError::from((
                    redis::ErrorKind::ResponseError, 
                    "Unexpected PING response",
                )));
            }
            
            Ok(())
        }

        /// Get current health status
        pub async fn get_health_info(&self) -> RedisHealthInfo {
            self.health_info.read().await.clone()
        }

        /// Check if Redis is currently healthy
        pub async fn is_healthy(&self) -> bool {
            self.health_info.read().await.status == RedisHealthStatus::Healthy
        }

        async fn get_user_connections(&self, user_id: &UserId) -> RedisResult<Vec<Uuid>> {
            let mut conn = timeout(self.redis_timeout, self.redis_manager.clone().get_async_connection()).await
                .map_err(|_| redis::RedisError::from((redis::ErrorKind::IoError, "Redis timeout")))??;
            
            let key = format!("{}{}", USER_CONNECTIONS_PREFIX, user_id);
            let connections: Vec<String> = conn.smembers(&key).await?;
            
            let mut uuids = Vec::new();
            for conn_str in connections {
                if let Ok(uuid) = Uuid::parse_str(&conn_str) {
                    uuids.push(uuid);
                }
            }
            Ok(uuids)
        }

        async fn add_user_connection(&self, user_id: &UserId, entry_uuid: Uuid) -> RedisResult<()> {
            let mut conn = timeout(self.redis_timeout, self.redis_manager.clone().get_async_connection()).await
                .map_err(|_| redis::RedisError::from((redis::ErrorKind::IoError, "Redis timeout")))??;
            
            let key = format!("{}{}", USER_CONNECTIONS_PREFIX, user_id);
            
            // Use MULTI/EXEC transaction to atomically add connection and set expiration
            // This prevents race conditions where the key could be deleted between SADD and EXPIRE
            redis::pipe()
                .atomic()
                .sadd(&key, entry_uuid.to_string())
                .expire(&key, 3600)
                .query_async(&mut conn)
                .await?;
            
            Ok(())
        }

        async fn remove_user_connection(&self, user_id: &UserId, entry_uuid: Uuid) -> RedisResult<()> {
            let mut conn = timeout(self.redis_timeout, self.redis_manager.clone().get_async_connection()).await
                .map_err(|_| redis::RedisError::from((redis::ErrorKind::IoError, "Redis timeout")))??;
            
            let key = format!("{}{}", USER_CONNECTIONS_PREFIX, user_id);
            conn.srem(&key, entry_uuid.to_string()).await?;
            Ok(())
        }

        async fn publish_update(&self, user_id: &UserId, data: &[u8]) -> RedisResult<()> {
            let mut conn = timeout(self.redis_timeout, self.redis_manager.clone().get_async_connection()).await
                .map_err(|_| redis::RedisError::from((redis::ErrorKind::IoError, "Redis timeout")))??;
            
            let key = format!("{}{}", USER_CONNECTIONS_PREFIX, user_id);
            let channel = format!("{}{}", PUBSUB_CHANNEL_PREFIX, user_id);
            let encoded_data = BASE64.encode(data);
            
            // Use Lua script to atomically get connection UUIDs and publish message
            // This completely eliminates race conditions between getting UUIDs and publishing
            let lua_script = r#"
                local key = KEYS[1]
                local channel = KEYS[2] 
                local encoded_data = ARGV[1]
                
                local uuids = redis.call('SMEMBERS', key)
                if #uuids > 0 then
                    local message_data = {
                        connection_uuids = uuids,
                        data = encoded_data
                    }
                    local message_json = cjson.encode(message_data)
                    redis.call('PUBLISH', channel, message_json)
                    return #uuids
                end
                return 0
            "#;
            
            let published_count: i32 = Script::new(lua_script)
                .key(&key)
                .key(&channel)
                .arg(&encoded_data)
                .invoke_async(&mut conn)
                .await?;
            
            if published_count > 0 {
                info!("Published WebSocket update to {} connections for user {}", published_count, user_id);
            }
            
            Ok(())
        }
    }

    impl WebSocketBackend for RedisWebSocketBackend {
        async fn add_connection(&self, user_id: &UserId, entry_uuid: uuid::Uuid, sender: Sender<Message>) {
            // Store locally for this instance
            {
                let mut connections = self.local_connections.write().await;
                connections.insert(entry_uuid, sender);
            }
            
            // Register in Redis
            if let Err(e) = self.add_user_connection(user_id, entry_uuid).await {
                error!("Failed to register WebSocket connection in Redis: {}", e);
                
                // Fallback: remove from local connections if Redis fails
                if self.fallback_to_memory {
                    let mut connections = self.local_connections.write().await;
                    connections.remove(&entry_uuid);
                }
            }
        }
        
        async fn remove_connection(&self, user_id: &UserId, entry_uuid: uuid::Uuid) {
            // Remove locally
            {
                let mut connections = self.local_connections.write().await;
                connections.remove(&entry_uuid);
            }
            
            // Remove from Redis
            if let Err(e) = self.remove_user_connection(user_id, entry_uuid).await {
                error!("Failed to remove WebSocket connection from Redis: {}", e);
            }
        }
        
        async fn send_update(&self, user_id: &UserId, data: &[u8]) {
            // Check if Redis is healthy before attempting to use it
            if !self.is_healthy().await && self.fallback_to_memory {
                warn!("Redis is unhealthy, using memory backend fallback for user {}", user_id);
                let connections = self.local_connections.read().await;
                for (_, sender) in connections.iter() {
                    if let Err(e) = sender.send(Message::binary(data)).await {
                        error!("Error sending local WS update: {}", e);
                    }
                }
                return;
            }
            
            // Try to send via Redis pub/sub
            if let Err(e) = self.publish_update(user_id, data).await {
                error!("Failed to publish WebSocket update to Redis: {}", e);
                
                // Fallback to local connections only if configured
                if self.fallback_to_memory {
                    warn!("Falling back to local WebSocket connections only");
                    let connections = self.local_connections.read().await;
                    for (_, sender) in connections.iter() {
                        if let Err(e) = sender.send(Message::binary(data)).await {
                            error!("Error sending local WS update: {}", e);
                        }
                    }
                }
            }
        }
    }

    #[derive(Clone)]
    pub struct RedisAnonymousWebSocketBackend {
        redis_manager: Arc<ConnectionManager>,
        local_connections: Arc<RwLock<HashMap<String, Sender<Message>>>>,
        _pubsub_task: Arc<tokio::task::JoinHandle<()>>,
        redis_timeout: Duration,
        fallback_to_memory: bool,
        health_check_interval: Duration,
        health_info: Arc<RwLock<RedisHealthInfo>>,
        _health_check_task: Arc<tokio::task::JoinHandle<()>>,
    }

    impl RedisAnonymousWebSocketBackend {
        pub async fn new(redis_url: &str) -> Result<Self, redis::RedisError> {
            let client = Client::open(redis_url)?;
            let manager = ConnectionManager::new(client.clone()).await?;
            let local_connections = Arc::new(RwLock::new(HashMap::new()));
            
            // Get config values once during initialization  
            let redis_timeout = Duration::from_secs(CONFIG.redis_websocket_timeout());
            let fallback_to_memory = CONFIG.redis_websocket_fallback_memory();
            let health_check_interval = Duration::from_secs(CONFIG.redis_websocket_health_check_interval());
            
            // Initialize health info
            let health_info = Arc::new(RwLock::new(RedisHealthInfo::default()));
            
            // Start pubsub listener for anonymous connections
            let pubsub_connections = Arc::clone(&local_connections);
            let pubsub_client = client.clone();
            let pubsub_task = tokio::spawn(async move {
                Self::pubsub_listener(pubsub_client, pubsub_connections).await;
            });

            // Start health check task (shared with main backend)
            let health_check_manager = Arc::new(manager.clone());
            let health_check_info = Arc::clone(&health_info);
            let health_check_timeout = redis_timeout;
            let health_check_task = tokio::spawn(async move {
                RedisWebSocketBackend::health_check_loop(
                    health_check_manager,
                    health_check_info,
                    health_check_interval,
                    health_check_timeout,
                ).await;
            });

            info!("Redis anonymous WebSocket backend initialized with {}s timeout, {}s health checks, fallback: {}", 
                  redis_timeout.as_secs(), health_check_interval.as_secs(), fallback_to_memory);
            
            Ok(Self {
                redis_manager: Arc::new(manager),
                local_connections,
                _pubsub_task: Arc::new(pubsub_task),
                redis_timeout,
                fallback_to_memory,
                health_check_interval,
                health_info,
                _health_check_task: Arc::new(health_check_task),
            })
        }

        async fn pubsub_listener(client: Client, local_connections: Arc<RwLock<HashMap<String, Sender<Message>>>>) {
            loop {
                match client.get_async_connection().await {
                    Ok(mut conn) => {
                        let mut pubsub = conn.into_pubsub();
                        
                        // Subscribe to anonymous message channels
                        if let Err(e) = pubsub.psubscribe(&format!("{}*", ANONYMOUS_PUBSUB_CHANNEL_PREFIX)).await {
                            error!("Failed to subscribe to anonymous Redis pub/sub: {}", e);
                            tokio::time::sleep(Duration::from_secs(5)).await;
                            continue;
                        }

                        info!("Redis anonymous pub/sub listener started");

                        loop {
                            match pubsub.on_message().next().await {
                                Some(msg) => {
                                    if let Ok(channel) = msg.get_channel_name::<String>() {
                                        if let Ok(payload) = msg.get_payload::<Vec<u8>>() {
                                            Self::handle_anonymous_pubsub_message(&channel, &payload, &local_connections).await;
                                        }
                                    }
                                }
                                None => {
                                    warn!("Redis anonymous pub/sub connection lost, reconnecting...");
                                    break;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        error!("Failed to connect to Redis for anonymous pub/sub: {}", e);
                        tokio::time::sleep(Duration::from_secs(5)).await;
                    }
                }
            }
        }

        async fn handle_anonymous_pubsub_message(channel: &str, payload: &[u8], local_connections: &Arc<RwLock<HashMap<String, Sender<Message>>>>) {
            if let Some(token) = channel.strip_prefix(ANONYMOUS_PUBSUB_CHANNEL_PREFIX) {
                if let Ok(message_data) = serde_json::from_slice::<serde_json::Value>(payload) {
                    if let Some(data) = message_data.get("data").and_then(|v| v.as_str()) {
                        if let Ok(decoded_data) = BASE64.decode(data.as_bytes()) {
                            let connections = local_connections.read().await;
                            if let Some(sender) = connections.get(token) {
                                if let Err(e) = sender.send(Message::binary(&decoded_data)).await {
                                    error!("Error sending anonymous WS update via Redis: {}", e);
                                }
                            }
                        }
                    }
                }
            }
        }

        async fn add_anonymous_connection(&self, token: &str) -> RedisResult<()> {
            let mut conn = timeout(self.redis_timeout, self.redis_manager.clone().get_async_connection()).await
                .map_err(|_| redis::RedisError::from((redis::ErrorKind::IoError, "Redis timeout")))??;
            
            let key = format!("{}{}", ANONYMOUS_CONNECTIONS_PREFIX, token);
            conn.set_ex(&key, "active", 3600).await?; // 1 hour expiration
            Ok(())
        }

        async fn remove_anonymous_connection(&self, token: &str) -> RedisResult<()> {
            let mut conn = timeout(self.redis_timeout, self.redis_manager.clone().get_async_connection()).await
                .map_err(|_| redis::RedisError::from((redis::ErrorKind::IoError, "Redis timeout")))??;
            
            let key = format!("{}{}", ANONYMOUS_CONNECTIONS_PREFIX, token);
            conn.del(&key).await?;
            Ok(())
        }

        async fn publish_anonymous_update(&self, token: &str, data: &[u8]) -> RedisResult<()> {
            let mut conn = timeout(self.redis_timeout, self.redis_manager.clone().get_async_connection()).await
                .map_err(|_| redis::RedisError::from((redis::ErrorKind::IoError, "Redis timeout")))??;
            
            let channel = format!("{}{}", ANONYMOUS_PUBSUB_CHANNEL_PREFIX, token);
            let message = serde_json::json!({
                "data": BASE64.encode(data)
            });
            
            conn.publish(&channel, message.to_string()).await?;
            Ok(())
        }

        /// Get current health status
        pub async fn get_health_info(&self) -> RedisHealthInfo {
            self.health_info.read().await.clone()
        }

        /// Check if Redis is currently healthy
        pub async fn is_healthy(&self) -> bool {
            self.health_info.read().await.status == RedisHealthStatus::Healthy
        }
    }

    impl AnonymousWebSocketBackend for RedisAnonymousWebSocketBackend {
        async fn add_connection(&self, token: &str, sender: Sender<Message>) {
            // Store locally for this instance
            {
                let mut connections = self.local_connections.write().await;
                connections.insert(token.to_string(), sender);
            }
            
            // Register in Redis
            if let Err(e) = self.add_anonymous_connection(token).await {
                error!("Failed to register anonymous WebSocket connection in Redis: {}", e);
                
                // Fallback: remove from local connections if Redis fails
                if self.fallback_to_memory {
                    let mut connections = self.local_connections.write().await;
                    connections.remove(token);
                }
            }
        }
        
        async fn remove_connection(&self, token: &str) {
            // Remove locally
            {
                let mut connections = self.local_connections.write().await;
                connections.remove(token);
            }
            
            // Remove from Redis
            if let Err(e) = self.remove_anonymous_connection(token).await {
                error!("Failed to remove anonymous WebSocket connection from Redis: {}", e);
            }
        }
        
        async fn send_update(&self, token: &str, data: &[u8]) {
            // Check if Redis is healthy before attempting to use it
            if !self.is_healthy().await && self.fallback_to_memory {
                warn!("Redis is unhealthy, using memory backend fallback for anonymous connection {}", token);
                let connections = self.local_connections.read().await;
                if let Some(sender) = connections.get(token) {
                    if let Err(e) = sender.send(Message::binary(data)).await {
                        error!("Error sending local anonymous WS update: {}", e);
                    }
                }
                return;
            }
            
            // Try to send via Redis pub/sub
            if let Err(e) = self.publish_anonymous_update(token, data).await {
                error!("Failed to publish anonymous WebSocket update to Redis: {}", e);
                
                // Fallback to local connection only if configured
                if self.fallback_to_memory {
                    warn!("Falling back to local anonymous WebSocket connection only");
                    let connections = self.local_connections.read().await;
                    if let Some(sender) = connections.get(token) {
                        if let Err(e) = sender.send(Message::binary(data)).await {
                            error!("Error sending local anonymous WS update: {}", e);
                        }
                    }
                }
            }
        }
    }
}

#[cfg(feature = "redis-websockets")]
pub use redis_impl::{RedisWebSocketBackend, RedisAnonymousWebSocketBackend, RedisHealthStatus, RedisHealthInfo};
