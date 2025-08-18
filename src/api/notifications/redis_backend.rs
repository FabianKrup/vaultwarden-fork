#[cfg(redis_websockets)]
use std::{collections::HashMap, sync::Arc, time::Duration};

#[cfg(redis_websockets)]
use redis::{
    aio::{ConnectionManager, PubSub},
    AsyncCommands, Client, RedisError, RedisResult,
};

#[cfg(redis_websockets)]
use rocket_ws::Message;

#[cfg(redis_websockets)]
use serde::{Deserialize, Serialize};

#[cfg(redis_websockets)]
use tokio::sync::{mpsc::Sender, RwLock};

#[cfg(redis_websockets)]
use crate::{
    db::{
        models::{AuthRequestId, Cipher, CollectionId, Device, DeviceId, Folder, PushId, Send as DbSend, User, UserId},
        DbConn,
    },
    Error, CONFIG,
};

#[cfg(redis_websockets)]
use super::{serialize_date, UpdateType, WebSocketBackend};

#[cfg(redis_websockets)]
const REDIS_CONNECTION_CHANNEL_PREFIX: &str = "vw:connections";
#[cfg(redis_websockets)]
const REDIS_NOTIFICATION_CHANNEL_PREFIX: &str = "vw:notifications";
#[cfg(redis_websockets)]
const REDIS_PRESENCE_KEY_PREFIX: &str = "vw:presence";
#[cfg(redis_websockets)]
const REDIS_SERVER_INSTANCE_KEY: &str = "vw:server_instance";
#[cfg(redis_websockets)]
const REDIS_KEY_TTL: u64 = 300; // 5 minutes

#[cfg(redis_websockets)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RedisNotificationMessage {
    pub update_type: UpdateType,
    pub payload: serde_json::Value,
    pub acting_device_id: Option<DeviceId>,
    pub target_users: Vec<UserId>,
    pub server_instance: String,
}

#[cfg(redis_websockets)]
#[derive(Debug, Clone)]
pub struct RedisConnection {
    pub user_id: UserId,
    pub connection_id: uuid::Uuid,
    pub server_instance: String,
    pub connected_at: chrono::NaiveDateTime,
}

#[cfg(redis_websockets)]
#[derive(Clone)]
pub struct RedisWebSocketBackend {
    client: Client,
    connection_manager: Arc<RwLock<Option<ConnectionManager>>>,
    pubsub: Arc<RwLock<Option<PubSub>>>,
    server_instance_id: String,
    local_connections: Arc<RwLock<HashMap<UserId, Vec<(uuid::Uuid, Sender<Message>)>>>>,
}

#[cfg(redis_websockets)]
impl RedisWebSocketBackend {
    pub async fn new() -> Result<Self, Error> {
        let redis_url = CONFIG.redis_websocket_url();
        let client = Client::open(redis_url)?;
        
        // Test connection
        let mut conn = client.get_multiplexed_async_connection().await?;
        let _: String = conn.ping().await?;
        
        let hostname = std::env::var("HOSTNAME").unwrap_or_else(|_| "localhost".to_string());
        let server_instance_id = format!("{}:{}", hostname, std::process::id());
        
        let backend = Self {
            client,
            connection_manager: Arc::new(RwLock::new(None)),
            pubsub: Arc::new(RwLock::new(None)),
            server_instance_id,
            local_connections: Arc::new(RwLock::new(HashMap::new())),
        };
        
        backend.initialize().await?;
        Ok(backend)
    }
    
    async fn initialize(&self) -> Result<(), Error> {
        // Initialize connection manager
        let conn_mgr = self.client.get_multiplexed_async_connection().await?;
        *self.connection_manager.write().await = Some(conn_mgr);
        
        // Initialize pub/sub
        let pubsub = self.client.get_async_pubsub().await?;
        *self.pubsub.write().await = Some(pubsub);
        
        // Start listening for notifications
        self.start_notification_listener().await?;
        
        // Register server instance
        self.register_server_instance().await?;
        
        Ok(())
    }
    
    async fn start_notification_listener(&self) -> Result<(), Error> {
        let pubsub_clone = Arc::clone(&self.pubsub);
        let local_connections_clone = Arc::clone(&self.local_connections);
        let server_instance_id = self.server_instance_id.clone();
        
        tokio::spawn(async move {
            loop {
                if let Err(e) = Self::notification_listener_loop(
                    Arc::clone(&pubsub_clone),
                    Arc::clone(&local_connections_clone),
                    server_instance_id.clone(),
                ).await {
                    error!("Redis WebSocket notification listener error: {}", e);
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            }
        });
        
        Ok(())
    }
    
    async fn notification_listener_loop(
        pubsub: Arc<RwLock<Option<PubSub>>>,
        local_connections: Arc<RwLock<HashMap<UserId, Vec<(uuid::Uuid, Sender<Message>)>>>>,
        server_instance_id: String,
    ) -> Result<(), Error> {
        let mut pubsub_guard = pubsub.write().await;
        if let Some(ref mut pubsub) = *pubsub_guard {
            // Subscribe to all notification channels
            pubsub.subscribe(format!("{}:*", REDIS_NOTIFICATION_CHANNEL_PREFIX)).await?;
            
            drop(pubsub_guard); // Release lock before entering loop
            
            use redis::AsyncCommands;
            loop {
                let msg: Result<redis::Msg, redis::RedisError> = pubsub.get_message().await;
                match msg {
                    Ok(redis::Msg { channel, payload, .. }) => {
                        if let Err(e) = Self::handle_notification_message(
                            &channel,
                            &payload,
                            Arc::clone(&local_connections),
                            &server_instance_id,
                        ).await {
                            error!("Error handling Redis notification: {}", e);
                        }
                    }
                    Err(e) => {
                        error!("Redis pub/sub stream error: {}", e);
                        break;
                    }
                }
            }
        }
        
        Ok(())
    }
    
    async fn handle_notification_message(
        channel: &str,
        payload: &str,
        local_connections: Arc<RwLock<HashMap<UserId, Vec<(uuid::Uuid, Sender<Message>)>>>>,
        server_instance_id: &str,
    ) -> Result<(), Error> {
        let notification: RedisNotificationMessage = serde_json::from_str(payload)?;
        
        // Skip notifications from our own server instance to avoid loops
        if notification.server_instance == server_instance_id {
            return Ok(());
        }
        
        // Send to local connections for target users
        let connections = local_connections.read().await;
        for user_id in &notification.target_users {
            if let Some(user_connections) = connections.get(user_id) {
                let data = super::create_update_from_payload(
                    notification.payload.clone(),
                    notification.update_type,
                    notification.acting_device_id.clone(),
                );
                
                for (_, sender) in user_connections {
                    if let Err(e) = sender.send(Message::binary(data.clone())).await {
                        error!("Error sending WebSocket message to local connection: {}", e);
                    }
                }
            }
        }
        
        Ok(())
    }
    
    async fn register_server_instance(&self) -> Result<(), Error> {
        let conn_mgr_guard = self.connection_manager.read().await;
        if let Some(ref mut conn) = *conn_mgr_guard {
            let key = format!("{}:{}", REDIS_SERVER_INSTANCE_KEY, self.server_instance_id);
            let _: () = conn.set_ex(key, chrono::Utc::now().timestamp(), REDIS_KEY_TTL).await?;
        }
        
        Ok(())
    }
    
    async fn register_connection(&self, user_id: &UserId, connection_id: uuid::Uuid) -> Result<(), Error> {
        let conn_mgr_guard = self.connection_manager.read().await;
        if let Some(ref mut conn) = *conn_mgr_guard {
            let connection = RedisConnection {
                user_id: user_id.clone(),
                connection_id,
                server_instance: self.server_instance_id.clone(),
                connected_at: chrono::Utc::now().naive_utc(),
            };
            
            let key = format!("{}:{}:{}", REDIS_CONNECTION_CHANNEL_PREFIX, user_id, connection_id);
            let value = serde_json::to_string(&connection)?;
            let _: () = conn.set_ex(key, value, REDIS_KEY_TTL).await?;
            
            // Update presence
            let presence_key = format!("{}:{}", REDIS_PRESENCE_KEY_PREFIX, user_id);
            let _: () = conn.sadd(presence_key.clone(), &self.server_instance_id).await?;
            let _: () = conn.expire(presence_key, REDIS_KEY_TTL as i64).await?;
        }
        
        Ok(())
    }
    
    async fn unregister_connection(&self, user_id: &UserId, connection_id: uuid::Uuid) -> Result<(), Error> {
        let conn_mgr_guard = self.connection_manager.read().await;
        if let Some(ref mut conn) = *conn_mgr_guard {
            let key = format!("{}:{}:{}", REDIS_CONNECTION_CHANNEL_PREFIX, user_id, connection_id);
            let _: () = conn.del(key).await?;
            
            // Check if user has other connections on this server
            let pattern = format!("{}:{}:*", REDIS_CONNECTION_CHANNEL_PREFIX, user_id);
            let keys: Vec<String> = conn.keys(pattern).await?;
            let server_connections: Vec<String> = keys.into_iter()
                .filter(|k| k.contains(&self.server_instance_id))
                .collect();
                
            if server_connections.is_empty() {
                // Remove from presence if no more connections on this server
                let presence_key = format!("{}:{}", REDIS_PRESENCE_KEY_PREFIX, user_id);
                let _: () = conn.srem(presence_key, &self.server_instance_id).await?;
            }
        }
        
        Ok(())
    }
    
    async fn publish_notification(&self, message: RedisNotificationMessage) -> Result<(), Error> {
        let conn_mgr_guard = self.connection_manager.read().await;
        if let Some(ref mut conn) = *conn_mgr_guard {
            let channel = format!("{}:broadcast", REDIS_NOTIFICATION_CHANNEL_PREFIX);
            let payload = serde_json::to_string(&message)?;
            let _: () = conn.publish(channel, payload).await?;
        }
        
        Ok(())
    }
}

#[cfg(redis_websockets)]
#[rocket::async_trait]
impl WebSocketBackend for RedisWebSocketBackend {
    async fn add_connection(&self, user_id: UserId, connection_id: uuid::Uuid, sender: Sender<Message>) -> Result<(), Error> {
        // Add to local connections
        let mut connections = self.local_connections.write().await;
        connections.entry(user_id.clone()).or_default().push((connection_id, sender));
        
        // Register in Redis
        self.register_connection(&user_id, connection_id).await?;
        
        Ok(())
    }
    
    async fn remove_connection(&self, user_id: &UserId, connection_id: uuid::Uuid) -> Result<(), Error> {
        // Remove from local connections
        {
            let mut connections = self.local_connections.write().await;
            if let Some(user_connections) = connections.get_mut(user_id) {
                user_connections.retain(|(id, _)| *id != connection_id);
                if user_connections.is_empty() {
                    connections.remove(user_id);
                }
            }
        }
        
        // Unregister from Redis
        self.unregister_connection(user_id, connection_id).await?;
        
        Ok(())
    }
    
    async fn send_user_update(&self, ut: UpdateType, user: &User, _push_uuid: &Option<PushId>, _conn: &mut DbConn) -> Result<(), Error> {
        let payload = serde_json::json!({
            "UserId": user.uuid.to_string(),
            "Date": serialize_date(user.updated_at)
        });
        
        let message = RedisNotificationMessage {
            update_type: ut,
            payload,
            acting_device_id: None,
            target_users: vec![user.uuid.clone()],
            server_instance: self.server_instance_id.clone(),
        };
        
        self.publish_notification(message).await
    }
    
    async fn send_logout(&self, user: &User, acting_device_id: Option<DeviceId>, _conn: &mut DbConn) -> Result<(), Error> {
        let payload = serde_json::json!({
            "UserId": user.uuid.to_string(),
            "Date": serialize_date(user.updated_at)
        });
        
        let message = RedisNotificationMessage {
            update_type: UpdateType::LogOut,
            payload,
            acting_device_id,
            target_users: vec![user.uuid.clone()],
            server_instance: self.server_instance_id.clone(),
        };
        
        self.publish_notification(message).await
    }
    
    async fn send_folder_update(&self, ut: UpdateType, folder: &Folder, device: &Device, _conn: &mut DbConn) -> Result<(), Error> {
        let payload = serde_json::json!({
            "Id": folder.uuid.to_string(),
            "UserId": folder.user_uuid.to_string(),
            "RevisionDate": serialize_date(folder.updated_at)
        });
        
        let message = RedisNotificationMessage {
            update_type: ut,
            payload,
            acting_device_id: Some(device.uuid.clone()),
            target_users: vec![folder.user_uuid.clone()],
            server_instance: self.server_instance_id.clone(),
        };
        
        self.publish_notification(message).await
    }
    
    async fn send_cipher_update(
        &self,
        ut: UpdateType,
        cipher: &Cipher,
        user_ids: &[UserId],
        device: &Device,
        collection_uuids: Option<Vec<CollectionId>>,
        _conn: &mut DbConn,
    ) -> Result<(), Error> {
        use rmpv::Value;
        use crate::api::notifications::convert_option;
        use chrono::Utc;
        
        let org_id = convert_option(cipher.organization_uuid.as_deref());
        let (user_id, collection_uuids, revision_date) = if let Some(collection_uuids) = collection_uuids {
            (
                Value::Nil,
                Value::Array(collection_uuids.into_iter().map(|v| v.to_string().into()).collect::<Vec<Value>>()),
                serialize_date(Utc::now().naive_utc()),
            )
        } else {
            (convert_option(cipher.user_uuid.as_deref()), Value::Nil, serialize_date(cipher.updated_at))
        };
        
        let payload = serde_json::json!({
            "Id": cipher.uuid.to_string(),
            "UserId": user_id,
            "OrganizationId": org_id,
            "CollectionIds": collection_uuids,
            "RevisionDate": revision_date
        });
        
        let message = RedisNotificationMessage {
            update_type: ut,
            payload,
            acting_device_id: Some(device.uuid.clone()),
            target_users: user_ids.to_vec(),
            server_instance: self.server_instance_id.clone(),
        };
        
        self.publish_notification(message).await
    }
    
    async fn send_send_update(
        &self,
        ut: UpdateType,
        send: &DbSend,
        user_ids: &[UserId],
        _device: &Device,
        _conn: &mut DbConn,
    ) -> Result<(), Error> {
        use crate::api::notifications::convert_option;
        
        let user_id = convert_option(send.user_uuid.as_deref());
        
        let payload = serde_json::json!({
            "Id": send.uuid.to_string(),
            "UserId": user_id,
            "RevisionDate": serialize_date(send.revision_date)
        });
        
        let message = RedisNotificationMessage {
            update_type: ut,
            payload,
            acting_device_id: None,
            target_users: user_ids.to_vec(),
            server_instance: self.server_instance_id.clone(),
        };
        
        self.publish_notification(message).await
    }
    
    async fn send_auth_request(
        &self,
        user_id: &UserId,
        auth_request_uuid: &str,
        device: &Device,
        _conn: &mut DbConn,
    ) -> Result<(), Error> {
        let payload = serde_json::json!({
            "Id": auth_request_uuid,
            "UserId": user_id.to_string()
        });
        
        let message = RedisNotificationMessage {
            update_type: UpdateType::AuthRequest,
            payload,
            acting_device_id: Some(device.uuid.clone()),
            target_users: vec![user_id.clone()],
            server_instance: self.server_instance_id.clone(),
        };
        
        self.publish_notification(message).await
    }
    
    async fn send_auth_response(
        &self,
        user_id: &UserId,
        auth_request_id: &AuthRequestId,
        device: &Device,
        _conn: &mut DbConn,
    ) -> Result<(), Error> {
        let payload = serde_json::json!({
            "Id": auth_request_id.to_string(),
            "UserId": user_id.to_string()
        });
        
        let message = RedisNotificationMessage {
            update_type: UpdateType::AuthRequestResponse,
            payload,
            acting_device_id: Some(device.uuid.clone()),
            target_users: vec![user_id.clone()],
            server_instance: self.server_instance_id.clone(),
        };
        
        self.publish_notification(message).await
    }
}

#[cfg(redis_websockets)]
impl From<RedisError> for Error {
    fn from(err: RedisError) -> Self {
        Error::new("Redis error", err.to_string())
    }
}
