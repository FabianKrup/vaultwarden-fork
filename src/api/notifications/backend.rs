use std::sync::Arc;

use rocket_ws::Message;
use tokio::sync::mpsc::Sender;

use crate::{
    db::{
        models::{AuthRequestId, Cipher, CollectionId, Device, DeviceId, Folder, PushId, Send as DbSend, User, UserId},
        DbConn,
    },
    Error,
};

use super::UpdateType;

/// WebSocket backend trait for different storage implementations
#[rocket::async_trait]
pub trait WebSocketBackend: Send + Sync {
    /// Add a new WebSocket connection for a user
    async fn add_connection(&self, user_id: UserId, connection_id: uuid::Uuid, sender: Sender<Message>) -> Result<(), Error>;
    
    /// Remove a WebSocket connection for a user
    async fn remove_connection(&self, user_id: &UserId, connection_id: uuid::Uuid) -> Result<(), Error>;
    
    /// Send user update notification
    async fn send_user_update(&self, ut: UpdateType, user: &User, push_uuid: &Option<PushId>, conn: &mut DbConn) -> Result<(), Error>;
    
    /// Send logout notification
    async fn send_logout(&self, user: &User, acting_device_id: Option<DeviceId>, conn: &mut DbConn) -> Result<(), Error>;
    
    /// Send folder update notification
    async fn send_folder_update(&self, ut: UpdateType, folder: &Folder, device: &Device, conn: &mut DbConn) -> Result<(), Error>;
    
    /// Send cipher update notification
    async fn send_cipher_update(
        &self,
        ut: UpdateType,
        cipher: &Cipher,
        user_ids: &[UserId],
        device: &Device,
        collection_uuids: Option<Vec<CollectionId>>,
        conn: &mut DbConn,
    ) -> Result<(), Error>;
    
    /// Send send update notification
    async fn send_send_update(
        &self,
        ut: UpdateType,
        send: &DbSend,
        user_ids: &[UserId],
        device: &Device,
        conn: &mut DbConn,
    ) -> Result<(), Error>;
    
    /// Send auth request notification
    async fn send_auth_request(
        &self,
        user_id: &UserId,
        auth_request_uuid: &str,
        device: &Device,
        conn: &mut DbConn,
    ) -> Result<(), Error>;
    
    /// Send auth response notification
    async fn send_auth_response(
        &self,
        user_id: &UserId,
        auth_request_id: &AuthRequestId,
        device: &Device,
        conn: &mut DbConn,
    ) -> Result<(), Error>;
}

/// Backend type enum for different WebSocket storage implementations
pub enum WebSocketBackendType {
    Memory(Arc<super::memory_backend::MemoryWebSocketBackend>),
    #[cfg(redis_websockets)]
    Redis(Arc<super::redis_backend::RedisWebSocketBackend>),
}

impl WebSocketBackendType {
    /// Create new WebSocket backend based on configuration
    pub async fn new() -> Result<Self, Error> {
        #[cfg(redis_websockets)]
        if crate::CONFIG.redis_websocket_enabled() {
            info!("Initializing Redis WebSocket backend (experimental)");
            match super::redis_backend::RedisWebSocketBackend::new().await {
                Ok(backend) => return Ok(Self::Redis(Arc::new(backend))),
                Err(e) => {
                    if crate::CONFIG.redis_websocket_fallback_memory() {
                        warn!("Redis WebSocket backend failed to initialize, falling back to memory: {}", e);
                    } else {
                        return Err(e);
                    }
                }
            }
        }
        
        info!("Using memory WebSocket backend");
        Ok(Self::Memory(Arc::new(super::memory_backend::MemoryWebSocketBackend::new())))
    }
    
    /// Get the backend as a trait object
    pub fn as_backend(&self) -> &dyn WebSocketBackend {
        match self {
            Self::Memory(backend) => backend.as_ref(),
            #[cfg(redis_websockets)]
            Self::Redis(backend) => backend.as_ref(),
        }
    }
}

#[rocket::async_trait]
impl WebSocketBackend for WebSocketBackendType {
    async fn add_connection(&self, user_id: UserId, connection_id: uuid::Uuid, sender: Sender<Message>) -> Result<(), Error> {
        self.as_backend().add_connection(user_id, connection_id, sender).await
    }
    
    async fn remove_connection(&self, user_id: &UserId, connection_id: uuid::Uuid) -> Result<(), Error> {
        self.as_backend().remove_connection(user_id, connection_id).await
    }
    
    async fn send_user_update(&self, ut: UpdateType, user: &User, push_uuid: &Option<PushId>, conn: &mut DbConn) -> Result<(), Error> {
        self.as_backend().send_user_update(ut, user, push_uuid, conn).await
    }
    
    async fn send_logout(&self, user: &User, acting_device_id: Option<DeviceId>, conn: &mut DbConn) -> Result<(), Error> {
        self.as_backend().send_logout(user, acting_device_id, conn).await
    }
    
    async fn send_folder_update(&self, ut: UpdateType, folder: &Folder, device: &Device, conn: &mut DbConn) -> Result<(), Error> {
        self.as_backend().send_folder_update(ut, folder, device, conn).await
    }
    
    async fn send_cipher_update(
        &self,
        ut: UpdateType,
        cipher: &Cipher,
        user_ids: &[UserId],
        device: &Device,
        collection_uuids: Option<Vec<CollectionId>>,
        conn: &mut DbConn,
    ) -> Result<(), Error> {
        self.as_backend().send_cipher_update(ut, cipher, user_ids, device, collection_uuids, conn).await
    }
    
    async fn send_send_update(
        &self,
        ut: UpdateType,
        send: &DbSend,
        user_ids: &[UserId],
        device: &Device,
        conn: &mut DbConn,
    ) -> Result<(), Error> {
        self.as_backend().send_send_update(ut, send, user_ids, device, conn).await
    }
    
    async fn send_auth_request(
        &self,
        user_id: &UserId,
        auth_request_uuid: &str,
        device: &Device,
        conn: &mut DbConn,
    ) -> Result<(), Error> {
        self.as_backend().send_auth_request(user_id, auth_request_uuid, device, conn).await
    }
    
    async fn send_auth_response(
        &self,
        user_id: &UserId,
        auth_request_id: &AuthRequestId,
        device: &Device,
        conn: &mut DbConn,
    ) -> Result<(), Error> {
        self.as_backend().send_auth_response(user_id, auth_request_id, device, conn).await
    }
}
