/*!
 * In-Memory WebSocket Backend Implementation
 * 
 * This provides the default in-memory implementation for WebSocket connections.
 * Uses DashMap for concurrent access to connection storage.
 */

use std::sync::Arc;
use log::{error, info};
use tokio::sync::mpsc::Sender;
use rocket_ws::Message;

use crate::db::models::{UserId, AuthRequestId};
use super::backends::{WebSocketBackend, AnonymousWebSocketBackend};

// We attach the UUID to the sender so we can differentiate them when we need to remove them from the Vec
type UserSenders = (uuid::Uuid, Sender<Message>);

#[derive(Clone)]
pub struct MemoryWebSocketBackend {
    map: Arc<dashmap::DashMap<String, Vec<UserSenders>>>,
}

impl MemoryWebSocketBackend {
    pub fn new() -> Self {
        Self {
            map: Arc::new(dashmap::DashMap::new()),
        }
    }
}

impl WebSocketBackend for MemoryWebSocketBackend {
    async fn add_connection(&self, user_id: &UserId, entry_uuid: uuid::Uuid, sender: Sender<Message>) {
        self.map.entry(user_id.to_string()).or_default().push((entry_uuid, sender));
    }
    
    async fn remove_connection(&self, user_id: &UserId, entry_uuid: uuid::Uuid) {
        if let Some(mut entry) = self.map.get_mut(user_id.as_ref()) {
            entry.retain(|(uuid, _)| uuid != &entry_uuid);
        }
    }
    
    async fn send_update(&self, user_id: &UserId, data: &[u8]) {
        if let Some(user) = self.map.get(user_id.as_ref()).map(|v| v.clone()) {
            for (_, sender) in user.iter() {
                if let Err(e) = sender.send(Message::binary(data)).await {
                    error!("Error sending WS update {e}");
                }
            }
        }
    }

    fn shutdown(&self) {
        // Memory backend doesn't need graceful shutdown - no background tasks to clean up
        info!("Memory WebSocket backend shutdown called (no-op)");
    }
}

#[derive(Clone)]
pub struct MemoryAnonymousWebSocketBackend {
    map: Arc<dashmap::DashMap<String, Sender<Message>>>,
}

impl MemoryAnonymousWebSocketBackend {
    pub fn new() -> Self {
        Self {
            map: Arc::new(dashmap::DashMap::new()),
        }
    }
}

impl AnonymousWebSocketBackend for MemoryAnonymousWebSocketBackend {
    async fn add_connection(&self, token: &str, sender: Sender<Message>) {
        self.map.insert(token.to_string(), sender);
    }
    
    async fn remove_connection(&self, token: &str) {
        self.map.remove(token);
    }
    
    async fn send_update(&self, token: &str, data: &[u8]) {
        if let Some(sender) = self.map.get(token).map(|v| v.clone()) {
            if let Err(e) = sender.send(Message::binary(data)).await {
                error!("Error sending WS update {e}");
            }
        }
    }

    fn shutdown(&self) {
        // Memory backend doesn't need graceful shutdown - no background tasks to clean up
        info!("Memory anonymous WebSocket backend shutdown called (no-op)");
    }
}
