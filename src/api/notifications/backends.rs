/*!
 * WebSocket Backend Traits
 * 
 * These traits define the interface that all WebSocket backends must implement.
 * This allows for pluggable backends (memory, Redis, etc.) without changing
 * the core notification logic.
 */

use tokio::sync::mpsc::Sender;
use rocket_ws::Message;

use crate::db::models::{UserId, AuthRequestId};

/// Trait for WebSocket backend implementations
/// 
/// This trait defines the interface for managing user WebSocket connections.
/// Implementations can use different storage mechanisms (in-memory, Redis, etc.)
pub trait WebSocketBackend: Send + Sync {
    /// Add a new WebSocket connection for a user
    fn add_connection(&self, user_id: &UserId, entry_uuid: uuid::Uuid, sender: Sender<Message>) -> impl std::future::Future<Output = ()> + Send;
    
    /// Remove a specific WebSocket connection for a user
    fn remove_connection(&self, user_id: &UserId, entry_uuid: uuid::Uuid) -> impl std::future::Future<Output = ()> + Send;
    
    /// Send an update to all connections for a specific user
    fn send_update(&self, user_id: &UserId, data: &[u8]) -> impl std::future::Future<Output = ()> + Send;
    
    /// Shutdown the backend gracefully
    /// This should clean up any background tasks, close connections, etc.
    fn shutdown(&self);
}

/// Trait for anonymous WebSocket backend implementations
/// 
/// This trait defines the interface for managing anonymous WebSocket connections
/// (used for auth requests from devices that aren't logged in yet).
pub trait AnonymousWebSocketBackend: Send + Sync {
    /// Add a new anonymous WebSocket connection
    fn add_connection(&self, token: &str, sender: Sender<Message>) -> impl std::future::Future<Output = ()> + Send;
    
    /// Remove an anonymous WebSocket connection
    fn remove_connection(&self, token: &str) -> impl std::future::Future<Output = ()> + Send;
    
    /// Send an update to a specific anonymous connection
    fn send_update(&self, token: &str, data: &[u8]) -> impl std::future::Future<Output = ()> + Send;
    
    /// Shutdown the backend gracefully
    /// This should clean up any background tasks, close connections, etc.
    fn shutdown(&self);
}
