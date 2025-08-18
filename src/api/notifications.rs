use std::{net::IpAddr, sync::Arc, time::Duration};

use chrono::{NaiveDateTime, Utc};
use rmpv::Value;
use rocket::{futures::StreamExt, Route};
use tokio::sync::mpsc::Sender;

use rocket_ws::{Message, WebSocket};

use crate::{
    auth::{ClientIp, WsAccessTokenHeader},
    db::{
        models::{AuthRequestId, Cipher, CollectionId, Device, DeviceId, Folder, PushId, Send as DbSend, User, UserId},
        DbConn,
    },
    Error, CONFIG,
};

use once_cell::sync::Lazy;

pub mod backend;
pub mod memory_backend;
#[cfg(redis_websockets)]
pub mod redis_backend;

pub use backend::{WebSocketBackend, WebSocketBackendType};

// Global WebSocket backend instance
pub static WS_BACKEND: Lazy<WebSocketBackendType> = Lazy::new(|| {
    tokio::runtime::Handle::current().block_on(async {
        WebSocketBackendType::new().await.unwrap_or_else(|e| {
            error!("Failed to initialize WebSocket backend: {}", e);
            panic!("WebSocket backend initialization failed")
        })
    })
});

pub static WS_ANONYMOUS_SUBSCRIPTIONS: Lazy<Arc<AnonymousWebSocketSubscriptions>> = Lazy::new(|| {
    Arc::new(AnonymousWebSocketSubscriptions {
        map: Arc::new(dashmap::DashMap::new()),
    })
});

use super::{
    push::push_auth_request, push::push_auth_response, push_cipher_update, push_folder_update, push_logout,
    push_send_update, push_user_update,
};

static NOTIFICATIONS_DISABLED: Lazy<bool> = Lazy::new(|| !CONFIG.enable_websocket() && !CONFIG.push_enabled());

pub fn routes() -> Vec<Route> {
    if CONFIG.enable_websocket() {
        routes![websockets_hub, anonymous_websockets_hub]
    } else {
        info!("WebSocket are disabled, realtime sync functionality will not work!");
        routes![]
    }
}

#[derive(FromForm, Debug)]
struct WsAccessToken {
    access_token: Option<String>,
}

struct WSEntryMapGuard {
    user_uuid: UserId,
    entry_uuid: uuid::Uuid,
    addr: IpAddr,
}

impl WSEntryMapGuard {
    fn new(user_uuid: UserId, entry_uuid: uuid::Uuid, addr: IpAddr) -> Self {
        Self {
            user_uuid,
            entry_uuid,
            addr,
        }
    }
}

impl Drop for WSEntryMapGuard {
    fn drop(&mut self) {
        info!("Closing WS connection from {}", self.addr);
        
        // Use blocking version since Drop can't be async
        let rt = tokio::runtime::Handle::current();
        let user_uuid = self.user_uuid.clone();
        let entry_uuid = self.entry_uuid;
        
        rt.spawn(async move {
            if let Err(e) = WS_BACKEND.remove_connection(&user_uuid, entry_uuid).await {
                error!("Failed to remove WebSocket connection: {}", e);
            }
        });
    }
}

struct WSAnonymousEntryMapGuard {
    subscriptions: Arc<AnonymousWebSocketSubscriptions>,
    token: String,
    addr: IpAddr,
}

impl WSAnonymousEntryMapGuard {
    fn new(subscriptions: Arc<AnonymousWebSocketSubscriptions>, token: String, addr: IpAddr) -> Self {
        Self {
            subscriptions,
            token,
            addr,
        }
    }
}

impl Drop for WSAnonymousEntryMapGuard {
    fn drop(&mut self) {
        info!("Closing WS connection from {}", self.addr);
        self.subscriptions.map.remove(&self.token);
    }
}

#[allow(tail_expr_drop_order)]
#[get("/hub?<data..>")]
fn websockets_hub<'r>(
    ws: WebSocket,
    data: WsAccessToken,
    ip: ClientIp,
    header_token: WsAccessTokenHeader,
) -> Result<rocket_ws::Stream!['r], Error> {
    let addr = ip.ip;
    info!("Accepting Rocket WS connection from {addr}");

    let token = if let Some(token) = data.access_token {
        token
    } else if let Some(token) = header_token.access_token {
        token
    } else {
        err_code!("Invalid claim", 401)
    };

    let Ok(claims) = crate::auth::decode_login(&token) else {
        err_code!("Invalid token", 401)
    };

    let (mut rx, guard) = {
        // Add a channel to send messages to this client to the backend
        let entry_uuid = uuid::Uuid::new_v4();
        let (tx, rx) = tokio::sync::mpsc::channel::<Message>(100);
        
        // Add connection to the backend
        if let Err(e) = WS_BACKEND.add_connection(claims.sub.clone(), entry_uuid, tx).await {
            err_code!("Failed to register WebSocket connection", 500);
        }

        // Once the guard goes out of scope, the connection will have been closed and the entry will be deleted from the backend
        (rx, WSEntryMapGuard::new(claims.sub, entry_uuid, addr))
    };

    Ok({
        rocket_ws::Stream! { ws => {
            let mut ws = ws;
            let _guard = guard;
            let mut interval = tokio::time::interval(Duration::from_secs(15));
            loop {
                tokio::select! {
                    res = ws.next() =>  {
                        match res {
                            Some(Ok(message)) => {
                                match message {
                                    // Respond to any pings
                                    Message::Ping(ping) => yield Message::Pong(ping),
                                    Message::Pong(_) => {/* Ignored */},

                                    // We should receive an initial message with the protocol and version, and we will reply to it
                                    Message::Text(ref message) => {
                                        let msg = message.strip_suffix(RECORD_SEPARATOR as char).unwrap_or(message);

                                        if serde_json::from_str(msg).ok() == Some(INITIAL_MESSAGE) {
                                            yield Message::binary(INITIAL_RESPONSE);
                                        }
                                    }

                                    // Prevent sending anything back when a `Close` Message is received.
                                    // Just break the loop
                                    Message::Close(_) => break,

                                    // Just echo anything else the client sends
                                    _ => yield message,
                                }
                            }
                            _ => break,
                        }
                    }

                    res = rx.recv() => {
                        match res {
                            Some(res) => yield res,
                            None => break,
                        }
                    }

                    _ = interval.tick() => yield Message::Ping(create_ping())
                }
            }
        }}
    })
}

#[allow(tail_expr_drop_order)]
#[get("/anonymous-hub?<token..>")]
fn anonymous_websockets_hub<'r>(ws: WebSocket, token: String, ip: ClientIp) -> Result<rocket_ws::Stream!['r], Error> {
    let addr = ip.ip;
    info!("Accepting Anonymous Rocket WS connection from {addr}");

    let (mut rx, guard) = {
        let subscriptions = Arc::clone(&WS_ANONYMOUS_SUBSCRIPTIONS);

        // Add a channel to send messages to this client to the map
        let (tx, rx) = tokio::sync::mpsc::channel::<Message>(100);
        subscriptions.map.insert(token.clone(), tx);

        // Once the guard goes out of scope, the connection will have been closed and the entry will be deleted from the map
        (rx, WSAnonymousEntryMapGuard::new(subscriptions, token, addr))
    };

    Ok({
        rocket_ws::Stream! { ws => {
            let mut ws = ws;
            let _guard = guard;
            let mut interval = tokio::time::interval(Duration::from_secs(15));
            loop {
                tokio::select! {
                    res = ws.next() =>  {
                        match res {
                            Some(Ok(message)) => {
                                match message {
                                    // Respond to any pings
                                    Message::Ping(ping) => yield Message::Pong(ping),
                                    Message::Pong(_) => {/* Ignored */},

                                    // We should receive an initial message with the protocol and version, and we will reply to it
                                    Message::Text(ref message) => {
                                        let msg = message.strip_suffix(RECORD_SEPARATOR as char).unwrap_or(message);

                                        if serde_json::from_str(msg).ok() == Some(INITIAL_MESSAGE) {
                                            yield Message::binary(INITIAL_RESPONSE);
                                        }
                                    }

                                    // Prevent sending anything back when a `Close` Message is received.
                                    // Just break the loop
                                    Message::Close(_) => break,

                                    // Just echo anything else the client sends
                                    _ => yield message,
                                }
                            }
                            _ => break,
                        }
                    }

                    res = rx.recv() => {
                        match res {
                            Some(res) => yield res,
                            None => break,
                        }
                    }

                    _ = interval.tick() => yield Message::Ping(create_ping())
                }
            }
        }}
    })
}

//
// Websockets server
//

fn serialize(val: Value) -> Vec<u8> {
    use rmpv::encode::write_value;

    let mut buf = Vec::new();
    write_value(&mut buf, &val).expect("Error encoding MsgPack");

    // Add size bytes at the start
    // Extracted from BinaryMessageFormat.js
    let mut size: usize = buf.len();
    let mut len_buf: Vec<u8> = Vec::new();

    loop {
        let mut size_part = size & 0x7f;
        size >>= 7;

        if size > 0 {
            size_part |= 0x80;
        }

        len_buf.push(size_part as u8);

        if size == 0 {
            break;
        }
    }

    len_buf.append(&mut buf);
    len_buf
}

fn serialize_date(date: NaiveDateTime) -> Value {
    let seconds: i64 = date.and_utc().timestamp();
    let nanos: i64 = date.and_utc().timestamp_subsec_nanos().into();
    let timestamp = (nanos << 34) | seconds;

    let bs = timestamp.to_be_bytes();

    // -1 is Timestamp
    // https://github.com/msgpack/msgpack/blob/master/spec.md#timestamp-extension-type
    Value::Ext(-1, bs.to_vec())
}

fn convert_option<T: Into<Value>>(option: Option<T>) -> Value {
    match option {
        Some(a) => a.into(),
        None => Value::Nil,
    }
}

const RECORD_SEPARATOR: u8 = 0x1e;
const INITIAL_RESPONSE: [u8; 3] = [0x7b, 0x7d, RECORD_SEPARATOR]; // {, }, <RS>

#[derive(Deserialize, Copy, Clone, Eq, PartialEq)]
struct InitialMessage<'a> {
    protocol: &'a str,
    version: i32,
}

static INITIAL_MESSAGE: InitialMessage<'static> = InitialMessage {
    protocol: "messagepack",
    version: 1,
};

/// Unified WebSocket notification interface
pub struct WebSocketUsers;

impl WebSocketUsers {
    // NOTE: The last modified date needs to be updated before calling these methods
    pub async fn send_user_update(&self, ut: UpdateType, user: &User, push_uuid: &Option<PushId>, conn: &mut DbConn) {
        // Skip any processing if both WebSockets and Push are not active
        if *NOTIFICATIONS_DISABLED {
            return;
        }

        if let Err(e) = WS_BACKEND.send_user_update(ut, user, push_uuid, conn).await {
            error!("Failed to send user update: {}", e);
        }
    }

    pub async fn send_logout(&self, user: &User, acting_device_id: Option<DeviceId>, conn: &mut DbConn) {
        // Skip any processing if both WebSockets and Push are not active
        if *NOTIFICATIONS_DISABLED {
            return;
        }

        if let Err(e) = WS_BACKEND.send_logout(user, acting_device_id, conn).await {
            error!("Failed to send logout: {}", e);
        }
    }

    pub async fn send_folder_update(&self, ut: UpdateType, folder: &Folder, device: &Device, conn: &mut DbConn) {
        // Skip any processing if both WebSockets and Push are not active
        if *NOTIFICATIONS_DISABLED {
            return;
        }

        if let Err(e) = WS_BACKEND.send_folder_update(ut, folder, device, conn).await {
            error!("Failed to send folder update: {}", e);
        }
    }

    pub async fn send_cipher_update(
        &self,
        ut: UpdateType,
        cipher: &Cipher,
        user_ids: &[UserId],
        device: &Device,
        collection_uuids: Option<Vec<CollectionId>>,
        conn: &mut DbConn,
    ) {
        // Skip any processing if both WebSockets and Push are not active
        if *NOTIFICATIONS_DISABLED {
            return;
        }

        if let Err(e) = WS_BACKEND.send_cipher_update(ut, cipher, user_ids, device, collection_uuids, conn).await {
            error!("Failed to send cipher update: {}", e);
        }
    }

    pub async fn send_send_update(
        &self,
        ut: UpdateType,
        send: &DbSend,
        user_ids: &[UserId],
        device: &Device,
        conn: &mut DbConn,
    ) {
        // Skip any processing if both WebSockets and Push are not active
        if *NOTIFICATIONS_DISABLED {
            return;
        }

        if let Err(e) = WS_BACKEND.send_send_update(ut, send, user_ids, device, conn).await {
            error!("Failed to send send update: {}", e);
        }
    }

    pub async fn send_auth_request(
        &self,
        user_id: &UserId,
        auth_request_uuid: &str,
        device: &Device,
        conn: &mut DbConn,
    ) {
        // Skip any processing if both WebSockets and Push are not active
        if *NOTIFICATIONS_DISABLED {
            return;
        }

        if let Err(e) = WS_BACKEND.send_auth_request(user_id, auth_request_uuid, device, conn).await {
            error!("Failed to send auth request: {}", e);
        }
    }

    pub async fn send_auth_response(
        &self,
        user_id: &UserId,
        auth_request_id: &AuthRequestId,
        device: &Device,
        conn: &mut DbConn,
    ) {
        // Skip any processing if both WebSockets and Push are not active
        if *NOTIFICATIONS_DISABLED {
            return;
        }

        if let Err(e) = WS_BACKEND.send_auth_response(user_id, auth_request_id, device, conn).await {
            error!("Failed to send auth response: {}", e);
        }
    }
}

#[derive(Clone)]
pub struct AnonymousWebSocketSubscriptions {
    map: Arc<dashmap::DashMap<String, Sender<Message>>>,
}

impl AnonymousWebSocketSubscriptions {
    async fn send_update(&self, token: &str, data: &[u8]) {
        if let Some(sender) = self.map.get(token).map(|v| v.clone()) {
            if let Err(e) = sender.send(Message::binary(data)).await {
                error!("Error sending WS update {e}");
            }
        }
    }

    pub async fn send_auth_response(&self, user_id: &UserId, auth_request_id: &AuthRequestId) {
        if !CONFIG.enable_websocket() {
            return;
        }
        let data = create_anonymous_update(
            vec![("Id".into(), auth_request_id.to_string().into()), ("UserId".into(), user_id.to_string().into())],
            UpdateType::AuthRequestResponse,
            user_id.clone(),
        );
        self.send_update(auth_request_id, &data).await;
    }
}

/* Message Structure
[
    1, // MessageType.Invocation
    {}, // Headers (map)
    null, // InvocationId
    "ReceiveMessage", // Target
    [ // Arguments
        {
            "ContextId": acting_device_id || Nil,
            "Type": ut as i32,
            "Payload": {}
        }
    ]
]
*/
fn create_update(payload: Vec<(Value, Value)>, ut: UpdateType, acting_device_id: Option<DeviceId>) -> Vec<u8> {
    use rmpv::Value as V;

    let value = V::Array(vec![
        1.into(),
        V::Map(vec![]),
        V::Nil,
        "ReceiveMessage".into(),
        V::Array(vec![V::Map(vec![
            ("ContextId".into(), acting_device_id.map(|v| v.to_string().into()).unwrap_or_else(|| V::Nil)),
            ("Type".into(), (ut as i32).into()),
            ("Payload".into(), payload.into()),
        ])]),
    ]);

    serialize(value)
}

fn create_anonymous_update(payload: Vec<(Value, Value)>, ut: UpdateType, user_id: UserId) -> Vec<u8> {
    use rmpv::Value as V;

    let value = V::Array(vec![
        1.into(),
        V::Map(vec![]),
        V::Nil,
        "AuthRequestResponseRecieved".into(),
        V::Array(vec![V::Map(vec![
            ("Type".into(), (ut as i32).into()),
            ("Payload".into(), payload.into()),
            ("UserId".into(), user_id.to_string().into()),
        ])]),
    ]);

    serialize(value)
}

fn create_ping() -> Vec<u8> {
    serialize(Value::Array(vec![6.into()]))
}

/// Helper function to create update data from JSON payload
pub fn create_update_from_payload(payload: serde_json::Value, ut: UpdateType, acting_device_id: Option<DeviceId>) -> Vec<u8> {
    use rmpv::Value as V;
    
    // Convert JSON to rmpv Value
    let payload_value = match serde_json::to_string(&payload) {
        Ok(json_str) => match serde_json::from_str::<V>(&json_str) {
            Ok(val) => val,
            Err(_) => V::Nil,
        },
        Err(_) => V::Nil,
    };
    
    let value = V::Array(vec![
        1.into(),
        V::Map(vec![]),
        V::Nil,
        "ReceiveMessage".into(),
        V::Array(vec![V::Map(vec![
            ("ContextId".into(), acting_device_id.map(|v| v.to_string().into()).unwrap_or_else(|| V::Nil)),
            ("Type".into(), (ut as i32).into()),
            ("Payload".into(), payload_value),
        ])]),
    ]);

    serialize(value)
}

#[allow(dead_code)]
#[derive(Copy, Clone, Eq, PartialEq)]
pub enum UpdateType {
    SyncCipherUpdate = 0,
    SyncCipherCreate = 1,
    SyncLoginDelete = 2,
    SyncFolderDelete = 3,
    SyncCiphers = 4,

    SyncVault = 5,
    SyncOrgKeys = 6,
    SyncFolderCreate = 7,
    SyncFolderUpdate = 8,
    SyncCipherDelete = 9,
    SyncSettings = 10,

    LogOut = 11,

    SyncSendCreate = 12,
    SyncSendUpdate = 13,
    SyncSendDelete = 14,

    AuthRequest = 15,
    AuthRequestResponse = 16,

    None = 100,
}

pub type Notify<'a> = &'a rocket::State<WebSocketUsers>;
pub type AnonymousNotify<'a> = &'a rocket::State<Arc<AnonymousWebSocketSubscriptions>>;
