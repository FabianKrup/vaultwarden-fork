use std::{collections::HashMap, sync::Arc};

use chrono::{NaiveDateTime, Utc};
use rmpv::Value;
use rocket_ws::Message;
use tokio::sync::{mpsc::Sender, RwLock};

use crate::{
    api::push::{push_auth_request, push_auth_response, push_cipher_update, push_folder_update, push_logout,
                push_send_update, push_user_update},
    db::{
        models::{AuthRequestId, Cipher, CollectionId, Device, DeviceId, Folder, PushId, Send as DbSend, User, UserId},
        DbConn,
    },
    Error, CONFIG,
};

use super::{backend::WebSocketBackend, convert_option, create_update, serialize_date, UpdateType};

// We attach the UUID to the sender so we can differentiate them when we need to remove them from the Vec
type UserSenders = (uuid::Uuid, Sender<Message>);

/// In-memory WebSocket backend (original implementation)
#[derive(Clone)]
pub struct MemoryWebSocketBackend {
    map: Arc<RwLock<HashMap<String, Vec<UserSenders>>>>,
}

impl MemoryWebSocketBackend {
    pub fn new() -> Self {
        Self {
            map: Arc::new(RwLock::new(HashMap::new())),
        }
    }
    
    async fn send_update(&self, user_id: &UserId, data: &[u8]) {
        if let Some(user) = self.map.read().await.get(user_id.as_ref()).map(|v| v.clone()) {
            for (_, sender) in user.iter() {
                if let Err(e) = sender.send(Message::binary(data)).await {
                    error!("Error sending WS update {e}");
                }
            }
        }
    }
}

#[rocket::async_trait]
impl WebSocketBackend for MemoryWebSocketBackend {
    async fn add_connection(&self, user_id: UserId, connection_id: uuid::Uuid, sender: Sender<Message>) -> Result<(), Error> {
        self.map.write().await.entry(user_id.to_string()).or_default().push((connection_id, sender));
        Ok(())
    }
    
    async fn remove_connection(&self, user_id: &UserId, connection_id: uuid::Uuid) -> Result<(), Error> {
        let mut map = self.map.write().await;
        if let Some(entry) = map.get_mut(user_id.as_ref()) {
            entry.retain(|(uuid, _)| uuid != &connection_id);
            if entry.is_empty() {
                map.remove(user_id.as_ref());
            }
        }
        Ok(())
    }
    
    async fn send_user_update(&self, ut: UpdateType, user: &User, push_uuid: &Option<PushId>, conn: &mut DbConn) -> Result<(), Error> {
        let data = create_update(
            vec![("UserId".into(), user.uuid.to_string().into()), ("Date".into(), serialize_date(user.updated_at))],
            ut,
            None,
        );

        if CONFIG.enable_websocket() {
            self.send_update(&user.uuid, &data).await;
        }

        if CONFIG.push_enabled() {
            push_user_update(ut, user, push_uuid, conn).await;
        }
        
        Ok(())
    }
    
    async fn send_logout(&self, user: &User, acting_device_id: Option<DeviceId>, conn: &mut DbConn) -> Result<(), Error> {
        let data = create_update(
            vec![("UserId".into(), user.uuid.to_string().into()), ("Date".into(), serialize_date(user.updated_at))],
            UpdateType::LogOut,
            acting_device_id.clone(),
        );

        if CONFIG.enable_websocket() {
            self.send_update(&user.uuid, &data).await;
        }

        if CONFIG.push_enabled() {
            push_logout(user, acting_device_id.clone(), conn).await;
        }
        
        Ok(())
    }
    
    async fn send_folder_update(&self, ut: UpdateType, folder: &Folder, device: &Device, conn: &mut DbConn) -> Result<(), Error> {
        let data = create_update(
            vec![
                ("Id".into(), folder.uuid.to_string().into()),
                ("UserId".into(), folder.user_uuid.to_string().into()),
                ("RevisionDate".into(), serialize_date(folder.updated_at)),
            ],
            ut,
            Some(device.uuid.clone()),
        );

        if CONFIG.enable_websocket() {
            self.send_update(&folder.user_uuid, &data).await;
        }

        if CONFIG.push_enabled() {
            push_folder_update(ut, folder, device, conn).await;
        }
        
        Ok(())
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
        let org_id = convert_option(cipher.organization_uuid.as_deref());
        // Depending if there are collections provided or not, we need to have different values for the following variables.
        // The user_uuid should be `null`, and the revision date should be set to now, else the clients won't sync the collection change.
        let (user_id, collection_uuids, revision_date) = if let Some(collection_uuids) = collection_uuids {
            (
                Value::Nil,
                Value::Array(collection_uuids.into_iter().map(|v| v.to_string().into()).collect::<Vec<Value>>()),
                serialize_date(Utc::now().naive_utc()),
            )
        } else {
            (convert_option(cipher.user_uuid.as_deref()), Value::Nil, serialize_date(cipher.updated_at))
        };

        let data = create_update(
            vec![
                ("Id".into(), cipher.uuid.to_string().into()),
                ("UserId".into(), user_id),
                ("OrganizationId".into(), org_id),
                ("CollectionIds".into(), collection_uuids),
                ("RevisionDate".into(), revision_date),
            ],
            ut,
            Some(device.uuid.clone()), // Acting device id (unique device/app uuid)
        );

        if CONFIG.enable_websocket() {
            for uuid in user_ids {
                self.send_update(uuid, &data).await;
            }
        }

        if CONFIG.push_enabled() && user_ids.len() == 1 {
            push_cipher_update(ut, cipher, device, conn).await;
        }
        
        Ok(())
    }
    
    async fn send_send_update(
        &self,
        ut: UpdateType,
        send: &DbSend,
        user_ids: &[UserId],
        device: &Device,
        conn: &mut DbConn,
    ) -> Result<(), Error> {
        let user_id = convert_option(send.user_uuid.as_deref());

        let data = create_update(
            vec![
                ("Id".into(), send.uuid.to_string().into()),
                ("UserId".into(), user_id),
                ("RevisionDate".into(), serialize_date(send.revision_date)),
            ],
            ut,
            None,
        );

        if CONFIG.enable_websocket() {
            for uuid in user_ids {
                self.send_update(uuid, &data).await;
            }
        }
        if CONFIG.push_enabled() && user_ids.len() == 1 {
            push_send_update(ut, send, device, conn).await;
        }
        
        Ok(())
    }
    
    async fn send_auth_request(
        &self,
        user_id: &UserId,
        auth_request_uuid: &str,
        device: &Device,
        conn: &mut DbConn,
    ) -> Result<(), Error> {
        let data = create_update(
            vec![("Id".into(), auth_request_uuid.to_owned().into()), ("UserId".into(), user_id.to_string().into())],
            UpdateType::AuthRequest,
            Some(device.uuid.clone()),
        );
        if CONFIG.enable_websocket() {
            self.send_update(user_id, &data).await;
        }

        if CONFIG.push_enabled() {
            push_auth_request(user_id, auth_request_uuid, device, conn).await;
        }
        
        Ok(())
    }
    
    async fn send_auth_response(
        &self,
        user_id: &UserId,
        auth_request_id: &AuthRequestId,
        device: &Device,
        conn: &mut DbConn,
    ) -> Result<(), Error> {
        let data = create_update(
            vec![("Id".into(), auth_request_id.to_string().into()), ("UserId".into(), user_id.to_string().into())],
            UpdateType::AuthRequestResponse,
            Some(device.uuid.clone()),
        );
        if CONFIG.enable_websocket() {
            self.send_update(user_id, &data).await;
        }

        if CONFIG.push_enabled() {
            push_auth_response(user_id, auth_request_id, device, conn).await;
        }
        
        Ok(())
    }
}
