# WebSocket Notifications Backend System

Dieses Modul bietet ein erweiterbares Backend-System für WebSocket-Benachrichtigungen in Vaultwarden.

## Architektur

Das System ist in mehrere Komponenten aufgeteilt:

### Backend Traits (`backends.rs`)

- `WebSocketBackend`: Interface für authentifizierte WebSocket-Verbindungen
- `AnonymousWebSocketBackend`: Interface für anonyme WebSocket-Verbindungen (Auth-Requests)

### Memory Backend (`memory_backend.rs`)

- Standard In-Memory-Implementation mit DashMap
- Aktuell der Default-Backend
- Behält alle Verbindungen im lokalen Speicher

### Redis Backend (`redis_backend.rs`)

- Vorbereitet für zukünftige Redis-Implementation
- Wird horizontale Skalierung ermöglichen
- Aktiviert durch das `redis-websockets` Feature

## Nutzung

Das System wählt automatisch das richtige Backend basierend auf der Konfiguration:

```rust
// Globale Backend-Instanzen
pub static WS_BACKEND: Lazy<Arc<dyn WebSocketBackend>> = Lazy::new(|| {
    create_websocket_backend()
});

pub static WS_ANONYMOUS_BACKEND: Lazy<Arc<dyn AnonymousWebSocketBackend>> = Lazy::new(|| {
    create_anonymous_websocket_backend()
});
```

## Rückwärtskompatibilität

Die alten `WS_USERS` und `WS_ANONYMOUS_SUBSCRIPTIONS` statischen Variablen bleiben als Legacy-Aliases bestehen, um bestehenden Code nicht zu brechen.

## Zukünftige Erweiterungen

Um ein neues Backend hinzuzufügen:

1. Implementiere die `WebSocketBackend` und `AnonymousWebSocketBackend` Traits
2. Füge die Backend-Auswahl in `create_websocket_backend()` und `create_anonymous_websocket_backend()` hinzu
3. Erweitere die Konfiguration um die notwendigen Einstellungen

## Redis Implementation (geplant)

Die Redis-Implementation wird:

- Redis Pub/Sub für Nachrichten zwischen Instanzen verwenden
- Connection-State in Redis Streams oder Hashmaps speichern
- Horizontale Skalierung von Vaultwarden ermöglichen
