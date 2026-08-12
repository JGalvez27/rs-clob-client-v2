#![expect(
    clippy::module_name_repetitions,
    reason = "Connection types expose their domain in the name for clarity"
)]

use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Instant;

use backoff::backoff::Backoff as _;
use futures::{SinkExt as _, StreamExt as _};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::net::TcpStream;
use tokio::sync::{broadcast, mpsc, watch};
use tokio::time::{interval, sleep, timeout};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};

use super::config::Config;
use super::error::WsError;
use super::traits::MessageParser;
use crate::auth::Credentials;
use crate::error::Kind;
use crate::ws::WithCredentials;
use crate::{Result, error::Error};

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Broadcast channel capacity for incoming messages.
const BROADCAST_CAPACITY: usize = 1024;

/// Connection state tracking.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    /// Not connected
    Disconnected,
    /// Attempting to connect
    Connecting,
    /// Successfully connected
    Connected {
        /// When the connection was established
        since: Instant,
    },
    /// Reconnecting after failure
    Reconnecting {
        /// Current reconnection attempt number
        attempt: u32,
    },
}

impl ConnectionState {
    /// Check if the connection is currently active.
    #[must_use]
    pub const fn is_connected(self) -> bool {
        matches!(self, Self::Connected { .. })
    }
}

/// One incoming WebSocket text frame, byte-exact, captured BEFORE any typed
/// parse. A valid-JSON frame with an unknown event/status/field shape still
/// yields a `RawWsFrame`; a typed-parse failure never suppresses it.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct RawWsFrame {
    /// The exact UTF-8 JSON payload as received on the wire.
    pub json: Arc<str>,
    /// Monotonic receipt instant, stamped in the read loop.
    pub received_at: Instant,
}

/// Event on the raw-frame channel (see [`ConnectionManager::subscribe_raw`]).
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum RawWsEvent {
    /// A frame within the configured size limit (or no limit configured).
    Frame(RawWsFrame),
    /// A frame exceeding [`Config::max_frame_bytes`]. It was NOT parsed and
    /// its payload is NOT carried (never truncated); consumers must treat
    /// this as data loss.
    ///
    /// [`Config::max_frame_bytes`]: super::config::Config::max_frame_bytes
    Oversized {
        /// Actual frame length in bytes.
        len: usize,
        /// Configured maximum.
        max: usize,
    },
}

/// Manages WebSocket connection lifecycle, reconnection, and heartbeat.
///
/// This generic connection manager handles all WebSocket connection concerns:
/// - Establishing and maintaining connections
/// - Automatic reconnection with exponential backoff
/// - Heartbeat monitoring via PING/PONG
/// - Broadcasting messages to multiple subscribers
///
/// # Type Parameters
///
/// - `M`: Message type that implements [`DeserializeOwned`] among other "helper" types
/// - `P`: Parser type that implements [`MessageParser<M>`]
///
/// # Example
///
/// ```ignore
/// let parser = SimpleParser;
/// let connection = ConnectionManager::new(
///     "wss://example.com".to_owned(),
///     config,
///     parser,
/// )?;
///
/// // Subscribe to messages
/// let mut rx = connection.subscribe();
/// while let Ok(msg) = rx.recv().await {
///     println!("Received: {:?}", msg);
/// }
/// ```
#[derive(Clone)]
pub struct ConnectionManager<M, P>
where
    M: DeserializeOwned + Debug + Clone + Send + 'static,
    P: MessageParser<M>,
{
    /// Watch channel sender for state changes (enables reconnection detection)
    state_tx: watch::Sender<ConnectionState>,
    /// Watch channel receiver for state changes (for use in checking the current state)
    state_rx: watch::Receiver<ConnectionState>,
    /// Sender channel for outgoing messages
    sender_tx: mpsc::UnboundedSender<String>,
    /// Broadcast sender for incoming messages
    broadcast_tx: broadcast::Sender<M>,
    /// Broadcast sender for raw pre-parse frames (see [`Self::subscribe_raw`])
    raw_tx: broadcast::Sender<RawWsEvent>,
    /// Phantom data for unused type parameters
    _phantom: PhantomData<P>,
}

impl<M, P> ConnectionManager<M, P>
where
    M: DeserializeOwned + Debug + Clone + Send + 'static,
    P: MessageParser<M>,
{
    /// Create a new connection manager and start the connection loop.
    ///
    /// The `parser` is used to deserialize incoming WebSocket messages.
    /// The connection loop runs in a background task and automatically
    /// handles reconnection according to the config's `ReconnectConfig`.
    pub fn new(endpoint: String, config: Config, parser: P) -> Result<Self> {
        let (sender_tx, sender_rx) = mpsc::unbounded_channel();
        let (broadcast_tx, _) = broadcast::channel(BROADCAST_CAPACITY);
        let (raw_tx, _) = broadcast::channel(BROADCAST_CAPACITY);
        let (state_tx, state_rx) = watch::channel(ConnectionState::Disconnected);

        // Spawn connection task
        let connection_config = config;
        let connection_endpoint = endpoint;
        let broadcast_tx_clone = broadcast_tx.clone();
        let raw_tx_clone = raw_tx.clone();
        let state_tx_clone = state_tx.clone();

        tokio::spawn(async move {
            Self::connection_loop(
                connection_endpoint,
                connection_config,
                sender_rx,
                broadcast_tx_clone,
                raw_tx_clone,
                parser,
                state_tx_clone,
            )
            .await;
        });

        Ok(Self {
            state_tx,
            state_rx,
            sender_tx,
            broadcast_tx,
            raw_tx,
            _phantom: PhantomData,
        })
    }

    /// Main connection loop with automatic reconnection.
    async fn connection_loop(
        endpoint: String,
        config: Config,
        mut sender_rx: mpsc::UnboundedReceiver<String>,
        broadcast_tx: broadcast::Sender<M>,
        raw_tx: broadcast::Sender<RawWsEvent>,
        parser: P,
        state_tx: watch::Sender<ConnectionState>,
    ) {
        let mut attempt = 0_u32;
        let mut backoff: backoff::ExponentialBackoff = config.reconnect.clone().into();

        loop {
            // Check if ConnectionManager was dropped (all sender_tx instances gone)
            if sender_rx.is_closed() {
                #[cfg(feature = "tracing")]
                tracing::debug!("Sender channel closed, stopping connection loop");
                _ = state_tx.send(ConnectionState::Disconnected);
                break;
            }

            let state_rx = state_tx.subscribe();

            _ = state_tx.send(ConnectionState::Connecting);

            // Attempt connection
            match connect_async(&endpoint).await {
                Ok((ws_stream, _)) => {
                    attempt = 0;
                    backoff.reset();
                    _ = state_tx.send(ConnectionState::Connected {
                        since: Instant::now(),
                    });

                    // Handle connection
                    if let Err(e) = Self::handle_connection(
                        ws_stream,
                        &mut sender_rx,
                        &broadcast_tx,
                        &raw_tx,
                        state_rx,
                        config.clone(),
                        &parser,
                    )
                    .await
                    {
                        #[cfg(feature = "tracing")]
                        tracing::error!("Error handling connection: {e:?}");
                        #[cfg(not(feature = "tracing"))]
                        let _: &_ = &e;
                    }
                }
                Err(e) => {
                    let error = Error::with_source(Kind::WebSocket, WsError::Connection(e));
                    #[cfg(feature = "tracing")]
                    tracing::warn!("Unable to connect: {error:?}");
                    #[cfg(not(feature = "tracing"))]
                    let _: &_ = &error;
                    attempt = attempt.saturating_add(1);
                }
            }

            // Check if we should stop reconnecting
            if let Some(max) = config.reconnect.max_attempts
                && attempt >= max
            {
                _ = state_tx.send(ConnectionState::Disconnected);
                break;
            }

            // Update state and wait with exponential backoff
            _ = state_tx.send(ConnectionState::Reconnecting { attempt });

            if let Some(duration) = backoff.next_backoff() {
                sleep(duration).await;
            }
        }
    }

    /// Handle an active WebSocket connection.
    async fn handle_connection(
        ws_stream: WsStream,
        sender_rx: &mut mpsc::UnboundedReceiver<String>,
        broadcast_tx: &broadcast::Sender<M>,
        raw_tx: &broadcast::Sender<RawWsEvent>,
        state_rx: watch::Receiver<ConnectionState>,
        config: Config,
        parser: &P,
    ) -> Result<()> {
        let (mut write, mut read) = ws_stream.split();

        // Channel to notify heartbeat loop when PONG is received
        let (pong_tx, pong_rx) = watch::channel(Instant::now());
        let (ping_tx, mut ping_rx) = mpsc::unbounded_channel();

        let max_frame_bytes = config.max_frame_bytes;
        let heartbeat_handle = tokio::spawn(async move {
            Self::heartbeat_loop(ping_tx, state_rx, &config, pong_rx).await;
        });

        loop {
            tokio::select! {
                // Handle incoming messages
                Some(msg) = read.next() => {
                    match msg {
                        Ok(Message::Text(text)) if text == "PONG" => {
                            _ = pong_tx.send(Instant::now());
                        }
                        Ok(Message::Text(text)) => {
                            #[cfg(feature = "tracing")]
                            tracing::trace!(%text, "Received WebSocket text message");

                            // Raw-frame tap (pre-parse). Size guard FIRST: an
                            // oversized frame is surfaced as data loss and is
                            // NOT parsed — a frame too large to journal must
                            // not be acted on downstream.
                            if let Some(max) = max_frame_bytes
                                && text.len() > max
                            {
                                _ = raw_tx.send(RawWsEvent::Oversized {
                                    len: text.len(),
                                    max,
                                });
                                #[cfg(feature = "tracing")]
                                tracing::error!(
                                    len = text.len(),
                                    max,
                                    "WebSocket frame exceeds max_frame_bytes — not parsed"
                                );
                                continue;
                            }
                            // Byte-exact raw emission BEFORE the typed parse:
                            // a typed-parse failure must never suppress the
                            // raw frame.
                            _ = raw_tx.send(RawWsEvent::Frame(RawWsFrame {
                                json: Arc::from(text.as_str()),
                                received_at: Instant::now(),
                            }));

                            // Parse messages using the provided parser
                            match parser.parse(text.as_bytes()) {
                                Ok(messages) => {
                                    for message in messages {
                                        #[cfg(feature = "tracing")]
                                        tracing::trace!(?message, "Parsed WebSocket message");
                                        _ = broadcast_tx.send(message);
                                    }
                                }
                                Err(e) => {
                                    #[cfg(feature = "tracing")]
                                    tracing::warn!(%text, error = %e, "Failed to parse WebSocket message");
                                    #[cfg(not(feature = "tracing"))]
                                    let _: (&_, &_) = (&text, &e);
                                }
                            }
                        }
                        Ok(Message::Close(_)) => {
                            heartbeat_handle.abort();
                            return Err(Error::with_source(
                                Kind::WebSocket,
                                WsError::ConnectionClosed,
                            ))
                        }
                        Err(e) => {
                            heartbeat_handle.abort();
                            return Err(Error::with_source(
                                Kind::WebSocket,
                                WsError::Connection(e),
                            ));
                        }
                        _ => {
                            // Ignore binary frames and unsolicited PONG replies.
                        }
                    }
                }

                // Handle outgoing messages from subscriptions
                Some(text) = sender_rx.recv() => {
                    if write.send(Message::Text(text.into())).await.is_err() {
                        break;
                    }
                }

                // Handle PING requests from heartbeat loop
                Some(()) = ping_rx.recv() => {
                    if write.send(Message::Text("PING".into())).await.is_err() {
                        break;
                    }
                }

                // Check if connection is still active
                else => {
                    break;
                }
            }
        }

        // Cleanup
        heartbeat_handle.abort();

        Ok(())
    }

    /// Heartbeat loop that sends PING messages and monitors PONG responses.
    async fn heartbeat_loop(
        ping_tx: mpsc::UnboundedSender<()>,
        state_rx: watch::Receiver<ConnectionState>,
        config: &Config,
        mut pong_rx: watch::Receiver<Instant>,
    ) {
        let mut ping_interval = interval(config.heartbeat_interval);

        loop {
            ping_interval.tick().await;

            // Check if still connected
            if !state_rx.borrow().is_connected() {
                break;
            }

            // Mark current PONG state as seen before sending PING
            // This prevents changed() from returning immediately due to a stale PONG
            drop(pong_rx.borrow_and_update());

            // Send PING request to message loop
            let ping_sent = Instant::now();
            if ping_tx.send(()).is_err() {
                // Message loop has terminated
                break;
            }

            // Wait for PONG within timeout
            let pong_result = timeout(config.heartbeat_timeout, pong_rx.changed()).await;

            match pong_result {
                Ok(Ok(())) => {
                    let last_pong = *pong_rx.borrow_and_update();
                    if last_pong < ping_sent {
                        #[cfg(feature = "tracing")]
                        tracing::debug!(
                            "PONG received but older than last PING, connection may be stale"
                        );
                        break;
                    }
                }
                Ok(Err(_)) => {
                    // Channel closed, connection is terminating
                    break;
                }
                Err(_) => {
                    // Timeout waiting for PONG
                    #[cfg(feature = "tracing")]
                    tracing::warn!(
                        "Heartbeat timeout: no PONG received within {:?}",
                        config.heartbeat_timeout
                    );
                    break;
                }
            }
        }
    }

    /// Send a subscription request to the WebSocket server.
    pub fn send<R: Serialize>(&self, request: &R) -> Result<()> {
        let json = serde_json::to_string(request)?;
        self.sender_tx
            .send(json)
            .map_err(|_e| WsError::ConnectionClosed)?;
        Ok(())
    }

    /// Send a subscription request to the WebSocket server.
    pub fn send_authenticated<R: WithCredentials>(
        &self,
        request: &R,
        credentials: &Credentials,
    ) -> Result<()> {
        let json = request.as_authenticated(credentials)?;
        self.sender_tx
            .send(json)
            .map_err(|_e| WsError::ConnectionClosed)?;
        Ok(())
    }

    /// Get the current connection state.
    #[must_use]
    pub fn state(&self) -> ConnectionState {
        *self.state_rx.borrow()
    }

    /// Subscribe to incoming messages.
    ///
    /// Each call returns a new independent receiver. Multiple subscribers can
    /// receive messages concurrently without blocking each other.
    #[must_use]
    pub fn subscribe(&self) -> broadcast::Receiver<M> {
        self.broadcast_tx.subscribe()
    }

    /// Subscribe to the raw pre-parse frame channel.
    ///
    /// Each call returns a new independent receiver of [`RawWsEvent`]. Frames
    /// are emitted byte-exact BEFORE the typed parser runs, so consumers see
    /// every data frame the connection received — including frames the typed
    /// layer fails to parse. Transport control frames (PING/PONG/close) and
    /// binary frames are never emitted here.
    #[must_use]
    pub fn subscribe_raw(&self) -> broadcast::Receiver<RawWsEvent> {
        self.raw_tx.subscribe()
    }

    /// Subscribe to connection state changes.
    ///
    /// Returns a receiver that notifies when the connection state changes.
    /// This is useful for detecting reconnections and re-establishing subscriptions.
    #[must_use]
    pub fn state_receiver(&self) -> watch::Receiver<ConnectionState> {
        self.state_tx.subscribe()
    }
}
