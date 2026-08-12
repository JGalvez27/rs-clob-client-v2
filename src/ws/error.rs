#![expect(
    clippy::module_name_repetitions,
    reason = "Error types include the module name to indicate their scope"
)]

use std::error::Error as StdError;
use std::fmt;

/// WebSocket error variants.
#[non_exhaustive]
#[derive(Debug)]
pub enum WsError {
    /// Error connecting to or communicating with the WebSocket server
    Connection(tokio_tungstenite::tungstenite::Error),
    /// Error parsing a WebSocket message
    MessageParse(serde_json::Error),
    /// Subscription request failed
    SubscriptionFailed(String),
    /// Authentication failed for authenticated channel
    AuthenticationFailed,
    /// WebSocket connection was closed
    ConnectionClosed,
    /// Operation timed out
    Timeout,
    /// Received an invalid or unexpected message
    InvalidMessage(String),
    /// An incoming frame exceeded [`Config::max_frame_bytes`] and was not parsed
    ///
    /// [`Config::max_frame_bytes`]: crate::ws::config::Config::max_frame_bytes
    FrameOversized {
        /// Actual frame length in bytes
        len: usize,
        /// Configured maximum
        max: usize,
    },
    /// The subscriber lagged behind the broadcast channel and missed frames.
    /// Raw-frame consumers receive this as an ERROR ITEM (data loss is never
    /// a warning-only skip); the stream continues afterwards.
    StreamLagged(u64),
}

impl fmt::Display for WsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Connection(e) => write!(f, "WebSocket connection error: {e}"),
            Self::FrameOversized { len, max } => {
                write!(
                    f,
                    "WebSocket frame of {len} bytes exceeds max_frame_bytes={max}"
                )
            }
            Self::StreamLagged(n) => {
                write!(f, "WebSocket subscriber lagged: {n} frames lost")
            }
            Self::MessageParse(e) => write!(f, "Failed to parse WebSocket message: {e}"),
            Self::SubscriptionFailed(reason) => write!(f, "Subscription failed: {reason}"),
            Self::AuthenticationFailed => write!(f, "WebSocket authentication failed"),
            Self::ConnectionClosed => write!(f, "WebSocket connection closed"),
            Self::Timeout => write!(f, "WebSocket operation timed out"),
            Self::InvalidMessage(msg) => write!(f, "Invalid WebSocket message: {msg}"),
        }
    }
}

impl StdError for WsError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::Connection(e) => Some(e),
            Self::MessageParse(e) => Some(e),
            _ => None,
        }
    }
}

// Integration with main Error type
impl From<WsError> for crate::error::Error {
    fn from(e: WsError) -> Self {
        crate::error::Error::with_source(crate::error::Kind::WebSocket, e)
    }
}

impl From<tokio_tungstenite::tungstenite::Error> for crate::error::Error {
    fn from(e: tokio_tungstenite::tungstenite::Error) -> Self {
        crate::error::Error::with_source(crate::error::Kind::WebSocket, WsError::Connection(e))
    }
}
