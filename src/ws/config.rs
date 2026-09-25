#![expect(
    clippy::module_name_repetitions,
    reason = "Configuration types intentionally mirror the module name for clarity"
)]

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use backoff::{ExponentialBackoff, ExponentialBackoffBuilder};

const DEFAULT_HEARTBEAT_INTERVAL_DURATION: Duration = Duration::from_secs(5);
const DEFAULT_HEARTBEAT_TIMEOUT_DURATION: Duration = Duration::from_secs(15);
const DEFAULT_INITIAL_BACKOFF_DURATION: Duration = Duration::from_secs(1);
const DEFAULT_MAX_BACKOFF_DURATION: Duration = Duration::from_secs(60);
const DEFAULT_BACKOFF_MULTIPLIER: f64 = 2.0;

/// A synchronous observation from the WebSocket transport.
///
/// Observer callbacks must not panic or block.
#[non_exhaustive]
#[derive(Debug)]
pub enum WebSocketObservation {
    /// A WebSocket handshake completed.
    Connected {
        /// Time since the previous established connection ended, or `None` for the first connection.
        reconnect_duration: Option<Duration>,
    },
    /// An established WebSocket connection ended.
    Disconnected,
    /// A close frame was received from the peer.
    CloseFrame {
        /// The close code, when the frame included one.
        code: Option<u16>,
    },
    /// A non-PONG text message was received.
    TextMessage {
        /// UTF-8 payload length, excluding WebSocket and TLS headers.
        bytes: usize,
    },
    /// A text message was decoded.
    Decoded {
        /// Time spent in the parser.
        duration: Duration,
        /// Number of messages returned by the parser, or zero on error.
        messages: usize,
        /// Whether parsing failed.
        failed: bool,
    },
    /// A subscription receiver skipped broadcast deliveries because it lagged.
    ReceiverLagged {
        /// Number of skipped broadcast deliveries.
        messages: u64,
    },
}

/// Configuration for WebSocket client behavior.
#[non_exhaustive]
#[derive(Clone)]
pub struct Config {
    /// Interval for sending PING messages to keep connection alive
    pub heartbeat_interval: Duration,
    /// Maximum time to wait for PONG response before considering connection dead
    pub heartbeat_timeout: Duration,
    /// Reconnection strategy configuration
    pub reconnect: ReconnectConfig,
    /// Optional synchronous observer for WebSocket transport measurements.
    ///
    /// The callback runs inline and must not panic or block.
    pub observer: Option<Arc<dyn Fn(WebSocketObservation) + Send + Sync>>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            heartbeat_interval: DEFAULT_HEARTBEAT_INTERVAL_DURATION,
            heartbeat_timeout: DEFAULT_HEARTBEAT_TIMEOUT_DURATION,
            reconnect: ReconnectConfig::default(),
            observer: None,
        }
    }
}

impl fmt::Debug for Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Config")
            .field("heartbeat_interval", &self.heartbeat_interval)
            .field("heartbeat_timeout", &self.heartbeat_timeout)
            .field("reconnect", &self.reconnect)
            .field("observer", &self.observer.as_ref().map(|_| ".."))
            .finish()
    }
}

/// Configuration for automatic reconnection behavior.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct ReconnectConfig {
    /// Maximum number of reconnection attempts before giving up.
    /// `None` means infinite retries.
    pub max_attempts: Option<u32>,
    /// Initial backoff duration for first reconnection attempt
    pub initial_backoff: Duration,
    /// Maximum backoff duration
    pub max_backoff: Duration,
    /// Multiplier for exponential backoff
    pub backoff_multiplier: f64,
}

impl Default for ReconnectConfig {
    fn default() -> Self {
        Self {
            max_attempts: None, // Infinite reconnection by default
            initial_backoff: DEFAULT_INITIAL_BACKOFF_DURATION,
            max_backoff: DEFAULT_MAX_BACKOFF_DURATION,
            backoff_multiplier: DEFAULT_BACKOFF_MULTIPLIER,
        }
    }
}

impl From<ReconnectConfig> for ExponentialBackoff {
    fn from(config: ReconnectConfig) -> Self {
        ExponentialBackoffBuilder::default()
            .with_initial_interval(config.initial_backoff)
            .with_max_interval(config.max_backoff)
            .with_multiplier(config.backoff_multiplier)
            .with_max_elapsed_time(None) // We handle max attempts separately
            .build()
    }
}

#[cfg(test)]
mod tests {
    use backoff::backoff::Backoff as _;

    use super::*;

    #[test]
    fn backoff_sequence() {
        let config = ReconnectConfig::default();
        let mut backoff: ExponentialBackoff = config.into();

        // First backoff should be around initial_backoff (with some jitter)
        let first = backoff.next_backoff().unwrap();
        assert!(first >= Duration::from_millis(500) && first <= Duration::from_millis(1500));
    }

    #[test]
    fn backoff_respects_max() {
        let config = ReconnectConfig {
            initial_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(2),
            backoff_multiplier: 3.0,
            max_attempts: None,
        };
        let mut backoff: ExponentialBackoff = config.into();

        // Exhaust several iterations
        for _ in 0..10 {
            let _next = backoff.next_backoff();
        }

        // Should still return values capped at max
        let duration = backoff.next_backoff().unwrap();
        assert!(duration <= Duration::from_secs(3));
    }

    #[test]
    fn default_heartbeat_is_five_seconds() {
        let config = Config::default();
        assert_eq!(config.heartbeat_interval, Duration::from_secs(5));
    }
}
