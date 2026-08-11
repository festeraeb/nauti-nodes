use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("node not found: {0}")]
    NodeNotFound(String),
    #[error("job not found: {0}")]
    JobNotFound(String),
    #[error("no eligible nodes")]
    NoNodes,
    #[error("quota exceeded for api key")]
    QuotaExceeded,
    #[error("auth failed: {0}")]
    Auth(String),
    #[error("config error: {0}")]
    Config(String),
    #[error("database error: {0}")]
    Db(#[from] rusqlite::Error),
    #[error("channel closed")]
    ChannelClosed,
    #[error("http error: {0}")]
    Http(String),
    #[error("inference error: {0}")]
    Inference(String),
    #[error("{0}")]
    Internal(String),
}
