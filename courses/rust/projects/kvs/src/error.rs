// 使用ErrorKind与Error结合的方式完成错误处理


/// Error type for kvs.
#[derive(Debug)]
pub enum KvsError {
    /// IO Error
    IoError(std::io::Error),
    /// SerdeJsonError
    SerdeJsonError(serde_json::Error),
    /// UnexpectedCommandType
    UnexpectedCommandType,
    /// Key not found
    KeyNotFound,
}

impl std::fmt::Display for KvsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            KvsError::IoError(e) => write!(f, "IO error: {}", e),
            KvsError::SerdeJsonError(e) => write!(f, "SerdeJson error: {}", e),
            KvsError::UnexpectedCommandType => write!(f, "Unexpected command type"),
            KvsError::KeyNotFound => write!(f, "Key not found"),
        }
    }
}

impl std::error::Error for KvsError {
    // return source error
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            KvsError::IoError(e) => Some(e),
            KvsError::SerdeJsonError(e) => Some(e),
           _ => None,
        }
    }
}

impl From<std::io::Error> for KvsError {
    fn from(value: std::io::Error) -> Self {
        Self::IoError(value)
    }
}

impl From<serde_json::Error> for KvsError {
    fn from(value: serde_json::Error) -> Self {
        Self::SerdeJsonError(value)
    }
}

/// Result type for kvs.
pub type Result<T> = std::result::Result<T, KvsError>;