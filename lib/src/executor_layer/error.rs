use super::super::storage_layer::error::StorageError;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum ExecutorError {
    #[error("Message type mismatch")]
    MessageTypeMismatch,

    #[error("Table already exists")]
    TableAlreadyExists,

    #[error("Table not found")]
    TableNotFound,

    #[error("Underlying error: {0}")]
    StorageError(StorageError),
}

impl<T: Into<StorageError>> From<T> for ExecutorError {
    fn from(item: T) -> Self {
        let storage_err: StorageError = item.into();
        Self::StorageError(storage_err)
    }
}
