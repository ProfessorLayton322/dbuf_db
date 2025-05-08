use super::super::executor_layer::error::ExecutorError;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum PlannerError {
    #[error("Wrong expression operand types")]
    WrongOperandTypes,

    #[error("Ambiguous match return type")]
    AmbiguousMatchType,

    #[error("Empty match cases")]
    EmptyMatchCases,

    #[error("Unexisting message type: {0}")]
    UnexistingMessageType(String),

    #[error("Unexisting enum type: {0}")]
    UnexistingEnumType(String),

    #[error("Duplicate message type: {0}")]
    DuplicateMessageType(String),

    #[error("Duplicate enum type: {0}")]
    DuplicateEnumType(String),

    #[error("Dependency dropped")]
    DependencyDropped,

    #[error("Underlying executor error: {0}")]
    ExecutorError(ExecutorError),
}

impl<T: Into<ExecutorError>> From<T> for PlannerError {
    fn from(item: T) -> Self {
        let executor_err: ExecutorError = item.into();
        Self::ExecutorError(executor_err)
    }
}
