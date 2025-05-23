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

    #[error("Column not found: {0}")]
    ColumnNotFound(String),

    #[error("Unexisting message type: {0}")]
    UnexistingMessageType(String),

    #[error("Unexisting enum type: {0}")]
    UnexistingEnumType(String),

    #[error("Mismatched field types for type: {0}")]
    MismatchedFieldTypes(String),

    #[error("Enum variant not found {0} {1}")]
    EnumVariantNotFound(String, String),

    #[error("Ill formed match statement")]
    IllFormedMatchStatement,

    #[error("Duplicate message type: {0}")]
    DuplicateMessageType(String),

    #[error("Duplicate enum type: {0}")]
    DuplicateEnumType(String),

    #[error("Dependency dropped by projection")]
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
