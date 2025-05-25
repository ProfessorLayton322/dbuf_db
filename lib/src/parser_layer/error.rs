use thiserror::Error;

#[derive(Error, Debug)]
pub enum ParsingError {
    #[error("Could not parse types")]
    TypesParseError,

    #[error("Cant deduce field type : {0}")]
    CantDeduceFieldType(String),

    #[error("Unknown type : {0}")]
    UnknownType(String),

    #[error("Duplicate field: {0}")]
    DuplicateField(String),

    #[error("Duplicate type name: {0}")]
    DuplicateTypeName(String),

    #[error("Field not found: {0}")]
    FieldNotFound(String),

    #[error("Invalid argument")]
    InvalidArgument,
}
