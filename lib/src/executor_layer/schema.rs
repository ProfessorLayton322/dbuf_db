use std::convert::From;
use std::string::String;
use std::vec::Vec;

use bincode::{Decode, Encode};

//TODO add enums
#[derive(PartialEq, Debug, Clone, Encode, Decode)]
pub enum DBType {
    Bool,
    Double,
    Int,
    UInt,
    String,
    MessageType(MessageType),
    EnumType(EnumType),
}

#[derive(PartialEq, Debug, Clone, Encode, Decode)]
pub struct Column {
    pub column_name: String,
    pub column_type: DBType,
    pub dependencies: Vec<usize>,
}

//Message constructor arguments are stored as columns
#[derive(PartialEq, Debug, Clone, Encode, Decode)]
pub struct MessageType {
    pub name: String,
    pub columns: Vec<Column>,
}

#[derive(PartialEq, Debug, Clone, Encode, Decode)]
pub enum DBValue {
    Bool(bool),
    Double(f32),
    Int(i32),
    UInt(u32),
    String(String),
    Message(Message),
    EnumValue(EnumValue),
}

#[derive(PartialEq, Debug, Clone, Encode, Decode)]
pub struct Message {
    //Should be not None for literals only
    pub type_name: Option<String>,
    pub fields: Vec<DBValue>,
}

impl MessageType {
    pub fn match_message(&self, message: &Message) -> bool {
        if self.columns.len() != message.fields.len() {
            return false;
        }

        self.columns
            .iter()
            .zip(message.fields.iter())
            .all(|(column, field)| match_type_value(&column.column_type, field))
    }
}

#[derive(PartialEq, Debug, Clone, Encode, Decode)]
pub struct EnumVariantType {
    pub name: String,
    //name, type
    pub content: Vec<(String, DBType)>,
}

impl From<&EnumVariantType> for MessageType {
    fn from(variant_type: &EnumVariantType) -> Self {
        Self {
            name: variant_type.name.clone(),
            columns: variant_type
                .content
                .iter()
                .map(|(column_name, field_type)| Column {
                    column_name: column_name.clone(),
                    column_type: field_type.clone(),
                    dependencies: vec![],
                })
                .collect(),
        }
    }
}

#[derive(PartialEq, Debug, Clone, Encode, Decode)]
pub struct EnumType {
    pub name: String,
    //name, type
    pub dependencies: Vec<(String, DBType)>,
    pub variants: Vec<EnumVariantType>,
}

#[derive(PartialEq, Debug, Clone, Encode, Decode)]
pub struct EnumValue {
    //only for enum literals
    pub type_name: Option<String>,
    pub dependencies: Vec<DBValue>,
    pub choice: usize,
    pub values: Vec<DBValue>,
}

impl EnumType {
    pub fn match_enum(&self, enum_value: &EnumValue) -> bool {
        self.dependencies.len() == enum_value.dependencies.len()
            && self
                .dependencies
                .iter()
                .zip(enum_value.dependencies.iter())
                .all(|(dep_type, dep_value)| match_type_value(&dep_type.1, dep_value))
            && enum_value.choice < self.variants.len()
            && self.variants[enum_value.choice].content.len() == enum_value.values.len()
            && self.variants[enum_value.choice]
                .content
                .iter()
                .zip(enum_value.values.iter())
                .all(|(db_type, db_value)| match_type_value(&db_type.1, db_value))
    }
}

pub fn match_type_value(db_type: &DBType, db_value: &DBValue) -> bool {
    match (db_type, db_value) {
        (DBType::Bool, DBValue::Bool(_)) => true,
        (DBType::Double, DBValue::Double(_)) => true,
        (DBType::Int, DBValue::Int(_)) => true,
        (DBType::UInt, DBValue::UInt(_)) => true,
        (DBType::String, DBValue::String(_)) => true,
        (DBType::MessageType(message_type), DBValue::Message(message)) => {
            message_type.match_message(message)
        }
        (DBType::EnumType(enum_type), DBValue::EnumValue(enum_value)) => {
            enum_type.match_enum(enum_value)
        }
        (_, _) => false,
    }
}
