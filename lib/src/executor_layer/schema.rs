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
    pub columns: Vec<Column>,
}

#[derive(PartialEq, Debug, Clone, Encode, Decode)]
pub enum Field {
    Bool(bool),
    Double(f32),
    Int(i32),
    UInt(u32),
    String(String),
    Message(Message),
}

#[derive(PartialEq, Debug, Clone, Encode, Decode)]
pub struct Message {
    pub fields: Vec<Field>,
}

impl MessageType {
    pub fn match_message(&self, message: &Message) -> bool {
        if self.columns.len() != message.fields.len() {
            return false;
        }

        self.columns
            .iter()
            .zip(message.fields.iter())
            .map(|(column, field)| match (&column.column_type, field) {
                (DBType::Bool, Field::Bool(_)) => true,
                (DBType::Double, Field::Double(_)) => true,
                (DBType::Int, Field::Int(_)) => true,
                (DBType::UInt, Field::UInt(_)) => true,
                (DBType::String, Field::String(_)) => true,
                (DBType::MessageType(message_type), Field::Message(message)) => {
                    message_type.match_message(message)
                }
                (_, _) => false,
            })
            .fold(true, |acc, x| acc & x)
    }
}
