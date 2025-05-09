use std::boxed::Box;
use std::string::String;
use std::vec::Vec;

use super::super::executor_layer::{expression::Expression, schema::*};

use super::raw_expression::RawExpression;

#[derive(Debug, PartialEq, Clone)]
pub enum RawPlan {
    Scan {
        table_name: String,
    },
    Filter {
        raw_expression: RawExpression,
        source: Box<RawPlan>,
    },
    Projection {
        raw_expressions: Vec<(String, RawExpression)>,
        source: Box<RawPlan>,
    },
}

#[derive(Debug, PartialEq, Clone)]
pub enum LogicalPlan {
    Scan {
        table_name: String,
        message_type: MessageType,
    },
    Filter {
        expression: Expression,
        source: Box<LogicalPlan>,
        message_type: MessageType,
    },
    Projection {
        expressions: Vec<(String, Expression)>,
        source: Box<LogicalPlan>,
        message_type: MessageType,
    },
}

impl LogicalPlan {
    pub fn get_message_type(&self) -> &MessageType {
        match self {
            LogicalPlan::Scan {
                table_name: _,
                message_type,
            } => message_type,
            LogicalPlan::Filter {
                expression: _,
                source: _,
                message_type,
            } => message_type,
            LogicalPlan::Projection {
                expressions: _,
                source: _,
                message_type,
            } => message_type,
        }
    }
}
