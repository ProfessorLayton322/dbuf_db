use std::string::String;

use super::super::executor_layer::{expression::*, schema::*};
use super::error::PlannerError;

#[derive(Debug, Clone, PartialEq)]
pub enum RawExpression {
    Literal(DBValue),
    ColumnRef(String),
    BinaryOp {
        op: BinaryOperator,
        left: Box<RawExpression>,
        right: Box<RawExpression>,
    },
    UnaryOp {
        op: RawUnaryOperator,
        expr: Box<RawExpression>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum RawUnaryOperator {
    Negate,                        // -
    Not,                           // NOT
    MessageField(String),          // foo.bar
    EnumMatch(Vec<RawExpression>), // match enum, foo => bar, lol => kek etc
}
