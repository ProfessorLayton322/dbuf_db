use std::boxed::Box;

use super::schema::*;

#[derive(Debug, Clone)]
pub enum Expression {
    Literal(DBValue),
    ColumnRef(usize),
    BinaryOp {
        op: BinaryOperator,
        left: Box<Expression>,
        right: Box<Expression>,
    },
    UnaryOp {
        op: UnaryOperator,
        expr: Box<Expression>,
    },
}

impl Expression {
    pub fn evaluate(&self, message: Message) -> DBValue {
        match self {
            Expression::Literal(value) => value.clone(),
            Expression::ColumnRef(index) => message.fields[*index].clone(),
            _ => panic!("LOL"),
        }
    }
}

#[derive(Debug, Clone)]
pub enum BinaryOperator {
    Add,         // +
    Subtract,    // -
    Multiply,    // *
    Divide,      // /
    Equals,      // =
    NotEquals,   // !=
    LessThan,    // <
    GreaterThan, // >
    And,         // &
    Or,          // |
}

impl BinaryOperator {
    pub fn apply(&self, left: DBValue, right: DBValue) -> DBValue {
        match self {
            BinaryOperator::Add => match (left, right) {
                (DBValue::Double(l), DBValue::Double(r)) => DBValue::Double(l + r),
                (DBValue::Int(l), DBValue::Int(r)) => DBValue::Int(l + r),
                (DBValue::UInt(l), DBValue::UInt(r)) => DBValue::UInt(l + r),
                (_, _) => panic!("Incorrect addition"),
            },
            BinaryOperator::Subtract => match (left, right) {
                (DBValue::Double(l), DBValue::Double(r)) => DBValue::Double(l - r),
                (DBValue::Int(l), DBValue::Int(r)) => DBValue::Int(l - r),
                (DBValue::UInt(l), DBValue::UInt(r)) => DBValue::UInt(l - r),
                (_, _) => panic!("Incorrect subtraction"),
            },
            BinaryOperator::Multiply => match (left, right) {
                (DBValue::Double(l), DBValue::Double(r)) => DBValue::Double(l * r),
                (DBValue::Int(l), DBValue::Int(r)) => DBValue::Int(l * r),
                (DBValue::UInt(l), DBValue::UInt(r)) => DBValue::UInt(l * r),
                (_, _) => panic!("Incorrect multiplication"),
            },
            BinaryOperator::Divide => match (left, right) {
                (DBValue::Double(l), DBValue::Double(r)) => DBValue::Double(l / r),
                (DBValue::Int(l), DBValue::Int(r)) => DBValue::Int(l / r),
                (DBValue::UInt(l), DBValue::UInt(r)) => DBValue::UInt(l / r),
                (_, _) => panic!("Incorrect division"),
            },
            BinaryOperator::Equals => DBValue::Bool(left == right),
            BinaryOperator::NotEquals => DBValue::Bool(left != right),
            BinaryOperator::LessThan => match (left, right) {
                (DBValue::Double(l), DBValue::Double(r)) => DBValue::Bool(l < r),
                (DBValue::Int(l), DBValue::Int(r)) => DBValue::Bool(l < r),
                (DBValue::UInt(l), DBValue::UInt(r)) => DBValue::Bool(l < r),
                (DBValue::String(l), DBValue::String(r)) => DBValue::Bool(l < r),
                (_, _) => panic!("Incorrect comparison"),
            },
            BinaryOperator::GreaterThan => match (left, right) {
                (DBValue::Double(l), DBValue::Double(r)) => DBValue::Bool(l > r),
                (DBValue::Int(l), DBValue::Int(r)) => DBValue::Bool(l > r),
                (DBValue::UInt(l), DBValue::UInt(r)) => DBValue::Bool(l > r),
                (DBValue::String(l), DBValue::String(r)) => DBValue::Bool(l > r),
                (_, _) => panic!("Incorrect comparison"),
            },
            BinaryOperator::And => match (left, right) {
                (DBValue::Bool(l), DBValue::Bool(r)) => DBValue::Bool(l & r),
                (_, _) => panic!("Incorrect binary AND"),
            },
            BinaryOperator::Or => match (left, right) {
                (DBValue::Bool(l), DBValue::Bool(r)) => DBValue::Bool(l | r),
                (_, _) => panic!("Incorrect binary AND"),
            },
        }
    }
}

#[derive(Debug, Clone)]
pub enum UnaryOperator {
    Negate, // -
    Not,    // NOT
}
