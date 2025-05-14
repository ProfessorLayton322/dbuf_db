use super::ast::{Expression, BinaryOperator, UnaryOperator};

pub fn bin_op(op: BinaryOperator, left: Expression, right: Expression) -> Expression {
    Expression::BinaryOp {
        op,
        left: Box::new(left),
        right: Box::new(right),
    }
}

pub fn unary_op(op: UnaryOperator, expr: Expression) -> Expression {
    Expression::UnaryOp {
        op,
        expr: Box::new(expr),
    }
}
