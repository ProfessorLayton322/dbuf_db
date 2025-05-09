use std::collections::HashMap;
use std::ops::Deref;
use std::string::String;

use super::super::executor_layer::{expression::*, schema::*, table_manager::TableManager};
use super::super::storage_layer::{
    indices::PLANNER_STATE_INDEX,
    utils::{load, save},
};

use super::error::PlannerError;
use super::logical_plan::*;
use super::raw_expression::*;

use bincode::{Decode, Encode};

#[derive(Debug, Encode, Decode, Default)]
pub struct QueryPlannerState {
    message_types: HashMap<String, MessageType>,
    enum_types: HashMap<String, EnumType>,
}

pub struct QueryPlanner {
    pub table_manager: TableManager,
    state: QueryPlannerState,
}

impl QueryPlanner {
    pub fn new(table_manager: TableManager) -> Result<Self, PlannerError> {
        match load(table_manager.paged_storage.marble(), PLANNER_STATE_INDEX)? {
            Some(state) => Ok(Self {
                table_manager,
                state,
            }),
            None => Ok(Self {
                table_manager,
                state: QueryPlannerState::default(),
            }),
        }
    }

    pub fn add_message_type(
        &mut self,
        type_name: String,
        message_type: MessageType,
    ) -> Result<(), PlannerError> {
        if self.state.message_types.contains_key(&type_name) {
            return Err(PlannerError::DuplicateMessageType(type_name));
        }

        self.state.message_types.insert(type_name, message_type);
        save(
            self.table_manager.paged_storage.marble(),
            &self.state,
            PLANNER_STATE_INDEX,
        )?;

        Ok(())
    }

    pub fn get_message_type(&self, type_name: &String) -> Result<MessageType, PlannerError> {
        match self.state.message_types.get(type_name) {
            Some(message_type) => Ok(message_type.clone()),
            None => Err(PlannerError::UnexistingMessageType(type_name.clone())),
        }
    }

    pub fn add_enum_type(
        &mut self,
        type_name: String,
        enum_type: EnumType,
    ) -> Result<(), PlannerError> {
        if self.state.enum_types.contains_key(&type_name) {
            return Err(PlannerError::DuplicateEnumType(type_name));
        }

        self.state.enum_types.insert(type_name, enum_type);
        save(
            self.table_manager.paged_storage.marble(),
            &self.state,
            PLANNER_STATE_INDEX,
        )?;

        Ok(())
    }

    pub fn get_enum_type(&self, type_name: &String) -> Result<EnumType, PlannerError> {
        match self.state.enum_types.get(type_name) {
            Some(enum_type) => Ok(enum_type.clone()),
            None => Err(PlannerError::UnexistingEnumType(type_name.clone())),
        }
    }

    //returns Some(column_index) only if expression is a chain of unary operators on top of a
    //column ref
    fn get_leaf_ref(expression: &Expression) -> Option<usize> {
        match expression {
            Expression::ColumnRef(column_index) => Some(*column_index),
            Expression::UnaryOp { op: _, expr } => Self::get_leaf_ref(expr.deref()),
            _ => None,
        }
    }

    fn is_complex_type(db_type: &DBType) -> bool {
        matches!(db_type, DBType::MessageType(_) | DBType::EnumType(_))
    }

    pub fn get_column_index(
        column_name: &String,
        message_type: &MessageType,
    ) -> Result<usize, PlannerError> {
        for i in 0usize..message_type.columns.len() {
            if column_name == &message_type.columns[i].column_name {
                return Ok(i);
            }
        }
        Err(PlannerError::ColumnNotFound(column_name.clone()))
    }

    pub fn build_expression(
        &self,
        raw_expression: &RawExpression,
        message_type: &MessageType,
    ) -> Result<Expression, PlannerError> {
        match raw_expression {
            RawExpression::Literal(db_value) => Ok(Expression::Literal(db_value.clone())),
            RawExpression::ColumnRef(column_name) => Ok(Expression::ColumnRef(
                Self::get_column_index(column_name, message_type)?,
            )),
            RawExpression::BinaryOp { op, left, right } => {
                let left_expression = self.build_expression(left.deref(), message_type)?;
                let right_expression = self.build_expression(right.deref(), message_type)?;
                Ok(Expression::BinaryOp {
                    op: *op,
                    left: Box::new(left_expression),
                    right: Box::new(right_expression),
                })
            }
            RawExpression::UnaryOp { op, expr } => {
                let expression = self.build_expression(expr.deref(), message_type)?;

                match op {
                    RawUnaryOperator::Negate => Ok(Expression::UnaryOp {
                        op: UnaryOperator::Negate,
                        expr: Box::new(expression),
                    }),
                    RawUnaryOperator::Not => Ok(Expression::UnaryOp {
                        op: UnaryOperator::Not,
                        expr: Box::new(expression),
                    }),
                    RawUnaryOperator::MessageField(field_name) => {
                        let deduced_type =
                            self.deduce_expression_type(&expression, message_type)?;
                        if let DBType::MessageType(message_type) = deduced_type {
                            Ok(Expression::UnaryOp {
                                op: UnaryOperator::MessageField(Self::get_column_index(
                                    field_name,
                                    &message_type,
                                )?),
                                expr: Box::new(expression),
                            })
                        } else {
                            Err(PlannerError::WrongOperandTypes)
                        }
                    }
                    RawUnaryOperator::EnumMatch(raw_expressions) => {
                        let deduced_type =
                            self.deduce_expression_type(&expression, message_type)?;
                        if let DBType::EnumType(enum_type) = deduced_type {
                            if raw_expressions.len() != enum_type.variants.len() {
                                return Err(PlannerError::WrongOperandTypes);
                            }

                            let result: Result<Vec<Expression>, PlannerError> = raw_expressions
                                .iter()
                                .zip(enum_type.variants.iter())
                                .map(|(raw_expression, variant)| {
                                    let variant_message_type: MessageType = variant.into();
                                    self.build_expression(raw_expression, &variant_message_type)
                                })
                                .collect();

                            Ok(Expression::UnaryOp {
                                op: UnaryOperator::EnumMatch(result?),
                                expr: Box::new(expression),
                            })
                        } else {
                            Err(PlannerError::WrongOperandTypes)
                        }
                    }
                }
            }
        }
    }

    pub fn build_logical_plan(&self, raw_plan: &RawPlan) -> Result<LogicalPlan, PlannerError> {
        let logical_plan = match raw_plan {
            RawPlan::Scan { table_name } => LogicalPlan::Scan {
                table_name: table_name.clone(),
                message_type: self.table_manager.schema(table_name.clone())?,
            },
            RawPlan::Filter {
                raw_expression,
                source,
            } => {
                let logical_source = self.build_logical_plan(source.deref())?;
                let message_type = logical_source.get_message_type().clone();
                let expression = self.build_expression(raw_expression, &message_type)?;
                let boxed = Box::new(logical_source);

                LogicalPlan::Filter {
                    expression: expression.clone(),
                    source: boxed,
                    message_type,
                }
            }
            RawPlan::Projection {
                raw_expressions,
                source,
            } => {
                let logical_source = self.build_logical_plan(source.deref())?;
                let source_type = logical_source.get_message_type().clone();
                let boxed = Box::new(logical_source);

                let try_convert: Result<Vec<Expression>, PlannerError> = raw_expressions
                    .iter()
                    .map(|raw_expression| self.build_expression(&raw_expression.1, &source_type))
                    .collect();
                let expressions: Vec<(String, Expression)> = try_convert?
                    .into_iter()
                    .zip(raw_expressions.iter())
                    .map(|(expression, raw)| (raw.0.clone(), expression))
                    .collect();

                // The logic behind dependencies and projection:
                //
                // Kepp track of all column refs from old message type, make a map of their new
                //indeices
                //
                // For every expression that returns message or enum we can be sure that it is a
                // chain of unary operators that ends either with a message/enum literal (that has
                // no depencies among the columns) or with a column ref
                //
                // This way we can determine dependencies for each message/enum expression
                let mut types: Vec<DBType> = vec![];
                let mut ref_map = HashMap::<usize, usize>::new();

                for (i, expression) in expressions.iter().enumerate() {
                    if let Expression::ColumnRef(index) = expression.1 {
                        ref_map.insert(index, i);
                    }
                    types.push(self.deduce_expression_type(&expression.1, &source_type)?);
                }

                let mut deps: Vec<Vec<usize>> = vec![];

                for i in 0..types.len() {
                    deps.push(vec![]);
                    if !Self::is_complex_type(&types[i]) {
                        continue;
                    }

                    if let Some(index) = Self::get_leaf_ref(&expressions[i].1) {
                        for dep in source_type.columns[index].dependencies.iter() {
                            if !ref_map.contains_key(dep) {
                                return Err(PlannerError::DependencyDropped);
                            }
                            deps.last_mut().unwrap().push(*ref_map.get(dep).unwrap());
                        }
                    }
                }

                let final_message_type = MessageType {
                    name: "".to_owned(),
                    columns: expressions
                        .iter()
                        .zip(types.iter())
                        .zip(deps.iter())
                        .map(|((expression, db_type), dep)| Column {
                            column_name: expression.0.clone(),
                            column_type: db_type.clone(),
                            dependencies: dep.clone(),
                        })
                        .collect(),
                };

                LogicalPlan::Projection {
                    expressions,
                    source: boxed,
                    message_type: final_message_type,
                }
            }
        };

        Ok(logical_plan)
    }

    pub fn deduce_expression_type(
        &self,
        expression: &Expression,
        message_type: &MessageType,
    ) -> Result<DBType, PlannerError> {
        let deduced_type = match expression {
            Expression::Literal(literal) => self.deduce_literal_type(literal)?,
            Expression::ColumnRef(index) => message_type.columns[*index].column_type.clone(),
            Expression::BinaryOp { op, left, right } => {
                let left_type = self.deduce_expression_type(left.deref(), message_type)?;
                let right_type = self.deduce_expression_type(right.deref(), message_type)?;
                self.deduce_binary_op_type(*op, left_type, right_type)?
            }
            Expression::UnaryOp { op, expr } => {
                let db_type = self.deduce_expression_type(expr.deref(), message_type)?;
                self.deduce_unary_op_type(op, &db_type)?
            }
        };

        Ok(deduced_type)
    }

    pub fn deduce_literal_type(&self, value: &DBValue) -> Result<DBType, PlannerError> {
        let deduced_type = match value {
            DBValue::Bool(_) => DBType::Bool,
            DBValue::Double(_) => DBType::Double,
            DBValue::Int(_) => DBType::Int,
            DBValue::UInt(_) => DBType::UInt,
            DBValue::String(_) => DBType::String,
            DBValue::Message(message) => {
                DBType::MessageType(self.get_message_type(message.type_name.as_ref().unwrap())?)
            }
            DBValue::EnumValue(enum_value) => {
                DBType::EnumType(self.get_enum_type(enum_value.type_name.as_ref().unwrap())?)
            }
        };

        Ok(deduced_type)
    }

    pub fn deduce_binary_op_type(
        &self,
        op: BinaryOperator,
        left_type: DBType,
        right_type: DBType,
    ) -> Result<DBType, PlannerError> {
        //later we assume that types are equal
        if left_type != right_type {
            return Err(PlannerError::WrongOperandTypes);
        }

        match op {
            //operators that are applied to numeric types
            BinaryOperator::Add
            | BinaryOperator::Subtract
            | BinaryOperator::Multiply
            | BinaryOperator::Divide => {
                if left_type == DBType::Double
                    || left_type == DBType::UInt
                    || left_type == DBType::Int
                {
                    return Ok(left_type);
                }
                Err(PlannerError::WrongOperandTypes)
            }
            BinaryOperator::Equals | BinaryOperator::NotEquals => Ok(left_type),
            //these can be applied to numeric types and strings
            BinaryOperator::LessThan | BinaryOperator::GreaterThan => {
                if left_type == DBType::Double
                    || left_type == DBType::UInt
                    || left_type == DBType::Int
                    || left_type == DBType::String
                {
                    return Ok(left_type);
                }
                Err(PlannerError::WrongOperandTypes)
            }
            BinaryOperator::And | BinaryOperator::Or => {
                if left_type == DBType::Bool {
                    return Ok(DBType::Bool);
                }
                Err(PlannerError::WrongOperandTypes)
            }
        }
    }

    pub fn deduce_unary_op_type(
        &self,
        op: &UnaryOperator,
        db_type: &DBType,
    ) -> Result<DBType, PlannerError> {
        match op {
            UnaryOperator::Negate => {
                if *db_type == DBType::Double || *db_type == DBType::Int {
                    return Ok(db_type.clone());
                }
                Err(PlannerError::WrongOperandTypes)
            }
            UnaryOperator::Not => {
                if *db_type == DBType::Bool {
                    return Ok(DBType::Bool);
                }
                Err(PlannerError::WrongOperandTypes)
            }
            UnaryOperator::MessageField(index) => match db_type {
                DBType::MessageType(message_type) => {
                    if *index >= message_type.columns.len() {
                        return Err(PlannerError::WrongOperandTypes);
                    }
                    Ok(message_type.columns[*index].column_type.clone())
                }
                _ => Err(PlannerError::WrongOperandTypes),
            },
            UnaryOperator::EnumMatch(expressions) => match db_type {
                DBType::EnumType(enum_type) => {
                    if expressions.is_empty() {
                        return Err(PlannerError::EmptyMatchCases);
                    }

                    if expressions.len() != enum_type.variants.len() {
                        return Err(PlannerError::WrongOperandTypes);
                    }

                    let iter = expressions.iter().zip(enum_type.variants.iter()).map(
                        |(expression, variant)| {
                            let message_type: MessageType = variant.into();
                            self.deduce_expression_type(expression, &message_type)
                        },
                    );

                    let mut types: Vec<DBType> = vec![];
                    for result in iter {
                        types.push(result?);
                    }
                    let first = &types[0];

                    if !types.iter().all(|db_type| db_type == first) {
                        return Err(PlannerError::AmbiguousMatchType);
                    }

                    Ok(first.clone())
                }
                _ => Err(PlannerError::WrongOperandTypes),
            },
        }
    }
}
