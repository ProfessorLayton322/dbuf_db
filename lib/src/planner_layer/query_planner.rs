use std::collections::HashMap;
use std::ops::Deref;
use std::string::String;

use super::super::executor_layer::{expression::*, schema::*, table_manager::TableManager};
use super::super::storage_layer::{
    indices::PLANNER_STATE_INDEX,
    utils::{load, save},
};

use super::error::PlannerError;

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
                            self.deduce_expression_type(&expression, &message_type)
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
