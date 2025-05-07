use std::iter::Iterator;
use std::ops::DerefMut;

use super::error::ExecutorError;
use super::expression::Expression;
use super::object_storage::MessageIterator;
use super::schema::{DBValue, Message};
use super::table_manager::TableManager;

//At this stage we assume all physical operators are correctly planned by the query planner
pub trait PhysicalOperator: Iterator<Item = Message> {
    //The contract is to call open before calling next
    fn open(&mut self) -> Result<(), ExecutorError>;
}

pub struct TableScan<'a> {
    table_manager: &'a TableManager,
    table_name: String,
    iterator: Option<MessageIterator<'a, 'a>>,
}

impl<'a> TableScan<'a> {
    pub fn new(table_manager: &'a TableManager, table_name: String) -> Self {
        Self {
            table_manager,
            table_name,
            iterator: None,
        }
    }
}

impl Iterator for TableScan<'_> {
    type Item = Message;

    fn next(&mut self) -> Option<Self::Item> {
        self.iterator.as_mut().and_then(|iter| iter.next())
    }
}

impl PhysicalOperator for TableScan<'_> {
    fn open(&mut self) -> Result<(), ExecutorError> {
        self.iterator = Some(self.table_manager.iter(self.table_name.clone())?);
        Ok(())
    }
}

pub struct Projection<'a> {
    pub expressions: Vec<Expression>,
    pub source: Box<dyn PhysicalOperator + 'a>,
}

impl Projection<'_> {
    pub fn project(&self, message: Message) -> Message {
        Message {
            fields: self
                .expressions
                .iter()
                .map(|expression| expression.evaluate(&message))
                .collect(),
        }
    }
}

impl Iterator for Projection<'_> {
    type Item = Message;

    fn next(&mut self) -> Option<Self::Item> {
        self.source
            .deref_mut()
            .next()
            .map(|item| self.project(item))
    }
}

impl PhysicalOperator for Projection<'_> {
    fn open(&mut self) -> Result<(), ExecutorError> {
        self.source.deref_mut().open()
    }
}

pub struct Filter<'a> {
    //the expression must always return DBValue::Bool
    pub filter_expr: Expression,
    pub source: Box<dyn PhysicalOperator + 'a>,
}

impl Iterator for Filter<'_> {
    type Item = Message;

    fn next(&mut self) -> Option<Self::Item> {
        //cant use filter here bc it consumes the iterator
        while let Some(message) = self.source.deref_mut().next() {
            if self.filter_expr.evaluate(&message) == DBValue::Bool(true) {
                return Some(message);
            }
        }
        None
    }
}

impl PhysicalOperator for Filter<'_> {
    fn open(&mut self) -> Result<(), ExecutorError> {
        self.source.deref_mut().open()
    }
}

//TODO order by, group by, join
//TODO set union
