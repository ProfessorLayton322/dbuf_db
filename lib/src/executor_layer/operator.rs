use std::iter::Iterator;
use std::ops::DerefMut;

use super::error::ExecutorError;
use super::object_storage::MessageIterator;
use super::schema::Message;
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

impl Iterator for TableScan<'_> {
    type Item = Message;

    fn next(&mut self) -> Option<Self::Item> {
        self.iterator.as_mut().map(|iter| iter.next()).flatten()
    }
}

impl PhysicalOperator for TableScan<'_> {
    fn open(&mut self) -> Result<(), ExecutorError> {
        self.iterator = Some(self.table_manager.iter(self.table_name.clone())?);
        Ok(())
    }
}

pub struct Projection {
    //Which columns to leave
    columns: Vec<bool>,
    source: Box<dyn PhysicalOperator>,
}

impl Projection {
    pub fn project(&self, message: Message) -> Message {
        Message {
            fields: message
                .fields
                .into_iter()
                .zip(self.columns.iter())
                .filter_map(|(field, status)| {
                    if *status {
                        return Some(field);
                    }
                    None
                })
                .collect(),
        }
    }
}

impl Iterator for Projection {
    type Item = Message;

    fn next(&mut self) -> Option<Self::Item> {
        self.source
            .deref_mut()
            .next()
            .map(|item| self.project(item))
    }
}

impl PhysicalOperator for Projection {
    fn open(&mut self) -> Result<(), ExecutorError> {
        self.source.deref_mut().open()
    }
}
