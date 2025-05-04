use std::collections::HashMap;

use super::super::storage_layer::{
    indices::TABLE_STATE_INDEX,
    paged_storage::PagedStorage,
    utils::{load, save},
};
use super::error::ExecutorError;
use super::object_storage::ObjectStorage;
use super::schema::{Message, MessageType};

use bincode::{Decode, Encode};

#[derive(Debug, Encode, Decode)]
pub struct TableManagerState {
    pub tables: HashMap<String, ObjectStorage>,
}

impl TableManagerState {
    pub fn new() -> Self {
        Self {
            tables: HashMap::<String, ObjectStorage>::new(),
        }
    }
}

pub struct TableManager {
    pub state: TableManagerState,
    pub paged_storage: PagedStorage,
}

impl TableManager {
    pub fn new(paged_storage: PagedStorage) -> Result<Self, ExecutorError> {
        match load(paged_storage.marble(), TABLE_STATE_INDEX)? {
            Some(state) => Ok(Self {
                state,
                paged_storage,
            }),
            None => Ok(Self {
                state: TableManagerState::new(),
                paged_storage,
            }),
        }
    }

    pub fn create_table(
        &mut self,
        table_name: String,
        schema: MessageType,
    ) -> Result<(), ExecutorError> {
        if self.state.tables.contains_key(&table_name) {
            return Err(ExecutorError::TableAlreadyExists);
        }

        self.state
            .tables
            .insert(table_name, ObjectStorage::new(schema));

        save(self.paged_storage.marble(), &self.state, TABLE_STATE_INDEX)?;
        self.paged_storage.flush()?;

        Ok(())
    }

    pub fn drop_table(&mut self, table_name: String) -> Result<(), ExecutorError> {
        match self.state.tables.remove(&table_name) {
            Some(mut object_storage) => object_storage.drop_items(&mut self.paged_storage)?,
            None => return Err(ExecutorError::TableNotFound),
        }

        save(self.paged_storage.marble(), &self.state, TABLE_STATE_INDEX)?;
        self.paged_storage.flush()?;

        Ok(())
    }

    pub fn insert_messages<T: Iterator<Item = Message>>(
        &mut self,
        table_name: String,
        messages: T,
    ) -> Result<(), ExecutorError> {
        match self.state.tables.get_mut(&table_name) {
            Some(object_storage) => {
                object_storage.insert_messages(&mut self.paged_storage, messages)?
            }
            None => return Err(ExecutorError::TableNotFound),
        };

        save(self.paged_storage.marble(), &self.state, TABLE_STATE_INDEX)?;
        self.paged_storage.flush()?;

        Ok(())
    }

    pub fn iter(&self, table_name: String) -> Result<impl Iterator<Item = Message>, ExecutorError> {
        match self.state.tables.get(&table_name) {
            Some(object_storage) => Ok(object_storage.iter(&self.paged_storage)),
            None => return Err(ExecutorError::TableNotFound),
        }
    }

    //TODO message deletions with FnMut
}
