use std::collections::HashMap;
use std::vec::Vec;

use super::super::storage_layer::{
    page::{PageId, PageType},
    paged_storage::PagedStorage,
    storage::Storage,
    utils::{BINCODE_CONFIG, load},
};
use super::error::ExecutorError;
use super::object_storage::ObjectStorage;
use super::schema::*;

use bincode::{Decode, Encode};

#[derive(Debug, Encode, Decode)]
pub struct TableManagerState {
    pub tables: HashMap<String, ObjectStorage>,
}

pub struct TableManager {
    pub state: TableManagerState,
    pub paged_Storage: PagedStorage,
}
