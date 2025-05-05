use std::vec::Vec;

use super::super::storage_layer::{
    page::{PageId, PageType},
    paged_storage::PagedStorage,
    storage::Storage,
    utils::{BINCODE_CONFIG, load},
};
use super::error::ExecutorError;
use super::schema::*;

use bincode::{Decode, Encode};

// We can not store mutable reference to paged storage in every ObjectStorage since there will be
// multiple of those, one for each table
#[derive(Debug, Encode, Decode)]
pub struct ObjectStorage {
    pub schema: MessageType,
    pages: Vec<PageId>,
    //these pages will need to be freed if the table is dropped
    overflow_pages: Vec<PageId>,
}

// We store message in it's real form as long as it's encoding fits into page size
// If it does not fit into page size then we simply save it into it's own marble id and store
// Index(page_id) on our page
// This way we guarantee we can fit each item into a page
#[derive(Debug, Encode, Decode)]
pub enum WrappedMessage {
    Real(Message),
    Index(PageId),
}

#[derive(Debug, Clone, Copy)]
pub struct MessageIterator<'a, 'b> {
    object_storage: &'a ObjectStorage,
    //we need to acces page ref directly here to avoid copying it's content every read
    paged_storage: &'b PagedStorage,
    page_index: usize,
    page_offset: usize,
    page_obj_count: usize,
}

impl<'a, 'b> Iterator for MessageIterator<'a, 'b> {
    type Item = Message;

    fn next(&mut self) -> Option<Self::Item> {
        if self.page_index >= self.object_storage.pages.len() {
            return None;
        }

        let page_id = self.object_storage.pages[self.page_index];
        let page = self.paged_storage.buffer_pool().get_page(page_id).unwrap();

        let encoded_segment = &page.0.data[self.page_offset..];
        let (message, len) =
            ObjectStorage::decode_and_unwrap(self.paged_storage.storage(), encoded_segment)
                .unwrap();

        self.page_offset += len;
        self.page_obj_count += 1usize;
        if self.page_obj_count == page.0.header.obj_count {
            self.page_offset = 0usize;
            self.page_obj_count = 0usize;
            self.page_index += 1;
        }

        Some(message)
    }
}

impl ObjectStorage {
    pub fn new(schema: MessageType) -> Self {
        Self {
            schema,
            pages: vec![],
            overflow_pages: vec![],
        }
    }
    //this is kinda weird but it has to be this way :/
    pub fn wrap_and_encode(
        &mut self,
        message: Message,
        storage: &mut Storage,
    ) -> Result<Vec<u8>, ExecutorError> {
        let wrapped = WrappedMessage::Real(message);
        let encoded = bincode::encode_to_vec(&wrapped, BINCODE_CONFIG)?;
        if encoded.len() <= storage.page_size() {
            return Ok(encoded);
        }

        let page_id = storage.allocate_id()?;
        self.overflow_pages.push(page_id);
        storage.marble().write_batch([(page_id, Some(&encoded))])?;

        let index = WrappedMessage::Index(page_id);
        let encoded = bincode::encode_to_vec(&index, BINCODE_CONFIG)?;
        Ok(encoded)
    }

    pub fn decode_and_unwrap(
        storage: &Storage,
        encoded: &[u8],
    ) -> Result<(Message, usize), ExecutorError> {
        let (wrapped, read): (WrappedMessage, usize) =
            bincode::decode_from_slice(encoded, BINCODE_CONFIG)?;

        match wrapped {
            WrappedMessage::Real(message) => Ok((message, read)),
            WrappedMessage::Index(id) => {
                let decoded: WrappedMessage = load(storage.marble(), id)?.unwrap();
                match decoded {
                    WrappedMessage::Index(_) => panic!("Incorrect overflow decoding"),
                    WrappedMessage::Real(message) => Ok((message, read)),
                }
            }
        }
    }

    fn add_page(&mut self, paged_storage: &mut PagedStorage) -> Result<(), ExecutorError> {
        self.pages
            .push(paged_storage.allocate_page(PageType::TableData)?);
        Ok(())
    }

    //use only when pages are non empty
    fn try_push(
        &mut self,
        paged_storage: &mut PagedStorage,
        encoded: &[u8],
    ) -> Result<(), ExecutorError> {
        paged_storage.append_data(*self.pages.last().unwrap(), encoded)?;
        Ok(())
    }

    pub fn insert_messages<T: Iterator<Item = Message>>(
        &mut self,
        paged_storage: &mut PagedStorage,
        messages: T,
    ) -> Result<(), ExecutorError> {
        if self.pages.len() == 0usize {
            self.add_page(paged_storage)?;
        }

        for message in messages {
            if !self.schema.match_message(&message) {
                return Err(ExecutorError::MessageTypeMismatch);
            }

            let encoded = self.wrap_and_encode(message.clone(), paged_storage.storage_mut())?;
            if let Err(_) = self.try_push(paged_storage, &encoded) {
                self.add_page(paged_storage)?;
                //If this panics set bigger page_size
                self.try_push(paged_storage, &encoded).unwrap();
            }

            paged_storage.bump_obj_count(*self.pages.last().unwrap())?;
        }

        Ok(())
    }

    pub fn drop_items(&mut self, paged_storage: &mut PagedStorage) -> Result<(), ExecutorError> {
        for page in self.pages.iter() {
            paged_storage.delete_page(*page)?;
        }

        self.pages.clear();

        for page in self.overflow_pages.iter() {
            paged_storage.storage_mut().free_id(*page)?;
        }

        self.overflow_pages.clear();

        Ok(())
    }

    pub fn iter<'a, 'b>(&'a self, paged_storage: &'b PagedStorage) -> MessageIterator<'a, 'b> {
        MessageIterator {
            object_storage: &self,
            paged_storage,
            page_index: 0usize,
            page_offset: 0usize,
            page_obj_count: 0usize,
        }
    }
}
