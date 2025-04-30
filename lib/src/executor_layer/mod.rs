pub mod error;
pub mod object_storage;
pub mod schema;

#[cfg(test)]
mod tests {
    use super::super::storage_layer::paged_storage::PagedStorage;
    use super::object_storage::ObjectStorage;
    use super::schema::*;

    pub mod utility {
        use std::process::Command;

        #[cfg(test)]
        pub fn cleanup(path: &str) {
            Command::new("sh")
                .arg("-c")
                .arg(format!("rm -rf {}", path))
                .output()
                .unwrap();
        }
    }

    #[test]
    fn object_storage_test() {
        let path = "temp_path4";
        utility::cleanup(path);

        let message_type = MessageType {
            columns: vec![
                Column {
                    column_name: "First".to_owned(),
                    column_type: ColumnType::UInt,
                    dependencies: vec![],
                },
                Column {
                    column_name: "Second".to_owned(),
                    column_type: ColumnType::Bool,
                    dependencies: vec![],
                },
                Column {
                    column_name: "Third".to_owned(),
                    column_type: ColumnType::String,
                    dependencies: vec![],
                },
            ],
        };
        let mut object_storage = ObjectStorage::new(message_type);

        let messages = vec![
            Message {
                fields: vec![
                    Field::UInt(15u32),
                    Field::Bool(true),
                    Field::String("hello".to_owned()),
                ],
            },
            Message {
                fields: vec![
                    Field::UInt(0u32),
                    Field::Bool(false),
                    Field::String("another".to_owned()),
                ],
            },
            Message {
                fields: vec![
                    Field::UInt(1337u32),
                    Field::Bool(true),
                    Field::String("something".to_owned()),
                ],
            },
        ];

        {
            let mut paged_storage = PagedStorage::new(path, 4096usize, 3usize).unwrap();

            object_storage
                .insert_messages(&mut paged_storage, messages.clone().into_iter())
                .unwrap();

            paged_storage.flush().unwrap();
        }

        {
            let mut paged_storage = PagedStorage::new(path, 4096usize, 3usize).unwrap();

            let retrieved_messages: Vec<Message> = object_storage.iter(&paged_storage).collect();

            assert_eq!(messages, retrieved_messages);

            object_storage.drop_items(&mut paged_storage).unwrap();
        }

        {
            let mut paged_storage = PagedStorage::new(path, 4096usize, 3usize).unwrap();

            let retrieved_messages: Vec<Message> = object_storage.iter(&paged_storage).collect();

            assert_eq!(Vec::<Message>::new(), retrieved_messages);

            let wrong_messages = vec![
                Message {
                    fields: vec![
                        Field::UInt(5u32),
                        Field::Bool(false),
                        Field::String("does not matter".to_owned()),
                    ],
                },
                Message {
                    fields: vec![Field::Int(7i32)],
                },
            ];

            let result =
                object_storage.insert_messages(&mut paged_storage, wrong_messages.into_iter());
            assert!(result.is_err());
        }

        utility::cleanup(path);
    }

    #[test]
    fn object_storage_big_test() {
        let path = "temp_path5";
        utility::cleanup(path);

        let message_type = MessageType {
            columns: vec![Column {
                column_name: "Something".to_owned(),
                column_type: ColumnType::String,
                dependencies: vec![],
            }],
        };
        let mut object_storage = ObjectStorage::new(message_type);

        //the border between Real and Index in WrappedMessage is somewhere in this cycle
        let messages: Vec<Message> = (4900..5000usize)
            .map(|i| Message {
                fields: vec![Field::String(
                    std::iter::repeat('a').take(i).collect::<String>(),
                )],
            })
            .collect();

        {
            let mut paged_storage = PagedStorage::new(path, 4096usize, 3usize).unwrap();

            object_storage
                .insert_messages(&mut paged_storage, messages.clone().into_iter())
                .unwrap();

            paged_storage.flush().unwrap();
        }

        {
            let paged_storage = PagedStorage::new(path, 4096usize, 3usize).unwrap();

            let retrieved_messages: Vec<Message> = object_storage.iter(&paged_storage).collect();

            assert_eq!(messages, retrieved_messages);
        }

        utility::cleanup(path);
    }
}
