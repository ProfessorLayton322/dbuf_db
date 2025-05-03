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
    fn enum_matching_test() {
        let enum_type = DBType::EnumType(EnumType {
            name: "My_enum".to_owned(),
            dependencies: vec![
                ("a".to_owned(), DBType::UInt),
            ],
            variants: vec![
                EnumVariantType {
                    name: "First".to_owned(),
                    content: vec! [],
                },
                EnumVariantType {
                    name: "Second".to_owned(),
                    content: vec! [
                        ("value".to_owned(), DBType::Int)
                    ],
                },
            ],
        });

        let first = DBValue::EnumValue(EnumValue {
            dependencies: vec![DBValue::UInt(0u32)],
            choice: 0usize,
            values: vec![],
        });
        assert!(match_type_value(&enum_type, &first));

        let second = DBValue::EnumValue(EnumValue {
            dependencies: vec![DBValue::UInt(1u32)],
            choice: 1usize,
            values: vec![DBValue::Int(123i32)],
        });
        assert!(match_type_value(&enum_type, &second));

        let third = DBValue::EnumValue(EnumValue {
            dependencies: vec![DBValue::UInt(2u32)],
            choice: 2usize,
            values: vec![],
        });
        assert_eq!(false, match_type_value(&enum_type, &third));

        let fourth = DBValue::EnumValue(EnumValue {
            dependencies: vec![DBValue::UInt(2u32)],
            choice: 0usize,
            values: vec![DBValue::Bool(true)],
        });
        assert_eq!(false, match_type_value(&enum_type, &fourth));
    }

    #[test]
    fn object_storage_test() {
        let path = "temp_path4";
        utility::cleanup(path);

        let message_type = MessageType {
            name: "Something".to_owned(),
            columns: vec![
                Column {
                    column_name: "First".to_owned(),
                    column_type: DBType::UInt,
                    dependencies: vec![],
                },
                Column {
                    column_name: "Second".to_owned(),
                    column_type: DBType::Bool,
                    dependencies: vec![],
                },
                Column {
                    column_name: "Third".to_owned(),
                    column_type: DBType::String,
                    dependencies: vec![],
                },
            ],
        };
        let mut object_storage = ObjectStorage::new(message_type);

        let messages = vec![
            Message {
                fields: vec![
                    DBValue::UInt(15u32),
                    DBValue::Bool(true),
                    DBValue::String("hello".to_owned()),
                ],
            },
            Message {
                fields: vec![
                    DBValue::UInt(0u32),
                    DBValue::Bool(false),
                    DBValue::String("another".to_owned()),
                ],
            },
            Message {
                fields: vec![
                    DBValue::UInt(1337u32),
                    DBValue::Bool(true),
                    DBValue::String("something".to_owned()),
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
                        DBValue::UInt(5u32),
                        DBValue::Bool(false),
                        DBValue::String("does not matter".to_owned()),
                    ],
                },
                Message {
                    fields: vec![DBValue::Int(7i32)],
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
            name: "Something".to_owned(),
            columns: vec![Column {
                column_name: "Something".to_owned(),
                column_type: DBType::String,
                dependencies: vec![],
            }],
        };
        let mut object_storage = ObjectStorage::new(message_type);

        //the border between Real and Index in WrappedMessage is somewhere in this cycle
        let messages: Vec<Message> = (4900..5000usize)
            .map(|i| Message {
                fields: vec![DBValue::String(
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

    #[test]
    fn object_storage_another_big_test() {
        let path = "temp_path6";
        utility::cleanup(path);

        let message_type = MessageType {
            name: "Something".to_owned(),
            columns: vec![Column {
                column_name: "Something".to_owned(),
                column_type: DBType::String,
                dependencies: vec![],
            }],
        };
        let mut object_storage = ObjectStorage::new(message_type);

        //the border between Real and Index in WrappedMessage is somewhere in this cycle
        let messages: Vec<Message> = (0..4000usize)
            .map(|_| Message {
                fields: vec![DBValue::String(
                    std::iter::repeat('a').take(8).collect::<String>(),
                )],
            })
            .collect();

        {
            let mut paged_storage = PagedStorage::new(path, 8192usize, 3usize).unwrap();

            object_storage
                .insert_messages(&mut paged_storage, messages.clone().into_iter())
                .unwrap();

            paged_storage.flush().unwrap();
        }

        {
            let paged_storage = PagedStorage::new(path, 8192usize, 3usize).unwrap();

            let retrieved_messages: Vec<Message> = object_storage.iter(&paged_storage).collect();

            assert_eq!(messages, retrieved_messages);
        }

        utility::cleanup(path);
    }
}
