pub mod error;
pub mod expression;
pub mod object_storage;
pub mod operator;
pub mod schema;
pub mod table_manager;

#[cfg(test)]
mod tests {
    use std::boxed::Box;

    use super::super::storage_layer::paged_storage::PagedStorage;
    use super::expression::{BinaryOperator, Expression, UnaryOperator};
    use super::object_storage::ObjectStorage;
    use super::schema::*;
    use super::table_manager::TableManager;

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
    fn expressions_test() {
        let first = Expression::BinaryOp {
            op: BinaryOperator::Add,
            left: Box::new(Expression::Literal(DBValue::UInt(1u32))),
            right: Box::new(Expression::Literal(DBValue::UInt(2u32))),
        };

        let empty_message = Message { fields: vec![] };

        assert_eq!(first.evaluate(&empty_message), DBValue::UInt(3u32));

        let a = Message {
            fields: vec![
                DBValue::UInt(10u32),
                DBValue::UInt(3u32),
                DBValue::Bool(true),
                DBValue::String("Hello world".to_owned()),
                DBValue::Message(Message {
                    fields: vec![DBValue::String("Goodbye".to_owned())],
                }),
            ],
        };

        let second = Expression::BinaryOp {
            op: BinaryOperator::GreaterThan,
            left: Box::new(Expression::ColumnRef(0usize)),
            right: Box::new(Expression::ColumnRef(1usize)),
        };
        assert_eq!(second.evaluate(&a), DBValue::Bool(true));

        let third = Expression::ColumnRef(3usize);
        assert_eq!(
            third.evaluate(&a),
            DBValue::String("Hello world".to_owned())
        );

        let fourth = Expression::BinaryOp {
            op: BinaryOperator::Equals,
            left: Box::new(Expression::ColumnRef(3usize)),
            right: Box::new(Expression::UnaryOp {
                op: UnaryOperator::MessageField(0usize),
                expr: Box::new(Expression::ColumnRef(4usize)),
            }),
        };
        assert_eq!(fourth.evaluate(&a), DBValue::Bool(false));

        let fifth = Expression::UnaryOp {
            op: UnaryOperator::Not,
            expr: Box::new(Expression::ColumnRef(2usize)),
        };
        assert_eq!(fifth.evaluate(&a), DBValue::Bool(false));

        let b = Message {
            fields: vec![DBValue::EnumValue(EnumValue {
                dependencies: vec![],
                choice: 0usize,
                values: vec![DBValue::String("First option".to_owned())],
            })],
        };

        let c = Message {
            fields: vec![DBValue::EnumValue(EnumValue {
                dependencies: vec![],
                choice: 1usize,
                values: vec![DBValue::UInt(3u32)],
            })],
        };

        let sixth = Expression::UnaryOp {
            op: UnaryOperator::EnumMatch(vec![
                Expression::ColumnRef(0usize),
                Expression::Literal(DBValue::String("Second option".to_owned())),
            ]),
            expr: Box::new(Expression::ColumnRef(0usize)),
        };

        assert_eq!(
            sixth.evaluate(&b),
            DBValue::String("First option".to_owned())
        );
        assert_eq!(
            sixth.evaluate(&c),
            DBValue::String("Second option".to_owned())
        );
    }

    #[test]
    fn enum_matching_test() {
        let enum_type = DBType::EnumType(EnumType {
            name: "My_enum".to_owned(),
            dependencies: vec![("a".to_owned(), DBType::UInt)],
            variants: vec![
                EnumVariantType {
                    name: "First".to_owned(),
                    content: vec![],
                },
                EnumVariantType {
                    name: "Second".to_owned(),
                    content: vec![("value".to_owned(), DBType::Int)],
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

    #[test]
    fn table_manager_test() {
        let path = "temp_path7";
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
            let paged_storage = PagedStorage::new(path, 4096usize, 3usize).unwrap();
            let mut table_manager = TableManager::new(paged_storage).unwrap();

            table_manager
                .create_table("First".to_owned(), message_type.clone())
                .unwrap();
            table_manager
                .insert_messages("First".to_owned(), messages.clone().into_iter())
                .unwrap();
        }

        {
            let paged_storage = PagedStorage::new(path, 4096usize, 3usize).unwrap();
            let mut table_manager = TableManager::new(paged_storage).unwrap();

            let retrieved_messages: Vec<Message> =
                table_manager.iter("First".to_owned()).unwrap().collect();
            assert_eq!(messages, retrieved_messages);

            table_manager.drop_table("First".to_owned()).unwrap();
        }

        {
            let paged_storage = PagedStorage::new(path, 4096usize, 3usize).unwrap();
            let table_manager = TableManager::new(paged_storage).unwrap();

            assert!(table_manager.iter("First".to_owned()).is_err());
            assert!(table_manager.iter("Unknown".to_owned()).is_err());
        }

        {
            let paged_storage = PagedStorage::new(path, 4096usize, 3usize).unwrap();
            let mut table_manager = TableManager::new(paged_storage).unwrap();

            table_manager
                .create_table("Second".to_owned(), message_type)
                .unwrap();
            let retrieved_messages: Vec<Message> =
                table_manager.iter("Second".to_owned()).unwrap().collect();
            assert_eq!(0usize, retrieved_messages.len());
        }

        utility::cleanup(path);
    }
}
