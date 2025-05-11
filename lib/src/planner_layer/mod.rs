pub mod error;
pub mod logical_plan;
pub mod query_planner;
pub mod raw_expression;

#[cfg(test)]
mod tests {
    use super::super::{
        executor_layer::{expression::*, schema::*, table_manager::TableManager},
        storage_layer::paged_storage::PagedStorage,
    };
    use super::{logical_plan::*, query_planner::QueryPlanner, raw_expression::*};

    pub mod utility {
        use super::*;
        use std::process::Command;

        pub fn cleanup(path: &str) {
            Command::new("sh")
                .arg("-c")
                .arg(format!("rm -rf {}", path))
                .output()
                .unwrap();
        }

        #[cfg(test)]
        pub fn create_query_planner(path: &str) -> QueryPlanner {
            let paged_storage = PagedStorage::new(path, 4096usize, 3usize).unwrap();
            let table_manager = TableManager::new(paged_storage).unwrap();
            QueryPlanner::new(table_manager).unwrap()
        }
    }

    #[test]
    fn deduce_expression_test() {
        let path = "temp_path9";
        utility::cleanup(path);

        let mut query_planner = utility::create_query_planner(path);

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

        query_planner
            .add_message_type("Something".to_owned(), message_type.clone())
            .unwrap();

        let filter_expression = Expression::BinaryOp {
            op: BinaryOperator::Or,
            left: Box::new(Expression::ColumnRef(1usize)),
            right: Box::new(Expression::BinaryOp {
                op: BinaryOperator::GreaterThan,
                left: Box::new(Expression::ColumnRef(0usize)),
                right: Box::new(Expression::Literal(DBValue::UInt(100u32))),
            }),
        };

        assert_eq!(
            query_planner
                .deduce_expression_type(&filter_expression, &message_type)
                .unwrap(),
            DBType::Bool
        );

        //TODO enums and messages tests

        utility::cleanup(path);
    }

    #[test]
    fn plan_conversion_test() {
        let path = "temp_path10";
        utility::cleanup(path);

        let mut query_planner = utility::create_query_planner(path);

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
                type_name: None,
                fields: vec![
                    DBValue::UInt(15u32),
                    DBValue::Bool(true),
                    DBValue::String("hello".to_owned()),
                ],
            },
            Message {
                type_name: None,
                fields: vec![
                    DBValue::UInt(0u32),
                    DBValue::Bool(false),
                    DBValue::String("another".to_owned()),
                ],
            },
            Message {
                type_name: None,
                fields: vec![
                    DBValue::UInt(1337u32),
                    DBValue::Bool(false),
                    DBValue::String("something".to_owned()),
                ],
            },
            Message {
                type_name: None,
                fields: vec![
                    DBValue::UInt(250u32),
                    DBValue::Bool(false),
                    DBValue::String("string".to_owned()),
                ],
            },
        ];

        let expected = vec![
            Message {
                type_name: None,
                fields: vec![DBValue::String("hello".to_owned()), DBValue::UInt(17u32)],
            },
            Message {
                type_name: None,
                fields: vec![
                    DBValue::String("something".to_owned()),
                    DBValue::UInt(1339u32),
                ],
            },
            Message {
                type_name: None,
                fields: vec![DBValue::String("string".to_owned()), DBValue::UInt(252u32)],
            },
        ];

        query_planner
            .table_manager
            .create_table("First".to_owned(), message_type)
            .unwrap();
        query_planner
            .table_manager
            .insert_messages("First".to_owned(), messages.clone().into_iter())
            .unwrap();

        let table_scan = RawPlan::Scan {
            table_name: "First".to_owned(),
        };

        let filter = RawPlan::Filter {
            raw_expression: RawExpression::BinaryOp {
                op: BinaryOperator::Or,
                left: Box::new(RawExpression::ColumnRef("Second".to_owned())),
                right: Box::new(RawExpression::BinaryOp {
                    op: BinaryOperator::GreaterThan,
                    left: Box::new(RawExpression::ColumnRef("First".to_owned())),
                    right: Box::new(RawExpression::Literal(DBValue::UInt(100u32))),
                }),
            },
            source: Box::new(table_scan),
        };

        let projection = RawPlan::Projection {
            raw_expressions: vec![
                (
                    "Column_ref".to_owned(),
                    RawExpression::ColumnRef("Third".to_owned()),
                ),
                (
                    "Sum".to_owned(),
                    RawExpression::BinaryOp {
                        op: BinaryOperator::Add,
                        left: Box::new(RawExpression::Literal(DBValue::UInt(2u32))),
                        right: Box::new(RawExpression::ColumnRef("First".to_owned())),
                    },
                ),
            ],
            source: Box::new(filter),
        };

        let logical_plan = query_planner.build_logical_plan(&projection).unwrap();

        let expected_type = MessageType {
            name: "".to_owned(),
            columns: vec![
                Column {
                    column_name: "Column_ref".to_owned(),
                    column_type: DBType::String,
                    dependencies: vec![],
                },
                Column {
                    column_name: "Sum".to_owned(),
                    column_type: DBType::UInt,
                    dependencies: vec![],
                },
            ],
        };

        assert_eq!(logical_plan.get_message_type(), &expected_type);

        let mut physical_plan = query_planner.build_physical_plan(&logical_plan);
        physical_plan.open().unwrap();

        let retreived_messages: Vec<Message> = physical_plan.collect();

        assert_eq!(expected, retreived_messages);

        utility::cleanup(path);
    }
}
