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
    use super::query_planner::QueryPlanner;

    pub mod utility {
        use super::*;
        use std::process::Command;

        #[cfg(test)]
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
}
