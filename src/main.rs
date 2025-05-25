use lib::executor_layer::{physical_plan::*, schema, table_manager::TableManager};
use lib::parser_layer::{ast::*, fetch_types::*, query::QueryParser};
use lib::planner_layer::{logical_plan::*, query_planner::QueryPlanner, raw_expression::*};
use lib::storage_layer::paged_storage::PagedStorage;

use std::io::{self, Stdin};

pub struct Executor {
    pub query_planner: QueryPlanner,
    pub query_parser: QueryParser,
}

impl Executor {
    pub fn new(query_planner: QueryPlanner, query_parser: QueryParser) -> Self {
        Self {
            query_planner,
            query_parser,
        }
    }

    fn read_query() -> Result<String, ()> {
        let mut query = String::new();
        let stdin: Stdin = io::stdin();

        loop {
            let mut line = String::new();

            if stdin.read_line(&mut line).is_err() {
                println!("Error: Failed to read from stdin");
                return Err(());
            }

            if let Some(pos) = line.find(';') {
                if pos != line.trim_end_matches(&['\n', '\r'][..]).len() - 1 {
                    println!("Error: Semicolon must only appear at the end of the query");
                    return Err(());
                }

                if query.contains(';') {
                    println!("Error: Multiple semicolons detected");
                    return Err(());
                }

                query.push_str(&line);
                return Ok(query);
            } else {
                query.push_str(&line);
            }
        }
    }

    //TODO better error handling
    fn execute_query(&mut self, query_string: String) -> () {
        let result = self.query_parser.parse(&query_string);

        if let Err(e) = result {
            println!("Parser error:\n{:?}", e);
            return;
        }

        let query = result.unwrap();
        match query {
            Query::FetchTypes(path) => {
                let result = std::fs::read_to_string(path);
                if let Err(e) = result {
                    println!("Can't read file:\n{:?}", e);
                    return;
                }
                let file = result.unwrap();

                match parse_types(file) {
                    Err(e) => println!("Dbuf parser error:\n{:?}", e),
                    Ok(types) => {
                        for fetched in types.iter() {
                            match fetched {
                                FetchedType::MessageType(message_type) => {
                                    let _ = self.query_planner.add_message_type(
                                        message_type.name.clone(),
                                        message_type.clone(),
                                    );
                                }
                                FetchedType::EnumType(enum_type) => {
                                    let _ = self
                                        .query_planner
                                        .add_enum_type(enum_type.name.clone(), enum_type.clone());
                                }
                            }
                        }
                    }
                }
            }
            Query::CreateTable(table_name, type_name) => {
                if let Ok(message_type) = self.query_planner.get_message_type(&type_name) {
                    if let Err(e) = self
                        .query_planner
                        .table_manager
                        .create_table(table_name, message_type.clone())
                    {
                        println!("Failed to create table:\n{:?}", e);
                    }
                } else {
                    println!("Could not find type {:?}", type_name);
                }
            }
            Query::DropTable(table_name) => {
                if let Err(e) = self
                    .query_planner
                    .table_manager
                    .drop_table(table_name.clone())
                {
                    println!("Failed to drop table:\n{:?}", e);
                }
            }
            Query::InsertMessages { table, messages } => {
                let mut converted_messages = Vec::<schema::Message>::new();

                for message in messages.iter() {
                    let value = Value::Message(message.clone());
                    let result = self.query_planner.from_parsed_value(&value);
                    if let Err(ref e) = result {
                        println!(
                            "Ill formed message or could not convert to desired type:\n{:?}",
                            e
                        );
                        return;
                    }
                    let converted = result.unwrap();

                    if let schema::DBValue::Message(message) = converted {
                        converted_messages.push(message.clone());
                    } else {
                        panic!("THIS SHOULD NEVER PANIC: CONVERSION ERROR");
                    }
                }

                if let Err(e) = self
                    .query_planner
                    .table_manager
                    .insert_messages(table.clone(), converted_messages.into_iter())
                {
                    println!("Insertion failed:\n{:?}", e);
                }
            }
            Query::Select {
                table,
                fields,
                condition,
            } => {
                let table_scan = RawPlan::Scan { table_name: table };

                let mut raw_expressions = Vec::<(String, RawExpression)>::new();
                for (parsed_expression, alias) in fields.iter() {
                    let result = self.query_planner.from_parsed_expression(parsed_expression);
                    if let Err(e) = result {
                        println!("Ill-formed expression:\n{:?}", e);
                        return;
                    }
                    raw_expressions.push((alias.clone(), result.unwrap()));
                }

                let raw_plan = match condition {
                    Some(filter_expression) => {
                        let result = self
                            .query_planner
                            .from_parsed_expression(&filter_expression);
                        if let Err(e) = result {
                            println!("Ill-formed expression:\n{:?}", e);
                            return;
                        }

                        let filter = RawPlan::Filter {
                            raw_expression: result.unwrap(),
                            source: Box::new(table_scan),
                        };

                        RawPlan::Projection {
                            raw_expressions: raw_expressions,
                            source: Box::new(filter),
                        }
                    }
                    None => RawPlan::Projection {
                        raw_expressions: raw_expressions,
                        source: Box::new(table_scan),
                    },
                };

                let result = self.query_planner.build_logical_plan(&raw_plan);
                if let Err(ref e) = result {
                    println!("Error building a logical plan:\n{:?}", e);
                    return;
                }
                let logical_plan = result.unwrap();

                let mut physical_plan = PhysicalPlan {
                    root: self.query_planner.build_physical_plan(&logical_plan),
                };

                if let Err(e) = physical_plan.open() {
                    println!("Error while preparing plan for execution: {:?}", e);
                    return;
                }

                let answer: Vec<lib::executor_layer::schema::Message> = physical_plan.collect();
                for message in answer.iter() {
                    println!("{:#?}", message);
                }
            }
        }
    }

    pub fn run(&mut self) {
        loop {
            let read = Self::read_query();
            if read.is_err() {
                continue;
            }
            let query = read.unwrap();

            self.execute_query(query);
        }
    }
}

fn main() {
    let path = "dbuf_db_storage".to_owned();
    let paged_storage = PagedStorage::new(path, 4096usize, 10usize).unwrap();
    let table_manager = TableManager::new(paged_storage).unwrap();
    let query_planner = QueryPlanner::new(table_manager).unwrap();

    let query_parser = QueryParser::new();

    let mut executor = Executor::new(query_planner, query_parser);
    executor.run();
}
