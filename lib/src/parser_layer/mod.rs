pub mod ast;
pub mod ast_helpers;
pub mod error;
pub mod fetch_types;

use lalrpop_util::lalrpop_mod;
lalrpop_mod!(pub query, "/parser_layer/query.rs");

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_parser_test() {
        let parser = query::QueryParser::new();

        let select = r#"
            SELECT user.age + 5 AS age_plus_five, user.name AS username
            FROM users
            WHERE MATCH user.role { UserRole::Admin => user.level > 5 };
        "#;

        match parser.parse(select) {
            Ok(ast::Query::Select {
                table: _,
                fields: _,
                condition: _,
            }) => {}
            _ => panic!("Cant parse select query"),
        }

        let fetch = r#"
            FETCH TYPES "something.dbuf";
        "#;

        match parser.parse(fetch) {
            Ok(ast::Query::FetchTypes(_)) => {}
            Err(e) => println!("{:?}", e),
            _ => panic!("Cant parse fetch query"),
        }

        let create = r#"
            CREATE TABLE user_table User;
        "#;

        match parser.parse(create) {
            Ok(ast::Query::CreateTable(_, _)) => {}
            _ => panic!("Cant parse create query"),
        }

        let drop = r#"
            DROP TABLE user_table;
        "#;

        match parser.parse(drop) {
            Ok(ast::Query::DropTable(_)) => {}
            _ => panic!("Cant parse drop query"),
        }

        let insert = r#"
            INSERT INTO user_table VALUES [User {"John", "Doe"}],  [User {"Jane", "Doe"}];
        "#;

        match parser.parse(insert) {
            Ok(ast::Query::InsertMessages {
                table: _,
                messages: _,
            }) => {}
            _ => panic!("Cant parse insert query"),
        }
    }

    #[test]
    fn fetch_types_test() {
        let file = std::fs::read_to_string("sample_dbuf/user.dbuf").unwrap();
        let parsed = fetch_types::parse_types(file).unwrap();
        //TODO check exact parsed value
    }
}
