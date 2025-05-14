pub mod ast;
pub mod ast_helpers;

use lalrpop_util::lalrpop_mod;
lalrpop_mod!(pub query, "/parser_layer/query.rs");

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn placeholder_test() {
        let parser = query::QueryParser::new();
        let input = r#"
            SELECT user.age + 5 AS age_plus_five, user.name AS username
            FROM users
            WHERE MATCH user.role { UserRole::Admin => user.level > 5 }
        "#;

        match parser.parse(input) {
            Ok(ast) => println!("{:#?}", ast),
            Err(e) => eprintln!("Parse error: {:?}", e),
        }

        assert_eq!(2 + 2, 5);
    }
}
