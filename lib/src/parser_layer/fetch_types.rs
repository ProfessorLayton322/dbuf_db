use dbuf_core::ast::parsed::*;
use dbuf_core::parser::parse;

use std::collections::HashMap;
use std::collections::HashSet;

use super::super::executor_layer::schema::*;
use super::error::*;

#[derive(Clone, Debug)]
pub enum FetchedType {
    MessageType(MessageType),
    EnumType(EnumType),
}

pub fn parse_types(data: String) -> Result<Vec<FetchedType>, ParsingError> {
    let mut ans: Vec<FetchedType> = vec![];

    let parsed = parse(&data).map_err(|_| ParsingError::TypesParseError)?;

    let mut type_cache = HashMap::<String, DBType>::new();
    type_cache.insert("Bool".to_owned(), DBType::Bool);
    type_cache.insert("Int".to_owned(), DBType::Int);
    type_cache.insert("Unsigned".to_owned(), DBType::UInt);
    type_cache.insert("Float".to_owned(), DBType::Double);
    type_cache.insert("String".to_owned(), DBType::String);

    for definition in parsed.iter() {
        let name = definition.name.clone();

        if type_cache.contains_key(&name) {
            return Err(ParsingError::DuplicateTypeName(name));
        }

        if let TypeDefinition::Message(constructor_body) = &definition.body {
            let mut message_type = MessageType {
                name: name.clone(),
                columns: vec![],
            };

            let mut column_indices = HashMap::<String, usize>::new();

            for (i, field) in definition
                .dependencies
                .iter()
                .chain(constructor_body.iter())
                .enumerate()
            {
                let field_name = field.name.clone();

                if column_indices.contains_key(&field_name) {
                    return Err(ParsingError::DuplicateField(field_name));
                }

                let node = &field.data.node;

                if let ExpressionNode::FunCall { fun, args } = node {
                    if let Some(db_type) = type_cache.get(fun) {
                        let mut dep_ind = Vec::<usize>::new();

                        for arg in args.iter() {
                            if let ExpressionNode::Variable { name } = &arg.node {
                                if let Some(index) = column_indices.get(name) {
                                    dep_ind.push(*index);
                                } else {
                                    return Err(ParsingError::FieldNotFound(name.clone()));
                                }
                            } else {
                                return Err(ParsingError::InvalidArgument);
                            }
                        }

                        message_type.columns.push(Column {
                            column_name: field_name.clone(),
                            column_type: db_type.clone(),
                            dependencies: dep_ind,
                        });
                    } else {
                        return Err(ParsingError::UnknownType(fun.clone()));
                    }
                } else {
                    return Err(ParsingError::CantDeduceFieldType(field_name));
                }

                column_indices.insert(field_name.clone(), i);
            }

            type_cache.insert(name, DBType::MessageType(message_type.clone()));

            ans.push(FetchedType::MessageType(message_type));
        } else if let TypeDefinition::Enum(enum_branches) = &definition.body {
            let mut enum_type = EnumType {
                name: name.clone(),
                variants: vec![],
            };

            let mut constructor_set = HashSet::<String>::new();

            for enum_branch in enum_branches.iter() {
                for constructor in enum_branch.constructors.iter() {
                    let constructor_name = constructor.name.clone();

                    if constructor_set.contains(&constructor_name) {
                        return Err(ParsingError::DuplicateVariantName(constructor_name));
                    }
                    constructor_set.insert(constructor_name.clone());

                    let mut fields = Vec::<(String, DBType)>::new();

                    for definition in constructor.data.iter() {
                        let field_name = definition.name.clone();

                        if let ExpressionNode::FunCall { fun, args: _ } = &definition.data.node {
                            if let Some(db_type) = type_cache.get(fun) {
                                fields.push((field_name, db_type.clone()));
                            } else {
                                return Err(ParsingError::UnknownType(fun.clone()));
                            }
                        } else {
                            return Err(ParsingError::CantDeduceFieldType(field_name));
                        }
                    }

                    enum_type.variants.push(EnumVariantType {
                        name: constructor_name,
                        content: fields,
                    });
                }
            }

            type_cache.insert(name, DBType::EnumType(enum_type.clone()));

            ans.push(FetchedType::EnumType(enum_type));
        }
    }

    Ok(ans)
}
