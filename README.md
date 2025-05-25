# Dependobuf database

This project is a database that stores data in [dependobuf](https://sphericalpotatoinvacuum.github.io/DependoBuf/) format

## How to run

You can run this project with `cargo run` from the root of this repository. Doing so will open a database console in which you can enter and run commands

## Commands

Lets assume we are working with a simple `dbuf` file like:
```
message User {
    name String;
    surname String;
    age Int;
    year_of_birth Int;
}
```

First you need to fetch the type you want to use to store your data with `FETCH TYPES` command:

```sql

FETCH TYPES "path/to/your/file.dbuf";
```

It will parse and proccess through all dependobuf declarations in this file so they can be later used for other queries

After that you can create tables with `CREATE TABLE` command:

```sql
CREATE TABLE user_table User;
```

To delete table use `DROP TABLE` command:

```sql
DROP TABLE user_table;
```

To insert values into tables you can use `INSERT INTO` command:

```sql
INSERT INTO user_table VALUES
    [User {"John", "Doe", 26, 1999}],
    [User {"Jane", "Doe", 18, 2007}]
;
```

After that you can use `SELECT` with arbitrary expressions to access values from tables:

```sql
SELECT
    name AS name,
    surname AS surname
FROM user_table
WHERE age > 20;
```

## Enums

Dependobuf allows to declare enum types. Lets assume we are now working with `sample_dbuf/user.dbuf` file:

```
enum Status {
  Admin
  User
}

message User {
  name String;
  surname String;
  age Int;
  year_of_birth Int;
  status Status;
}
```

To insert a enum literal into the table you should run a query like:

```sql
INERT INTO
user_table
VALUES
[User {"John", "Doe", 25, 2000, [Status::Admin {}] }],
[User {"Jane", "Doe", 20, 2005, [Status::User {}] }];
```

## Match statements

You can use `MATCH` keyword for match statements. They work pretty similar to Rust enums. Lets assume that we have a more complex enum type as one of the fields, like

```
enum ComplexEnum {
    First {
        a Int;
        b Int;
    }
    Second {
        c Int;
    }
    Third
}

message EnumWrapper {
    enum ComplexEnum;
}
```

and you already have a table with this data type called `enum_table`

Then you can insert values using the following query:

```sql
INSERT INTO enum_table VALUES
    [EnumWrapper {[ ComplexEnum::First { 100, -200 }] }],
    [EnumWrapper { [ComplexEnum::Second { 50 }] }],
    [EnumWrapper { [ComplexEnum::Third {} ] }];
```

Then you can use `MATCH` to match enums in `SELECT` statement:

```sql
SELECT MATCH field {
    ComplexEnum::First => a + b,
    ComplexEnum::Second => c,
    ComplexEnum::Third => 3
} AS number FROM enum_table;
```

## Dependencies

Dependobuf types may also have dependencies. Message dependencies are stored just like regular columns in the table. If one column is dependent on the other column, then the dependency column needs to be selected in order to select any expression that uses the dependent column. Otherwise the dependency would be dropped - it is a situation we want to avoid, so such `SELECT` queries are considered to be ill-formed.
