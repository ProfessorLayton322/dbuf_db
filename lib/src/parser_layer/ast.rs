#[derive(Debug, Clone)]
pub enum Query {
    FetchTypes(String),
    CreateTable(String, String),
    DropTable(String),
    InsertMessages {
        table: String,
        messages: Vec<Message>,
    },
    Select {
        table: String,
        fields: Vec<(Expression, String)>,
        condition: Option<Expression>,
    },
}

#[derive(Debug, Clone)]
pub enum Value {
    Int(i32),
    Double(f32),
    String(String),
    Bool(bool),
    Message(Message),
    Enum(Enum),
}

#[derive(Debug, Clone)]
pub struct Message {
    pub type_name: String,
    pub values: Vec<Value>,
}

#[derive(Debug, Clone)]
pub struct Enum {
    pub type_name: String,
    pub variant_name: String,
    pub values: Vec<Value>,
}

#[derive(Debug, Clone)]
pub enum Expression {
    Literal(Value),
    ColumnRef(String),
    BinaryOp {
        op: BinaryOperator,
        left: Box<Expression>,
        right: Box<Expression>,
    },
    UnaryOp {
        op: UnaryOperator,
        expr: Box<Expression>,
    },
}

#[derive(Debug, Clone, Copy)]
pub enum BinaryOperator {
    Add,         // +
    Subtract,    // -
    Multiply,    // *
    Divide,      // /
    Equals,      // =
    NotEquals,   // !=
    LessThan,    // <
    GreaterThan, // >
    And,         // &
    Or,          // |
}

#[derive(Debug, Clone)]
pub enum UnaryOperator {
    Negate,                               // unary minus
    Not,                                  // !
    MessageField(String),                 // foo.bar
    EnumMatch(Vec<(String, Expression)>), // match EnumType.Foo => expression
}
