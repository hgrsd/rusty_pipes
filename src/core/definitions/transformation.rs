use serde::Deserialize;

/// A definition for an operation to be performed as part of the transformation pipeline.
#[derive(Deserialize, Debug, Clone)]
#[serde(tag = "type")]
pub enum Operation {
    /// The expected format of this predicate is
    /// "column_name operator literal" where operator is one of >, >=, <, <=, ==, !=, contains and !contains; and the literal is
    /// an integer, decimal, or string. E.g., "column_one > 100" or "column_two !contains foo".
    ///
    /// This operation has an arity of 1 (i.e., it requires a single dataframe to operate on).
    Filter { predicate: String },
    /// The expected format of the "on" clause is "left_column_name = right_column_name" where left_column_name
    /// and right_column_name refer to the names of the identifying columns in the left and right dataframes. E.g.,
    /// "identifier = identifier".
    ///
    /// This operation has an arity of 2 (i.e., it requires two dataframes to operate on).
    InnerJoin { on: String },
}

/// A definition of a single transformation pipeline.
#[derive(Deserialize, Debug, Clone)]
pub struct TransformationDefinition {
    /// The identifiers of the sources on which this transformation pipeline depends.
    pub sources: Vec<String>,
    /// The operations that this transformation pipeline performs. These will be executed in sequence.
    pub operations: Vec<Operation>,
}
