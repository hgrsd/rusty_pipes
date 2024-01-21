use crate::core::dataframe::{ColumnValue, Dataframe};
use crate::core::error::RustyPipesError;
use crate::core::result::RustyPipesResult;
use crate::dataframe::Row;
use std::collections::HashMap;

use super::Transformation;

fn extract_identifier(from: &ColumnValue) -> Option<String> {
    match from {
        ColumnValue::String(s) => Some(s.clone()),
        ColumnValue::Integer(i) => Some(i.to_string()),
        _ => None,
    }
}

fn group_rows<'b>(key: &str, df: &'b Dataframe) -> HashMap<String, Vec<&'b Row>> {
    let grouped = df.iter().filter_map(|row| {
        row.get(key)
            .and_then(extract_identifier)
            .map(|identifier| (identifier, vec![row]))
    });

    let mut result: HashMap<String, Vec<&Row>> = HashMap::new();
    for (identifier, rows) in grouped {
        if let Some(existing_rows) = result.get_mut(&identifier) {
            existing_rows.extend(rows);
        } else {
            result.insert(identifier, rows);
        }
    }

    result
}

/// Inner Join two data frames. This operation has an arity of two: it requires two dataframes to be provided as its
/// inputs.
pub struct InnerJoin<'a> {
    left_key: &'a str,
    right_key: &'a str,
}

impl<'a> InnerJoin<'a> {
    /// Construct a new InnerJoin from the given join clause.
    /// The expected format of this clause is "left_column_name = right_column_name" where left_column_name
    /// and right_column_name refer to the names of the identifying columns in the left and right dataframes.
    pub fn new(join_on: &'a str) -> RustyPipesResult<Self> {
        let (left_key, right_key) =
            join_on
                .split_once('=')
                .ok_or(RustyPipesError::TransformationError(format!(
                    "Unable to parse join clause {}",
                    join_on
                )))?;

        Ok(InnerJoin {
            left_key: left_key.trim(),
            right_key: right_key.trim(),
        })
    }
}

impl Transformation for InnerJoin<'_> {
    fn transform(&self, dfs: &Vec<&Dataframe>) -> RustyPipesResult<Vec<Dataframe>> {
        let right_rows_by_key = group_rows(self.right_key, dfs[1]);
        let joined = dfs[0]
            .iter()
            .flat_map(|left_row| {
                left_row
                    .get(self.left_key)
                    .and_then(extract_identifier)
                    .and_then(|identifier| right_rows_by_key.get(&identifier))
                    .map_or(vec![], |matching_rows| {
                        matching_rows
                            .iter()
                            .map(|matching_row| {
                                left_row
                                    .iter()
                                    .chain(matching_row.iter())
                                    .map(|(key, value)| (key.clone(), value.clone()))
                                    .collect::<Row>()
                            })
                            .collect::<Vec<_>>()
                    })
            })
            .collect();
        Ok(vec![joined])
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn no_matching_ids() {
        let dfs = vec![
            vec![
                HashMap::from([
                    (String::from("id"), ColumnValue::String(String::from("id1"))),
                    (String::from("foo"), ColumnValue::Integer(0)),
                ]),
                HashMap::from([
                    (String::from("id"), ColumnValue::String(String::from("id2"))),
                    (String::from("foo"), ColumnValue::Integer(1)),
                ]),
                HashMap::from([
                    (String::from("id"), ColumnValue::String(String::from("id3"))),
                    (String::from("foo"), ColumnValue::Integer(2)),
                ]),
            ],
            vec![
                HashMap::from([
                    (String::from("id"), ColumnValue::String(String::from("id4"))),
                    (String::from("bar"), ColumnValue::Integer(3)),
                ]),
                HashMap::from([
                    (String::from("id"), ColumnValue::String(String::from("id5"))),
                    (String::from("bar"), ColumnValue::Integer(4)),
                ]),
                HashMap::from([
                    (String::from("id"), ColumnValue::String(String::from("id6"))),
                    (String::from("bar"), ColumnValue::Integer(5)),
                ]),
            ],
        ];

        let op = InnerJoin::new("id = id").unwrap();

        let df_refs = dfs.iter().collect();

        let result = op.transform(&df_refs);
        assert_eq!(result.unwrap()[0], vec![])
    }

    #[test]
    fn no_matching_column_names() {
        let dfs = vec![
            vec![HashMap::from([
                (String::from("id"), ColumnValue::String(String::from("id1"))),
                (String::from("foo"), ColumnValue::Integer(0)),
            ])],
            vec![HashMap::from([
                (String::from("id"), ColumnValue::String(String::from("id4"))),
                (String::from("bar"), ColumnValue::Integer(3)),
            ])],
        ];

        let op = InnerJoin::new("non_existing = non_existing").unwrap();
        let df_refs = dfs.iter().collect();

        let result = op.transform(&df_refs);
        assert_eq!(result.unwrap()[0], vec![])
    }

    #[test]
    fn matching_rows() {
        let dfs = vec![
            vec![
                HashMap::from([
                    (String::from("id"), ColumnValue::String(String::from("id1"))),
                    (String::from("foo"), ColumnValue::Integer(0)),
                ]),
                HashMap::from([
                    (String::from("id"), ColumnValue::String(String::from("id2"))),
                    (String::from("foo"), ColumnValue::Integer(1)),
                ]),
            ],
            vec![
                HashMap::from([
                    (String::from("id"), ColumnValue::String(String::from("id1"))),
                    (String::from("bar"), ColumnValue::Integer(3)),
                ]),
                HashMap::from([
                    (String::from("id"), ColumnValue::String(String::from("id2"))),
                    (String::from("bar"), ColumnValue::Integer(4)),
                ]),
            ],
        ];

        let op = InnerJoin::new("id = id").unwrap();

        let df_refs = dfs.iter().collect();

        let result = op.transform(&df_refs);
        assert_eq!(
            result.unwrap()[0],
            vec![
                HashMap::from([
                    (String::from("id"), ColumnValue::String(String::from("id1"))),
                    (String::from("foo"), ColumnValue::Integer(0)),
                    (String::from("bar"), ColumnValue::Integer(3)),
                ]),
                HashMap::from([
                    (String::from("id"), ColumnValue::String(String::from("id2"))),
                    (String::from("foo"), ColumnValue::Integer(1)),
                    (String::from("bar"), ColumnValue::Integer(4)),
                ])
            ]
        )
    }

    #[test]
    fn multiple_matching_rows_right_multiplex() {
        let dfs = vec![
            vec![HashMap::from([
                (String::from("id"), ColumnValue::String(String::from("id1"))),
                (String::from("foo"), ColumnValue::Integer(0)),
            ])],
            vec![
                HashMap::from([
                    (String::from("id"), ColumnValue::String(String::from("id1"))),
                    (String::from("bar"), ColumnValue::Integer(3)),
                ]),
                HashMap::from([
                    (String::from("id"), ColumnValue::String(String::from("id1"))),
                    (String::from("bar"), ColumnValue::Integer(4)),
                ]),
            ],
        ];

        let op = InnerJoin::new("id = id").unwrap();

        let df_refs = dfs.iter().collect();

        let result = op.transform(&df_refs);
        assert_eq!(
            result.unwrap()[0],
            vec![
                HashMap::from([
                    (String::from("id"), ColumnValue::String(String::from("id1"))),
                    (String::from("foo"), ColumnValue::Integer(0)),
                    (String::from("bar"), ColumnValue::Integer(3)),
                ]),
                HashMap::from([
                    (String::from("id"), ColumnValue::String(String::from("id1"))),
                    (String::from("foo"), ColumnValue::Integer(0)),
                    (String::from("bar"), ColumnValue::Integer(4)),
                ])
            ]
        )
    }

    #[test]
    fn multiple_matching_rows_left_multiplex() {
        let dfs = vec![
            vec![
                HashMap::from([
                    (String::from("id"), ColumnValue::String(String::from("id1"))),
                    (String::from("foo"), ColumnValue::Integer(0)),
                ]),
                HashMap::from([
                    (String::from("id"), ColumnValue::String(String::from("id1"))),
                    (String::from("foo"), ColumnValue::Integer(1)),
                ]),
            ],
            vec![HashMap::from([
                (String::from("id"), ColumnValue::String(String::from("id1"))),
                (String::from("bar"), ColumnValue::Integer(3)),
            ])],
        ];

        let op = InnerJoin::new("id = id").unwrap();

        let df_refs = dfs.iter().collect();

        let result = op.transform(&df_refs);
        assert_eq!(
            result.unwrap()[0],
            vec![
                HashMap::from([
                    (String::from("id"), ColumnValue::String(String::from("id1"))),
                    (String::from("foo"), ColumnValue::Integer(0)),
                    (String::from("bar"), ColumnValue::Integer(3)),
                ]),
                HashMap::from([
                    (String::from("id"), ColumnValue::String(String::from("id1"))),
                    (String::from("foo"), ColumnValue::Integer(1)),
                    (String::from("bar"), ColumnValue::Integer(3)),
                ])
            ]
        )
    }

    #[test]
    fn joins_on_integers() {
        let dfs = vec![
            vec![HashMap::from([
                (String::from("id"), ColumnValue::Integer(1)),
                (String::from("foo"), ColumnValue::Integer(0)),
            ])],
            vec![HashMap::from([
                (String::from("id"), ColumnValue::Integer(1)),
                (String::from("bar"), ColumnValue::Integer(3)),
            ])],
        ];

        let op = InnerJoin::new("id = id").unwrap();

        let df_refs = dfs.iter().collect();

        let result = op.transform(&df_refs);
        assert_eq!(
            result.unwrap()[0],
            vec![HashMap::from([
                (String::from("id"), ColumnValue::Integer(1)),
                (String::from("foo"), ColumnValue::Integer(0)),
                (String::from("bar"), ColumnValue::Integer(3)),
            ]),]
        )
    }

    #[test]
    fn does_not_join_on_floats() {
        let dfs = vec![
            vec![HashMap::from([
                (String::from("id"), ColumnValue::Decimal(1.0)),
                (String::from("foo"), ColumnValue::Integer(0)),
            ])],
            vec![HashMap::from([
                (String::from("id"), ColumnValue::Decimal(1.0)),
                (String::from("bar"), ColumnValue::Integer(3)),
            ])],
        ];

        let op = InnerJoin::new("id = id").unwrap();

        let df_refs = dfs.iter().collect();

        let result = op.transform(&df_refs);
        assert_eq!(result.unwrap()[0], vec![],)
    }

    #[test]
    fn unable_to_parse_clause() {
        let op = InnerJoin::new("id > 3");
        assert!(op.is_err_and(|err| match err {
            RustyPipesError::TransformationError(message) =>
                message.contains("Unable to parse join clause"),
            _ => false,
        }));
    }
}
