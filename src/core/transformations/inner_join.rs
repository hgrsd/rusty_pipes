use crate::core::dataframe::{ColumnValue, Dataframe};
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

/// Inner Join two data frames. This operation has an arity of two: it requires two dataframes to be provided as its
/// inputs.
pub struct InnerJoin {
    apply: Box<dyn Fn(&Dataframe, &Dataframe) -> Dataframe>,
}

impl InnerJoin {
    fn group_rows<'a>(key: &str, df: &'a Dataframe) -> HashMap<String, Vec<&'a Row>> {
        let grouped = df.iter().filter_map(|row| {
            row.get(key).and_then(extract_identifier).map(|identifier| (identifier, vec![row]))
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

    /// Construct a new InnerJoin from the given join clause.
    /// The expected format of this clause is "left_column_name = right_column_name" where left_column_name
    /// and right_column_name refer to the names of the identifying columns in the left and right dataframes.
    pub fn new(join_on: &str) -> Self {
        let (left_field_name, right_field_name) = join_on.split_once('=').unwrap();
        let (left_key, right_key) = (
            left_field_name.trim().to_owned(),
            right_field_name.trim().to_owned(),
        );

        let apply = Box::new(move |left: &Dataframe, right: &Dataframe| {
            let right_rows_by_key = Self::group_rows(&right_key, right);

            left.iter()
                .flat_map(|left_row| {
                    left_row
                        .get(&left_key)
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
                .collect()
        });

        InnerJoin { apply }
    }
}

impl Transformation for InnerJoin {
    fn transform(&self, dfs: &Vec<&Dataframe>) -> Vec<Dataframe> {
        vec![(self.apply)(dfs[0], dfs[1])]
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

        let op = InnerJoin::new("id = id");

        let df_refs = dfs.iter().collect();

        let result = op.transform(&df_refs);
        assert_eq!(result[0], vec![])
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

        let op = InnerJoin::new("non_existing = non_existing");
        let df_refs = dfs.iter().collect();

        let result = op.transform(&df_refs);
        assert_eq!(result[0], vec![])
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

        let op = InnerJoin::new("id = id");

        let df_refs = dfs.iter().collect();

        let result = op.transform(&df_refs);
        assert_eq!(
            result[0],
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

        let op = InnerJoin::new("id = id");

        let df_refs = dfs.iter().collect();

        let result = op.transform(&df_refs);
        assert_eq!(
            result[0],
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

        let op = InnerJoin::new("id = id");

        let df_refs = dfs.iter().collect();

        let result = op.transform(&df_refs);
        assert_eq!(
            result[0],
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

        let op = InnerJoin::new("id = id");

        let df_refs = dfs.iter().collect();

        let result = op.transform(&df_refs);
        assert_eq!(
            result[0],
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

        let op = InnerJoin::new("id = id");

        let df_refs = dfs.iter().collect();

        let result = op.transform(&df_refs);
        assert_eq!(result[0], vec![],)
    }
}
