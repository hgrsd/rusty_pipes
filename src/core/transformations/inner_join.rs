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

pub struct InnerJoin {
    apply: Box<dyn Fn(&Dataframe, &Dataframe) -> Dataframe>,
}

impl InnerJoin {
    fn group_columns<'a>(key: &str, df: &'a Dataframe) -> HashMap<String, Vec<&'a Row>> {
        let mut grouped: HashMap<String, Vec<&Row>> = HashMap::new();
        for row in df {
            if let Some(identifier) = row.get(key).and_then(extract_identifier) {
                if let Some(existing) = grouped.get_mut(&identifier) {
                    existing.push(row);
                } else {
                    grouped.insert(identifier, vec![row]);
                }
            }
        }
        grouped
    }

    pub fn new(join_on: &str) -> Self {
        let (left_field_name, right_field_name) = join_on.split_once("=").unwrap();
        let (left_owned, right_owned) = (
            left_field_name.trim().to_owned(),
            right_field_name.trim().to_owned(),
        );
        let apply = Box::new(move |left: &Dataframe, right: &Dataframe| {
            let right_by_key = Self::group_columns(&right_owned, right);
            let mut dataframe: Dataframe = vec![];

            for row in left.iter() {
                if let Some(identifier) = row.get(&left_owned).and_then(extract_identifier) {
                    if let Some(matches) = right_by_key.get(&identifier) {
                        for m in matches {
                            let mut joined_row: Row = HashMap::new();
                            for (k, v) in row {
                                joined_row.insert(k.clone(), v.clone());
                            }
                            for (k, v) in *m {
                                joined_row.insert(k.clone(), v.clone());
                            }
                            dataframe.push(joined_row);
                        }
                    }
                }
            }
            dataframe
        });
        InnerJoin { apply }
    }
}

impl Transformation for InnerJoin {
    fn transform(&self, dfs: Vec<Dataframe>) -> Vec<Dataframe> {
        vec![(self.apply)(&dfs[0], &dfs[1])]
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

        let result = op.transform(dfs);
        assert_eq!(result[0], vec![],)
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

        let result = op.transform(dfs);
        assert_eq!(result[0], vec![],)
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

        let result = op.transform(dfs);
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
            ],
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

        let result = op.transform(dfs);
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
                ]),
            ],
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

        let result = op.transform(dfs);
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
                ]),
            ],
        )
    }
}
