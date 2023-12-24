use crate::dataframe::{ColumnValue, DataFrame};
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
    apply: Box<dyn Fn(&DataFrame, &DataFrame) -> DataFrame>,
}

impl InnerJoin {
    fn group_columns<'a>(
        key: &str,
        df: &'a DataFrame,
    ) -> HashMap<String, Vec<&'a HashMap<String, ColumnValue>>> {
        let mut grouped: HashMap<String, Vec<&HashMap<String, ColumnValue>>> = HashMap::new();
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
        let apply = Box::new(move |left: &DataFrame, right: &DataFrame| {
            let right_by_key = Self::group_columns(&right_owned, right);
            let mut joined_set: DataFrame = vec![];
            for row in left.iter() {
                if let Some(identifier) = row.get(&left_owned).and_then(extract_identifier) {
                    if let Some(matches) = right_by_key.get(&identifier) {
                        for m in matches {
                            let mut joined_row: HashMap<String, ColumnValue> = HashMap::new();
                            for (k, v) in row {
                                joined_row.insert(k.clone(), v.clone());
                            }
                            for (k, v) in *m {
                                joined_row.insert(k.clone(), v.clone());
                            }
                            joined_set.push(joined_row);
                        }
                    }
                }
            }
            joined_set
        });
        InnerJoin { apply }
    }
}

impl Transformation for InnerJoin {
    fn transform(&self, dfs: Vec<DataFrame>) -> Vec<DataFrame> {
        vec![(self.apply)(&dfs[0], &dfs[1])]
    }
}
