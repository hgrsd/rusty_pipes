use std::collections::HashMap;
use crate::dataframe::{ColumnValue, DataFrame};

use super::Transformation;

pub struct InnerJoin {
    apply: Box<dyn Fn(&DataFrame, &DataFrame) -> DataFrame>,
}

impl InnerJoin {
    fn group_columns<'a>(key: &str, df: &'a DataFrame) -> HashMap<String, Vec<&'a HashMap<String, ColumnValue>>> {
        let mut grouped: HashMap<String, Vec<&HashMap<String, ColumnValue>>> = HashMap::new();
        for row in df {
            if let Some(ColumnValue::String(identifier_value)) = row.get(key) {
               if let Some(existing) = grouped.get_mut(identifier_value) {
                   existing.push(row);
               } else {
                   let v = vec![row];
                   grouped.insert(identifier_value.clone(), v);
               }
            }
        }
        grouped
    }

    pub fn new(join_on: &str) -> Self {
        let (left_field_name, right_field_name) = join_on.split_once("=").unwrap();
        let (left_owned, right_owned) = (left_field_name.trim().to_owned(), right_field_name.trim().to_owned());
        let apply = Box::new(move |left: &DataFrame, right: &DataFrame| {
            let right_by_key = Self::group_columns(&right_owned, right);
            let mut joined_set: DataFrame = vec![];
            for row in left.iter() {
                if let Some(ColumnValue::String(identifier)) = row.get(&left_owned) {
                    if let Some(matches) = right_by_key.get(identifier) {
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
