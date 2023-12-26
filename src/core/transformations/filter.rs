use super::Transformation;
use crate::core::dataframe::{ColumnValue, Dataframe};

pub struct Filter {
    apply: Box<dyn Fn(&Dataframe) -> Dataframe>,
}

macro_rules! compare {
    ($cmp:ident,$value:expr,$target:expr) => {
        match $value {
            ColumnValue::Decimal(v) => {
                let parsed = $target.parse::<f64>().unwrap();
                v.$cmp(&parsed)
            }
            ColumnValue::Integer(v) => {
                let parsed = $target.parse::<i64>().unwrap();
                v.$cmp(&parsed)
            }
            ColumnValue::String(v) => v.as_str().$cmp($target),
        }
    };
}

impl Filter {
    pub fn new(predicate: &str) -> Self {
        let mut s = predicate.split_whitespace();
        let field_name = s.next().unwrap().to_owned();
        let operator = s.next().unwrap().to_owned();
        let comparand = s.next().unwrap().to_owned();

        let apply = Box::new(move |df: &Dataframe| {
            df.iter()
                .filter(|row| {
                    if let Some(value) = row.get(&field_name) {
                        match operator.as_str() {
                            ">" => compare!(gt, value, &comparand),
                            ">=" => compare!(ge, value, &comparand),
                            "<" => compare!(lt, value, &comparand),
                            "<=" => compare!(le, value, &comparand),
                            "==" => compare!(eq, value, &comparand),
                            "!=" => compare!(ne, value, &comparand),
                            _ => unimplemented!(),
                        }
                    } else {
                        true
                    }
                })
                .cloned()
                .collect()
        });

        Filter { apply }
    }
}

impl Transformation for Filter {
    fn transform(&self, dfs: Vec<Dataframe>) -> Vec<Dataframe> {
        vec![(self.apply)(&dfs[0])]
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn filter_gt() {
        let df: Dataframe = vec![
            HashMap::from([(String::from("foo"), ColumnValue::Integer(0))]),
            HashMap::from([(String::from("foo"), ColumnValue::Integer(1))]),
            HashMap::from([(String::from("foo"), ColumnValue::Integer(2))]),
        ];

        let op = Filter::new("foo > 1");

        let result = op.transform(vec![df]);

        assert_eq!(
            result[0],
            vec![HashMap::from([(
                String::from("foo"),
                ColumnValue::Integer(2)
            )]),]
        )
    }

    #[test]
    fn filter_gte() {
        let df: Dataframe = vec![
            HashMap::from([(String::from("foo"), ColumnValue::Integer(0))]),
            HashMap::from([(String::from("foo"), ColumnValue::Integer(1))]),
            HashMap::from([(String::from("foo"), ColumnValue::Integer(2))]),
        ];

        let op = Filter::new("foo >= 1");

        let result = op.transform(vec![df]);

        assert_eq!(
            result[0],
            vec![
                HashMap::from([(String::from("foo"), ColumnValue::Integer(1))]),
                HashMap::from([(String::from("foo"), ColumnValue::Integer(2))]),
            ]
        )
    }

    #[test]
    fn filter_lt() {
        let df: Dataframe = vec![
            HashMap::from([(String::from("foo"), ColumnValue::Integer(0))]),
            HashMap::from([(String::from("foo"), ColumnValue::Integer(1))]),
            HashMap::from([(String::from("foo"), ColumnValue::Integer(2))]),
        ];

        let op = Filter::new("foo < 1");

        let result = op.transform(vec![df]);

        assert_eq!(
            result[0],
            vec![HashMap::from([(
                String::from("foo"),
                ColumnValue::Integer(0)
            )]),]
        )
    }

    #[test]
    fn filter_lte() {
        let df: Dataframe = vec![
            HashMap::from([(String::from("foo"), ColumnValue::Integer(0))]),
            HashMap::from([(String::from("foo"), ColumnValue::Integer(1))]),
            HashMap::from([(String::from("foo"), ColumnValue::Integer(2))]),
        ];

        let op = Filter::new("foo <= 1");

        let result = op.transform(vec![df]);

        assert_eq!(
            result[0],
            vec![
                HashMap::from([(String::from("foo"), ColumnValue::Integer(0))]),
                HashMap::from([(String::from("foo"), ColumnValue::Integer(1))]),
            ]
        )
    }

    #[test]
    fn filter_eq() {
        let df: Dataframe = vec![
            HashMap::from([(String::from("foo"), ColumnValue::Integer(0))]),
            HashMap::from([(String::from("foo"), ColumnValue::Integer(1))]),
            HashMap::from([(String::from("foo"), ColumnValue::Integer(2))]),
        ];

        let op = Filter::new("foo == 1");

        let result = op.transform(vec![df]);

        assert_eq!(
            result[0],
            vec![HashMap::from([(
                String::from("foo"),
                ColumnValue::Integer(1)
            )]),]
        )
    }

    #[test]
    fn filter_ne() {
        let df: Dataframe = vec![
            HashMap::from([(String::from("foo"), ColumnValue::Integer(0))]),
            HashMap::from([(String::from("foo"), ColumnValue::Integer(1))]),
            HashMap::from([(String::from("foo"), ColumnValue::Integer(2))]),
        ];

        let op = Filter::new("foo != 1");

        let result = op.transform(vec![df]);

        assert_eq!(
            result[0],
            vec![
                HashMap::from([(String::from("foo"), ColumnValue::Integer(0))]),
                HashMap::from([(String::from("foo"), ColumnValue::Integer(2))]),
            ]
        )
    }
}
