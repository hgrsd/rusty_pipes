use crate::dataframe::{ColumnValue, DataFrame};

use super::Transformation;

pub struct Filter {
    apply: Box<dyn Fn(&DataFrame) -> DataFrame>,
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

        let apply = Box::new(move |df: &DataFrame| {
            df.iter()
                .filter(|row| {
                    if let Some(value) = row.get(&field_name) {
                        match operator.as_str() {
                            ">" => compare!(gt, value, &comparand),
                            ">=" => compare!(ge, value, &comparand),
                            "<=" => compare!(le, value, &comparand),
                            "<" => compare!(lt, value, &comparand),
                            "!=" => compare!(ne, value, &comparand),
                            "==" => compare!(eq, value, &comparand),
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
    fn transform(&self, df: &DataFrame) -> DataFrame {
        (self.apply)(df)
    }
}
