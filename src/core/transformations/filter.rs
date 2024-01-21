use super::Transformation;
use crate::core::context::Context;
use crate::core::dataframe::{ColumnValue, Dataframe};
use crate::core::result::RustyPipesResult;

/// Filter a Dataframe based on a given predicate. Only those rows for which the predicate is true are retained.
/// This operation has an arity of one: it requires a single dataframe to be provided as its input.
pub struct Filter {
    apply: Box<dyn Fn(&Dataframe) -> RustyPipesResult<Dataframe>>,
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
            _ => false,
        }
    };
}

fn resolve_parameter(key: &str, context: &Context) -> String {
    if key.starts_with(':') {
        context
            .parameter_value(key.chars().skip(1).collect::<String>().as_str())
            .unwrap_or_else(|| panic!("No parameter with key {} found", key))
            .to_owned()
    } else {
        key.to_owned()
    }
}

fn contains(value: &ColumnValue, target: &str) -> bool {
    if let ColumnValue::String(v) = value {
        v.contains(target)
    } else {
        false
    }
}

impl Filter {
    /// Construct a new Filter from the given predicate. The expected format of this predicate is
    /// "column_name operation literal" where operation is one of >, >=, <, <=, ==, or != and the literal is
    /// an integer, decimal, or string. E.g., "column_one >= 100.5".
    pub fn new(predicate: &str, context: &Context) -> Self {
        let mut s = predicate.split_whitespace();
        let field_name = s.next().unwrap().to_owned();
        let operator = s.next().unwrap().to_owned();
        let target = s.next().map(|x| resolve_parameter(x, context)).unwrap();

        let apply = Box::new(move |df: &Dataframe| {
            Ok(df
                .iter()
                .filter(|row| {
                    if let Some(value) = row.get(&field_name) {
                        match operator.as_str() {
                            ">" => compare!(gt, value, &target),
                            ">=" => compare!(ge, value, &target),
                            "<" => compare!(lt, value, &target),
                            "<=" => compare!(le, value, &target),
                            "==" => compare!(eq, value, &target),
                            "!=" => compare!(ne, value, &target),
                            "contains" => contains(value, target.as_str()),
                            "!contains" => !contains(value, target.as_str()),
                            _ => unimplemented!(),
                        }
                    } else {
                        true
                    }
                })
                .cloned()
                .collect())
        });

        Filter { apply }
    }
}

impl Transformation for Filter {
    fn transform(&self, dfs: &Vec<&Dataframe>) -> RustyPipesResult<Vec<Dataframe>> {
        (self.apply)(dfs[0]).map(|x| vec![x])
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::collections::HashMap;

    fn df() -> Vec<Dataframe> {
        vec![vec![
            HashMap::from([(String::from("foo"), ColumnValue::Integer(0))]),
            HashMap::from([(String::from("foo"), ColumnValue::Integer(1))]),
            HashMap::from([(String::from("foo"), ColumnValue::Integer(2))]),
        ]]
    }

    fn ctx(params: HashMap<String, String>) -> Context {
        Context::new(params)
    }

    #[test]
    fn filter_gt() {
        let op = Filter::new("foo > 1", &ctx(HashMap::default()));
        let dfs = df();
        let df_refs = dfs.iter().collect();

        let result = op.transform(&df_refs);

        assert_eq!(
            result.unwrap()[0],
            vec![HashMap::from([(
                String::from("foo"),
                ColumnValue::Integer(2)
            )]),]
        )
    }

    #[test]
    fn filter_gte() {
        let op = Filter::new("foo >= 1", &ctx(HashMap::default()));

        let dfs = df();
        let df_refs = dfs.iter().collect();

        let result = op.transform(&df_refs);

        assert_eq!(
            result.unwrap()[0],
            vec![
                HashMap::from([(String::from("foo"), ColumnValue::Integer(1))]),
                HashMap::from([(String::from("foo"), ColumnValue::Integer(2))]),
            ]
        )
    }

    #[test]
    fn filter_lt() {
        let op = Filter::new("foo < 1", &ctx(HashMap::default()));
        let dfs = df();
        let df_refs = dfs.iter().collect();

        let result = op.transform(&df_refs);

        assert_eq!(
            result.unwrap()[0],
            vec![HashMap::from([(
                String::from("foo"),
                ColumnValue::Integer(0)
            )]),]
        )
    }

    #[test]
    fn filter_lte() {
        let op = Filter::new("foo <= 1", &ctx(HashMap::default()));
        let dfs = df();
        let df_refs = dfs.iter().collect();

        let result = op.transform(&df_refs);

        assert_eq!(
            result.unwrap()[0],
            vec![
                HashMap::from([(String::from("foo"), ColumnValue::Integer(0))]),
                HashMap::from([(String::from("foo"), ColumnValue::Integer(1))]),
            ]
        )
    }

    #[test]
    fn filter_eq() {
        let op = Filter::new("foo == 1", &ctx(HashMap::default()));
        let dfs = df();
        let df_refs = dfs.iter().collect();

        let result = op.transform(&df_refs);

        assert_eq!(
            result.unwrap()[0],
            vec![HashMap::from([(
                String::from("foo"),
                ColumnValue::Integer(1)
            )]),]
        )
    }

    #[test]
    fn filter_ne() {
        let op = Filter::new("foo != 1", &ctx(HashMap::default()));
        let dfs = df();
        let df_refs = dfs.iter().collect();

        let result = op.transform(&df_refs);

        assert_eq!(
            result.unwrap()[0],
            vec![
                HashMap::from([(String::from("foo"), ColumnValue::Integer(0))]),
                HashMap::from([(String::from("foo"), ColumnValue::Integer(2))]),
            ]
        )
    }

    #[test]
    fn filter_contains() {
        let op = Filter::new("foo contains bar", &Default::default());
        let dfs = vec![vec![
            HashMap::from([(
                String::from("foo"),
                ColumnValue::String(String::from("barrister")),
            )]),
            HashMap::from([(
                String::from("foo"),
                ColumnValue::String(String::from("arable")),
            )]),
        ]];
        let df_refs = dfs.iter().collect();

        let result = op.transform(&df_refs);

        assert_eq!(
            result.unwrap()[0],
            vec![HashMap::from([(
                String::from("foo"),
                ColumnValue::String(String::from("barrister"))
            )]),]
        )
    }

    #[test]
    fn filter_not_contains() {
        let op = Filter::new("foo !contains bar", &Default::default());
        let dfs = vec![vec![
            HashMap::from([(
                String::from("foo"),
                ColumnValue::String(String::from("barrister")),
            )]),
            HashMap::from([(
                String::from("foo"),
                ColumnValue::String(String::from("arable")),
            )]),
        ]];
        let df_refs = dfs.iter().collect();

        let result = op.transform(&df_refs);

        assert_eq!(
            result.unwrap()[0],
            vec![HashMap::from([(
                String::from("foo"),
                ColumnValue::String(String::from("arable"))
            )]),]
        )
    }

    #[test]
    fn filter_using_parameter() {
        let op = Filter::new(
            "foo != :param_name",
            &ctx(HashMap::from([("param_name".to_owned(), "1".to_owned())])),
        );
        let dfs = df();
        let df_refs = dfs.iter().collect();

        let result = op.transform(&df_refs);

        assert_eq!(
            result.unwrap()[0],
            vec![
                HashMap::from([(String::from("foo"), ColumnValue::Integer(0))]),
                HashMap::from([(String::from("foo"), ColumnValue::Integer(2))]),
            ]
        )
    }
}
