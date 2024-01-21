use super::Transformation;
use crate::core::context::Context;
use crate::core::dataframe::{ColumnValue, Dataframe};
use crate::core::error::RustyPipesError;
use crate::core::result::RustyPipesResult;

/// Filter a Dataframe based on a given predicate. Only those rows for which the predicate is true are retained.
/// This operation has an arity of one: it requires a single dataframe to be provided as its input.
pub struct Filter<'a> {
    field_name: &'a str,
    operator: &'a str,
    resolved_target: String,
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

fn resolve_parameter(key: &str, context: &Context) -> RustyPipesResult<String> {
    if key.starts_with(':') {
        context
            .parameter_value(key.chars().skip(1).collect::<String>().as_str())
            .map(|value| value.to_owned())
            .ok_or_else(|| {
                RustyPipesError::TransformationError(format!("Unable to resolve parameter {}", key))
            })
    } else {
        Ok(key.to_owned())
    }
}

fn contains(value: &ColumnValue, target: &str) -> bool {
    if let ColumnValue::String(v) = value {
        v.contains(target)
    } else {
        false
    }
}

impl<'a> Filter<'a> {
    /// Construct a new Filter from the given predicate. The expected format of this predicate is
    /// "column_name operation literal" where operation is one of >, >=, <, <=, ==, or != and the literal is
    /// an integer, decimal, or string. E.g., "column_one >= 100.5".
    pub fn new(predicate: &'a str, context: &Context) -> RustyPipesResult<Self> {
        let mut s = predicate.split_whitespace();
        let field_name = s.next().unwrap();
        let operator = s.next().unwrap();
        let target = s.next().unwrap();
        let resolved_target = resolve_parameter(target, context)?;

        Ok(Filter {
            field_name,
            operator,
            resolved_target,
        })
    }
}

impl Transformation for Filter<'_> {
    fn transform(&self, dfs: &Vec<&Dataframe>) -> RustyPipesResult<Vec<Dataframe>> {
        let mut filtered = vec![];
        for row in dfs[0] {
            if let Some(value) = row.get(self.field_name) {
                let should_include = match self.operator {
                    ">" => compare!(gt, value, &self.resolved_target),
                    ">=" => compare!(ge, value, &self.resolved_target),
                    "<" => compare!(lt, value, &self.resolved_target),
                    "<=" => compare!(le, value, &self.resolved_target),
                    "==" => compare!(eq, value, &self.resolved_target),
                    "!=" => compare!(ne, value, &self.resolved_target),
                    "contains" => contains(value, self.resolved_target.as_str()),
                    "!contains" => !contains(value, self.resolved_target.as_str()),
                    _ => unimplemented!(),
                };

                if should_include {
                    filtered.push(row.clone());
                }
            }
        }
        Ok(vec![filtered])
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
        let op = Filter::new("foo > 1", &ctx(HashMap::default())).unwrap();
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
        let op = Filter::new("foo >= 1", &ctx(HashMap::default())).unwrap();

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
        let op = Filter::new("foo < 1", &ctx(HashMap::default())).unwrap();
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
        let op = Filter::new("foo <= 1", &ctx(HashMap::default())).unwrap();
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
        let op = Filter::new("foo == 1", &ctx(HashMap::default())).unwrap();
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
        let op = Filter::new("foo != 1", &ctx(HashMap::default())).unwrap();
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
        let op = Filter::new("foo contains bar", &Default::default()).unwrap();
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
        let op = Filter::new("foo !contains bar", &Default::default()).unwrap();
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
        )
        .unwrap();
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
