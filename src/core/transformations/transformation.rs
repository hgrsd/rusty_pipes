use crate::core::dataframe::DataFrame;

pub trait Transformation {
    fn transform(&self, dfs: Vec<DataFrame>) -> Vec<DataFrame>;
}
