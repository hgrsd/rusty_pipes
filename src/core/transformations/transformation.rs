use crate::core::dataframe::Dataframe;

pub trait Transformation {
    fn transform(&self, dfs: Vec<Dataframe>) -> Vec<Dataframe>;
}
