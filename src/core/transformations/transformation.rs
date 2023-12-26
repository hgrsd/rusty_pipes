use crate::core::dataframe::Dataframe;

/// Transform a vector of data frames. Individual transformations will have to implement this trait.
/// Each individual transformation will have its own semantics about what it expects as its vector of inputs, as
/// well as the arity of this vector.
pub trait Transformation {
    fn transform(&self, dfs: Vec<Dataframe>) -> Vec<Dataframe>;
}
