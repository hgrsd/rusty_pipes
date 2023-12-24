use crate::dataframe::DataFrame;

pub trait Transformation {
    fn transform(&self, df: DataFrame) -> DataFrame;
}
