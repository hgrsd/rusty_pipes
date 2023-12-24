use std::error::Error;

use crate::dataframe::DataFrame;

pub trait Loader {
    fn load(&self) -> Result<DataFrame, Box<dyn Error>>;
}
