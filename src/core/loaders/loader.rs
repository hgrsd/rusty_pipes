use std::error::Error;

use crate::core::dataframe::Dataframe;

pub trait Loader {
    fn load(&self) -> Result<Dataframe, Box<dyn Error>>;
}
