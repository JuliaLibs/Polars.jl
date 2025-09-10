use polars::prelude::*;
use jlrs::prelude::*;

use crate::{leak_value, CCallResult};


#[derive(Debug, OpaqueType)]
#[allow(non_camel_case_types)]
pub struct polars_column_t {
  pub(crate) inner: Column,
}

impl polars_column_t {
  pub fn new_empty(name: JuliaString) -> CCallResult<Self> {
    let name = name.as_str()?.to_string();
    Ok(leak_value(Self { inner: Column::new_empty(name.into(), &polars::prelude::DataType::Int64) }))
  }

  pub fn len(&self) -> usize {
    self.inner.len()
  }
}
