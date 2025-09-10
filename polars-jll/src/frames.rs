use polars::prelude::*;
use jlrs::{data::managed::ccall_ref::CCallRefRet, prelude::*};

use crate::leak_value;

#[derive(Debug, OpaqueType)]
#[allow(non_camel_case_types)]
pub struct polars_dataframe_t {
  inner: DataFrame,
}

impl polars_dataframe_t {
  pub fn new_empty() -> CCallRefRet<Self> {
    leak_value(Self { inner: DataFrame::empty() })
  }

  pub fn height(&self) -> usize {
    self.inner.height()
  }
}

// Re-exported for use by the julia module
pub type DataFrameRef = *mut DataFrame;
