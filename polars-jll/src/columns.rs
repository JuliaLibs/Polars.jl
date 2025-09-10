use polars::prelude::*;
use jlrs::{data::managed::{ccall_ref::CCallRefRet, string::StringRet}, prelude::*};

use crate::{polars_value_type_t, utils::{leak_string, leak_value}, CCallResult};


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

  pub fn null_count(&self) -> usize {
    self.inner.null_count()
  }

  pub fn dtype(&self) -> CCallRefRet<polars_value_type_t> {
    leak_value(polars_value_type_t { inner: self.inner.dtype().clone() })
  }

  pub fn name(&self) -> StringRet {
    leak_string(self.inner.name().as_str())
  }
}
