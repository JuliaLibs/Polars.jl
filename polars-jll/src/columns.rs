use polars::prelude::*;
use jlrs::{data::managed::{ccall_ref::{CCallRef, CCallRefRet}, string::StringRet, value::typed::TypedValue}, prelude::*};

use crate::{polars_value_type_t, utils::{leak_string, leak_value}};


#[derive(Debug, OpaqueType)]
#[allow(non_camel_case_types)]
pub struct polars_column_t {
  pub(crate) inner: Column,
}

pub type ColumnRet = CCallRefRet<polars_column_t>;
pub type ColumnRef<'data> = CCallRef<'data, polars_column_t>;
pub type ColumnValue<'scope, 'data> = TypedValue<'scope, 'data, polars_column_t>;


impl polars_column_t {
  pub fn new_empty(name: JuliaString) -> JlrsResult<ColumnRet> {
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

  pub fn is_null(&self, idx: usize) -> bool {
    matches!(self.inner.get(idx), Ok(AnyValue::Null))
  }
}
