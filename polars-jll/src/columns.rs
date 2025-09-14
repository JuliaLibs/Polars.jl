use polars::prelude::*;
use jlrs::{data::managed::{string::StringRet, value::typed::TypedValue}, prelude::*};

use crate::{errors::PolarsJlError, polars_value_type_t, utils::{leak_string, leak_value, CCallRefExt}, values::{polars_value_t, AnyValueRet}, ValueTypeRef, ValueTypeRet};


#[derive(Debug, OpaqueType)]
#[allow(non_camel_case_types)]
pub struct polars_column_t {
  pub(crate) inner: Column,
}

pub type ColumnRet = jlrs::data::managed::ccall_ref::CCallRefRet<polars_column_t>;
pub type ColumnRef<'scope> = jlrs::data::managed::ccall_ref::CCallRef<'scope, ColumnValue<'scope, 'static>>;
pub type ColumnValue<'scope, 'data> = TypedValue<'scope, 'data, polars_column_t>;


impl polars_column_t {
  pub fn new_empty(name: JuliaString, dtype: ValueTypeRef) -> JlrsResult<ColumnRet> {
    let name = name.as_str()?.to_string();
    let dtype = dtype.tracked_map(|i| i.inner.clone())?;
    Ok(leak_value(Self { inner: Column::new_empty(name.into(), &dtype) }))
  }

  pub fn len(&self) -> usize {
    self.inner.len()
  }

  pub fn null_count(&self) -> usize {
    self.inner.null_count()
  }

  pub fn dtype(&self) -> ValueTypeRet {
    leak_value(polars_value_type_t { inner: self.inner.dtype().clone() })
  }

  pub fn name(&self) -> StringRet {
    leak_string(self.inner.name().as_str())
  }

  pub fn is_null(&self, idx: usize) -> bool {
    matches!(self.inner.get(idx), Ok(AnyValue::Null))
  }

  pub fn get(&self, idx: usize) -> JlrsResult<AnyValueRet> {
    let v = self.inner.get(idx).map_err(PolarsJlError::from)?;
    Ok(leak_value(polars_value_t { inner: v.into_static() }))
  }
}
