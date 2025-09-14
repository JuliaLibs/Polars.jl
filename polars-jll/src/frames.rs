use polars::prelude::*;
use jlrs::{data::{managed::{ccall_ref::CCallRef, value::typed::TypedValue}, types::abstract_type::IO}, prelude::*, weak_handle};

use crate::{errors::PolarsJlError, polars_column_t, utils::{leak_value, IOWrapper, TypedVecExt}, ColumnRet, ColumnValue};

#[derive(Debug, OpaqueType)]
#[allow(non_camel_case_types)]
pub struct polars_dataframe_t {
  pub(crate) inner: DataFrame,
}

pub type DataFrameRet = jlrs::data::managed::ccall_ref::CCallRefRet<polars_dataframe_t>;
pub type DataFrameRef<'scope> = jlrs::data::managed::ccall_ref::CCallRef<'scope, DataFrameValue<'scope, 'static>>;
pub type DataFrameValue<'scope, 'data> = TypedValue<'scope, 'data, polars_dataframe_t>;

impl polars_dataframe_t {
  pub fn new_empty() -> DataFrameRet {
    leak_value(Self { inner: DataFrame::empty() })
  }

  pub fn from_cols(cols: TypedVector<ColumnValue>) -> JlrsResult<DataFrameRet> {
    let cols = cols.extract_box(|c| c.inner.clone())?;
    Ok(leak_value(Self { inner: DataFrame::new(cols).unwrap() }))
  }

  pub fn height(&self) -> usize {
    self.inner.height()
  }

  pub fn read_parquet(path: JuliaString) -> JlrsResult<DataFrameRet> {
    let path = path.as_str()?;
    let file = std::fs::File::open(path).map_err(PolarsJlError::from)?;
    let df = ParquetReader::new(file).finish().map_err(PolarsJlError::from)?;
    Ok(leak_value(Self { inner: df }))
  }

  pub fn write_parquet(&mut self, path: JuliaString) -> JlrsResult<()> {
    let path = path.as_str()?;
    let file = std::fs::File::create(path).map_err(PolarsJlError::from)?;
    ParquetWriter::new(file).finish(&mut self.inner).map_err(PolarsJlError::from)?;
    Ok(())
  }

  pub fn show(&self, io: CCallRef<IO>) -> JlrsResult<()> {
    match weak_handle!() {
      Ok(handle) => {
        use std::io::Write;
        let mut w = IOWrapper::new(&handle, &io);
        writeln!(w, "{}", self.inner).map_err(PolarsJlError::from)?;
        Ok(())
      },
      Err(_) => PolarsJlError::WeakHandleError("polars_dataframe_t::show").panic(),
    }
  }

  pub fn get_column(&self, name: JuliaString) -> JlrsResult<ColumnRet> {
    let name = name.as_str()?;
    let col = self.inner.column(name).map_err(PolarsJlError::from)?;
    Ok(leak_value(polars_column_t { inner: col.clone() }))
  }
}
