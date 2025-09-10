use polars::prelude::*;
use jlrs::{data::{managed::ccall_ref::{CCallRef, CCallRefRet}, types::abstract_type::IO}, error::JlrsError, prelude::*, weak_handle};

use crate::{leak_value, CCallResult, IOWrapper};

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

  pub fn read_parquet(path: JuliaString) -> CCallResult<Self> {
    let path = path.as_str()?;
    let file = std::fs::File::open(path).map_err(JlrsError::other)?;
    let df = ParquetReader::new(file).finish().map_err(JlrsError::other)?;
    Ok(leak_value(Self { inner: df }))
  }

  pub fn write_parquet(&mut self, path: JuliaString) -> JlrsResult<()> {
    let path = path.as_str()?;
    let file = std::fs::File::create(path).map_err(JlrsError::other)?;
    ParquetWriter::new(file).finish(&mut self.inner).map_err(JlrsError::other)?;
    Ok(())
  }

  pub fn show(&self, io: CCallRef<IO>) -> JlrsResult<()> {
    match weak_handle!() {
      Ok(handle) => {
        use std::io::Write;
        let mut w = IOWrapper::new(&handle, &io);
        writeln!(w, "{}", self.inner).map_err(JlrsError::other)?;
        Ok(())
      },
      Err(_) => panic!("Could not create weak handle to Julia."),
    }
  }
}

// Re-exported for use by the julia module
pub type DataFrameRef = *mut DataFrame;
