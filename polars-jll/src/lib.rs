use core::panic;

use jlrs::{
  data::{
    managed::{ccall_ref::{CCallRef, CCallRefRet}, string::StringRet},
    types::abstract_type::IO
  }, prelude::*
};

pub mod utils;
pub mod errors;
pub mod columns;
pub mod frames;
pub mod values;

pub type CCallResult<T> = JlrsResult<CCallRefRet<T>>;

pub use errors::polars_error_t;
pub use frames::{polars_dataframe_t, DataFrameRef, DataFrameRet, DataFrameValue};
pub use columns::{polars_column_t, ColumnRef, ColumnRet, ColumnValue};
pub use values::polars_value_type_t;

julia_module!{
  become julia_module_polars_init_fn;

  fn polars_version() -> StringRet;

  struct polars_error_t;
  in polars_error_t fn new(msg: JuliaString) -> CCallRefRet<polars_error_t> as polars_error_new;
  in polars_error_t fn message(&self) -> StringRet as polars_error_message;

  struct polars_dataframe_t;
  in polars_dataframe_t fn new_empty() -> DataFrameRet as polars_dataframe_new_empty;
  in polars_dataframe_t fn from_cols(cols: TypedVector<ColumnValue>) -> JlrsResult<DataFrameRet> as polars_dataframe_from_cols;
  in polars_dataframe_t fn height(&self) -> usize as polars_dataframe_height;
  in polars_dataframe_t fn read_parquet(path: JuliaString) -> JlrsResult<DataFrameRet> as polars_dataframe_read_parquet;
  in polars_dataframe_t fn write_parquet(&mut self, path: JuliaString) -> JlrsResult<()> as polars_dataframe_write_parquet;
  in polars_dataframe_t fn show(&self, io: CCallRef<IO>) -> JlrsResult<()> as polars_dataframe_show;
  in polars_dataframe_t fn get_column(&self, name: JuliaString) -> JlrsResult<ColumnRet> as polars_dataframe_get_column;

  struct polars_column_t;
  in polars_column_t fn new_empty(name: JuliaString) -> JlrsResult<ColumnRet> as polars_column_new_empty;
  in polars_column_t fn len(&self) -> usize as polars_column_len;
  in polars_column_t fn dtype(&self) -> CCallRefRet<polars_value_type_t> as polars_column_dtype;
  in polars_column_t fn name(&self) -> StringRet as polars_column_name;
  in polars_column_t fn null_count(&self) -> usize as polars_column_null_count;

  struct polars_value_type_t;
  in polars_value_type_t fn display(&self) -> StringRet as polars_value_type_display;
}

pub fn polars_version() -> StringRet {
  utils::leak_string(polars::VERSION)
}
