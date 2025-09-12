use core::panic;

use jlrs::{
  data::{
    managed::{ccall_ref::{CCallRef, CCallRefRet}, string::StringRet, symbol::SymbolRet, value::ValueRet},
    types::abstract_type::IO
  }, prelude::*
};

pub mod utils;
pub mod errors;
pub mod columns;
pub mod frames;
pub mod value_types;

pub type CCallResult<T> = JlrsResult<CCallRefRet<T>>;

pub use errors::polars_error_t;
pub use frames::{polars_dataframe_t, DataFrameRef, DataFrameRet, DataFrameValue};
pub use columns::{polars_column_t, ColumnRef, ColumnRet, ColumnValue};
pub use value_types::{polars_value_type_t, ValueTypeRef, ValueTypeRet, ValueTypeValue};

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
  in polars_column_t fn dtype(&self) -> ValueTypeRet as polars_column_dtype;
  in polars_column_t fn name(&self) -> StringRet as polars_column_name;
  in polars_column_t fn null_count(&self) -> usize as polars_column_null_count;
  in polars_column_t fn is_null(&self, idx: usize) -> bool as polars_column_is_null;

  struct polars_value_type_t;
  in polars_value_type_t fn display(&self) -> StringRet as polars_value_type_display;
  in polars_value_type_t fn symbol(&self) -> SymbolRet as polars_value_type_symbol;
  // this is actually JlrsResult<NamedTupleRet>
  // https://github.com/Taaitaaiger/jlrs/issues/197
  in polars_value_type_t fn kwargs(&self) -> JlrsResult<ValueRet> as polars_value_type_kwargs;
}

pub fn polars_version() -> StringRet {
  utils::leak_string(polars::VERSION)
}
