use jlrs::{data::managed::{string::StringRet, symbol::SymbolRet}, prelude::*};

use crate::utils::{leak_string, leak_symbol};

#[derive(Debug, OpaqueType)]
#[allow(non_camel_case_types)]
pub struct polars_value_type_t {
  pub inner: polars::prelude::DataType,
}

impl polars_value_type_t {
  pub fn display(&self) -> StringRet {
    leak_string(format!("{}", self.inner))
  }

  pub fn symbol(&self) -> SymbolRet {
    leak_symbol(as_str(&self.inner))
  }

}

pub fn as_str(dtype: &polars::prelude::DataType) -> &'static str {
  match dtype {
    polars::prelude::DataType::Null => "Null",
    polars::prelude::DataType::Boolean => "Boolean",
    polars::prelude::DataType::String => "String",
    polars::prelude::DataType::Binary => "Binary",
    polars::prelude::DataType::BinaryOffset => "BinaryOffset",
    polars::prelude::DataType::Int8 => "Int8",
    polars::prelude::DataType::Int16 => "Int16",
    polars::prelude::DataType::Int32 => "Int32",
    polars::prelude::DataType::Int64 => "Int64",
    polars::prelude::DataType::Int128 => "Int128",
    polars::prelude::DataType::UInt8 => "UInt8",
    polars::prelude::DataType::UInt16 => "UInt16",
    polars::prelude::DataType::UInt32 => "UInt32",
    polars::prelude::DataType::UInt64 => "UInt64",
    polars::prelude::DataType::Float32 => "Float32",
    polars::prelude::DataType::Float64 => "Float64",
    polars::prelude::DataType::Date => "Date",
    polars::prelude::DataType::Time => "Time",
    polars::prelude::DataType::Datetime(_, _) => "Datetime",
    polars::prelude::DataType::Duration(_) => "Duration",
    polars::prelude::DataType::List(_) => "List",
    #[cfg(feature = "dtype-array")]
    polars::prelude::DataType::Array(_, _) => "Array",
    #[cfg(feature = "dtype-decimal")]
    polars::prelude::DataType::Decimal(_, _) => "Decimal",
    #[cfg(feature = "dtype-categorical")]
    polars::prelude::DataType::Enum(_, _) => "Enum",
    #[cfg(feature = "dtype-categorical")]
    polars::prelude::DataType::Categorical(_, _) => "Categorical",
    #[cfg(feature = "dtype-struct")]
    polars::prelude::DataType::Struct(_) => "Struct",
    polars::prelude::DataType::Unknown(_) => "Unknown",
    }
}
