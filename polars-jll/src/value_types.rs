use jlrs::{data::{managed::{ccall_ref::{CCallRef, CCallRefRet}, named_tuple::NamedTuple, string::StringRet, symbol::SymbolRet, value::{typed::TypedValue, ValueRet}}, types::construct_type::ConstructType}, error::JlrsError, prelude::*, weak_handle};

use crate::utils::{leak_string, leak_symbol};

#[derive(Debug, OpaqueType)]
#[allow(non_camel_case_types)]
pub struct polars_value_type_t {
  pub inner: polars::prelude::DataType,
}

// since DataType is used in julia, we use term ValueType instead
pub type ValueTypeRet = CCallRefRet<polars_value_type_t>;
pub type ValueTypeRef<'data> = CCallRef<'data, polars_value_type_t>;
pub type ValueTypeValue<'scope, 'data> = TypedValue<'scope, 'data, polars_value_type_t>;


impl polars_value_type_t {
  pub fn display(&self) -> StringRet {
    leak_string(format!("{}", self.inner))
  }

  pub fn symbol(&self) -> SymbolRet {
    leak_symbol(as_str(&self.inner))
  }

  // this is actually JlrsResult<NamedTupleRet>
  pub fn kwargs(&self) -> JlrsResult<ValueRet> {
    use jlrs::convert::into_julia::IntoJulia;
    match weak_handle!() {
      Ok(handle) => {
        let mut keys = Vec::<Symbol>::new();
        let mut vals = Vec::<Value>::new();
        fn jl_value<'s, 'd, T: IntoJulia + ConstructType>(handle: impl Target<'s>, v: T) -> Value<'s, 'd> {
          unsafe { TypedValue::new(&handle, v).as_value() }
        }
        let sym = |s: &str| Symbol::new(&handle, s);
        let jl_none = || { Value::nothing(&handle) };
        let jl_str = |s: &str| unsafe { JuliaString::new(&handle, s).as_value() };
        let jl_sym = |s: &str| { Symbol::new(&handle, s).as_value() };
        let jl_dtype = |dt: &polars::prelude::DataType| unsafe {
          TypedValue::new(&handle, polars_value_type_t { inner: dt.clone() }).as_value()
        };
        match &self.inner {
          polars::prelude::DataType::Null => {},
          polars::prelude::DataType::Boolean => {},
          polars::prelude::DataType::String => {},
          polars::prelude::DataType::Binary => {},
          polars::prelude::DataType::BinaryOffset => {},
          polars::prelude::DataType::Int8 => {},
          polars::prelude::DataType::Int16 => {},
          polars::prelude::DataType::Int32 => {},
          polars::prelude::DataType::Int64 => {},
          polars::prelude::DataType::Int128 => {},
          polars::prelude::DataType::UInt8 => {},
          polars::prelude::DataType::UInt16 => {},
          polars::prelude::DataType::UInt32 => {},
          polars::prelude::DataType::UInt64 => {},
          polars::prelude::DataType::Float32 => {},
          polars::prelude::DataType::Float64 => {},
          polars::prelude::DataType::Date => {},
          polars::prelude::DataType::Time => {},
          polars::prelude::DataType::Datetime(tu, tz) => {
            keys.push(sym("time_unit"));
            vals.push(jl_sym(time_unit_as_str(tu)));
            keys.push(sym("time_zone"));
            if let Some(tz) = tz {
              vals.push(jl_str(tz.as_str()));
            } else {
              vals.push(jl_none());
            }
          },
          polars::prelude::DataType::Duration(tu) => {
            keys.push(sym("time_unit"));
            vals.push(jl_sym(time_unit_as_str(tu)));
          },
          polars::prelude::DataType::List(inner) => {
            keys.push(sym("inner"));
            vals.push(jl_dtype(inner));
          },
          #[cfg(feature = "dtype-array")]
          polars::prelude::DataType::Array(inner, size) => {
            keys.push(sym("inner"));
            vals.push(jl_dtype(inner));
            keys.push(sym("size"));
            vals.push(jl_value(&handle, *size));
          },
          #[cfg(feature = "dtype-decimal")]
          polars::prelude::DataType::Decimal(precision, scale) => {
            keys.push(sym("precision"));
            if let Some(precision) = precision {
              vals.push(jl_value(&handle, *precision));
            } else {
              vals.push(jl_none());
            }
            keys.push(sym("scale"));
            if let Some(scale) = scale {
              vals.push(jl_value(&handle, *scale));
            } else {
              vals.push(jl_none());
            }
          }
          raw => {
            keys.push(sym("raw"));
            vals.push(jl_dtype(raw));
          }
        }
        let result = NamedTuple::new(&handle, &keys, &vals).map_err(|e| JlrsError::exception(format!("{:?}", e)))?;
        Ok(unsafe { result.as_value().leak() })
      },
      Err(_) => panic!("Could not create weak handle to Julia."),
    }
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

pub fn time_unit_as_str(tu: &polars::prelude::TimeUnit) -> &'static str {
  match tu {
    polars::prelude::TimeUnit::Nanoseconds => "ns",
    polars::prelude::TimeUnit::Microseconds => "μs",
    polars::prelude::TimeUnit::Milliseconds => "ms",
  }
}
