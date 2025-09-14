use jlrs::{data::{managed::{ccall_ref::CCallRef, named_tuple::NamedTuple, string::StringRet, symbol::SymbolRet, value::{typed::TypedValue, ValueRet}}, types::construct_type::ConstructType}, inline_static_ref, prelude::*, weak_handle};
use polars::prelude::TimeZone;

use crate::{errors::JuliaPolarsError, utils::{leak_string, leak_symbol, leak_value, JuliaNamedTupleExt, JuliaValueExt}};

#[derive(Debug, OpaqueType)]
#[allow(non_camel_case_types)]
pub struct polars_value_type_t {
  pub inner: polars::prelude::DataType,
}

// since DataType is used in julia, we use term ValueType instead
pub type ValueTypeRet = jlrs::data::managed::ccall_ref::CCallRefRet<polars_value_type_t>;
pub type ValueTypeRef<'scope> = jlrs::data::managed::ccall_ref::CCallRef<'scope, ValueTypeValue<'scope, 'static>>;
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
        fn jl_value<'s, 'd, T: IntoJulia + ConstructType>(handle: &impl Target<'s>, v: T) -> Value<'s, 'd> {
          unsafe { TypedValue::new(handle, v).as_value() }
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
        let result = NamedTuple::new(&handle, &keys, &vals)
          .map_err(|e| JuliaPolarsError::function_call("NamedTuple::new", e))?;
        Ok(unsafe { result.as_value().leak() })
      },
      Err(_) => JuliaPolarsError::WeakHandleError("polars_value_type_t::kwargs").panic(),
    }
  }

  pub fn from_name_and_kwargs<'scope, 'data>(name: CCallRef<'scope, Symbol<'scope>>, kwargs: CCallRef<'scope, NamedTuple<'scope, 'static>>) -> JlrsResult<ValueTypeRet> {
    match weak_handle!() {
      Ok(handle) => {
        let name = name.as_managed()?.as_str()?;
        let kwargs = kwargs.as_managed()?;
        println!("from_name_and_kwargs: name={}, kwargs={:?}", name, kwargs.field_names().iter().map(|i| i.as_str().unwrap_or_default()).collect::<Vec<_>>());
        let get_tu = || -> JlrsResult<_> {
          let s = kwargs.get_value(&handle, "time_unit")?.cast::<Symbol>()?;
          match s.as_str()? {
            "ns" => Ok(polars::prelude::TimeUnit::Nanoseconds),
            "μs" => Ok(polars::prelude::TimeUnit::Microseconds),
            "ms" => Ok(polars::prelude::TimeUnit::Milliseconds),
            s => Err(JuliaPolarsError::TimeUnitError(s.to_string()))?,
          }
        };
        let get_tz = || -> JlrsResult<_> {
          let Ok(v) = kwargs.get_value(&handle, "time_zone") else {
            return Ok(None)
          };
          let Some(v) = v.as_cast_opt::<JuliaString>()? else {
            return Ok(None);
          };
          Ok(Some(v.as_str()?.to_string()))
        };
        let get_dtype = |key: &str| -> JlrsResult<_> {
          let v = kwargs.get_value(&handle, key)?;
          let intoraw = inline_static_ref!(INTORAW_FUNCTION, Value, "Polars.DataTypes.intoraw", handle);
          match v.track_shared::<polars_value_type_t>() {
            Ok(dt) => Ok(dt.inner.clone()),
            Err(_) => match unsafe { intoraw.call(&handle, [v]) } {
              Ok(v) => {
                let v = unsafe { v.as_value() };
                Ok(v.track_shared::<polars_value_type_t>()?.inner.clone())
              },
              Err(e) => Err(JuliaPolarsError::function_call("Polars.DataTypes.intoraw", e))?,
            },
          }
        };
        let get_size = |key: &str| -> JlrsResult<_> {
          let size = kwargs.get_value(&handle, key)?.unbox::<i64>()?;
          // println!("size value: k={} {:?}", key, size);
          Ok(size as usize)
        };
        let dtype = match name {
          "Null" => polars::prelude::DataType::Null,
          "Boolean" => polars::prelude::DataType::Boolean,
          "String" => polars::prelude::DataType::String,
          "Binary" => polars::prelude::DataType::Binary,
          "BinaryOffset" => polars::prelude::DataType::BinaryOffset,
          "Int8" => polars::prelude::DataType::Int8,
          "Int16" => polars::prelude::DataType::Int16,
          "Int32" => polars::prelude::DataType::Int32,
          "Int64" => polars::prelude::DataType::Int64,
          "Int128" => polars::prelude::DataType::Int128,
          "UInt8" => polars::prelude::DataType::UInt8,
          "UInt16" => polars::prelude::DataType::UInt16,
          "UInt32" => polars::prelude::DataType::UInt32,
          "UInt64" => polars::prelude::DataType::UInt64,
          "Float32" => polars::prelude::DataType::Float32,
          "Float64" => polars::prelude::DataType::Float64,
          "Date" => polars::prelude::DataType::Date,
          "Time" => polars::prelude::DataType::Time,
          "Datetime" => {
            let tu = get_tu()?;
            let tz = get_tz()?.map(|s| unsafe { TimeZone::new_unchecked(s) });
            polars::prelude::DataType::Datetime(tu, tz)
          },
          "Duration" => {
            let tu = get_tu()?;
            polars::prelude::DataType::Duration(tu)
          },
          "List" => {
            let inner = get_dtype("inner")?;
            polars::prelude::DataType::List(Box::new(inner))
          },
          #[cfg(feature = "dtype-array")]
          "Array" => {
            let inner = get_dtype("inner")?;
            let size = get_size("size")?;
            polars::prelude::DataType::Array(Box::new(inner), size)
          },
          #[cfg(feature = "dtype-decimal")]
          "Decimal" => {
            let precision = if kwargs.contains("precision") {
              Some(get_size("precision")?)
            } else {
              None
            };
            let scale = if kwargs.contains("scale") {
              Some(get_size("scale")?)
            } else {
              None
            };
            polars::prelude::DataType::Decimal(precision, scale)
          },
          s => return Err(JuliaPolarsError::UnsupportedDataType(s.to_string()))?,
        };
        Ok(leak_value(polars_value_type_t { inner: dtype }))
      },
      Err(_) => JuliaPolarsError::WeakHandleError("polars_value_type_t::from_name_and_kwargs").panic(),
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
