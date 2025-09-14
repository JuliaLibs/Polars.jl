use polars::prelude::*;
use jlrs::{data::{managed::{value::{typed::TypedValue, ValueRet}}, types::construct_type::ConstructType}, inline_static_ref, prelude::*, weak_handle};

use crate::{errors::JuliaPolarsError, polars_value_type_t, utils::leak_value, value_types::time_unit_as_str, ValueTypeRet};

#[derive(Debug, OpaqueType)]
#[allow(non_camel_case_types)]
pub struct polars_value_t {
  pub(crate) inner: AnyValue<'static>,
}

pub type AnyValueRet = jlrs::data::managed::ccall_ref::CCallRefRet<polars_value_t>;
pub type AnyValueRef<'scope> = jlrs::data::managed::ccall_ref::CCallRef<'scope, AnyValueValue<'scope, 'static>>;
pub type AnyValueValue<'scope, 'data> = TypedValue<'scope, 'data, polars_value_t>;

impl polars_value_t {
  pub fn dtype(&self) -> ValueTypeRet {
    leak_value(polars_value_type_t { inner: self.inner.dtype() })
  }

  pub fn extract(&self) -> JlrsResult<ValueRet> {
    use jlrs::convert::into_julia::IntoJulia;

    match weak_handle!() {
      Ok(handle) => {
        fn jl_value<'s, 'd, T: IntoJulia + ConstructType>(handle: impl Target<'s>, v: T) -> ValueRet {
          Value::new(&handle, v).leak()
        }
        let jl_str = |s: &str| unsafe { JuliaString::new(&handle, s).as_value() }.leak();
        match &self.inner {
          AnyValue::Null => Ok(Value::nothing(&handle).leak()),
          AnyValue::Boolean(v) => Ok(jl_value(&handle, *v)),
          AnyValue::String(v) => Ok(jl_str(v)),
          AnyValue::StringOwned(v) => Ok(jl_str(v)),
          AnyValue::UInt8(v) => Ok(jl_value(&handle, *v)),
          AnyValue::UInt16(v) => Ok(jl_value(&handle, *v)),
          AnyValue::UInt32(v) => Ok(jl_value(&handle, *v)),
          AnyValue::UInt64(v) => Ok(jl_value(&handle, *v)),
          AnyValue::Int8(v) => Ok(jl_value(&handle, *v)),
          AnyValue::Int16(v) => Ok(jl_value(&handle, *v)),
          AnyValue::Int32(v) => Ok(jl_value(&handle, *v)),
          AnyValue::Int64(v) => Ok(jl_value(&handle, *v)),
          AnyValue::Float32(v) => Ok(jl_value(&handle, *v)),
          AnyValue::Float64(v) => Ok(jl_value(&handle, *v)),
          #[cfg(feature = "dtype-date")]
          AnyValue::Date(v) => Ok(jl_date(&handle, *v)?),
          #[cfg(feature = "dtype-datetime")]
          AnyValue::Datetime(v, unit, tz) => {
            let tz = tz.map(|s| s.as_str());
            let unit = time_unit_as_str(unit);
            Ok(jl_datetime(&handle, *v, unit, tz)?)
          },
          #[cfg(feature = "dtype-datetime")]
          AnyValue::DatetimeOwned(v, unit, tz) => {
            let tz = tz.as_ref().map(|s| s.as_str());
            let unit = time_unit_as_str(unit);
            Ok(jl_datetime(&handle, *v, unit, tz)?)
          },
          #[cfg(feature = "dtype-time")]
          AnyValue::Time(v) => Ok(jl_time(&handle, *v)?),
          #[cfg(feature = "dtype-duration")]
          AnyValue::Duration(v, unit) => Ok(jl_period(&handle, *v, time_unit_as_str(unit))?),
          _ => {
            let tag = as_tag_str(&self.inner);
            Err(JuliaPolarsError::UnsupportedAnyValue(tag))?
          },
        }
      }
      Err(_) => JuliaPolarsError::WeakHandleError("polars_value_t::extract").panic(),
    }
  }
}

pub fn as_tag_str(value: &AnyValue) -> &'static str {
  match value {
    AnyValue::Null => "Null",
    AnyValue::Boolean(_) => "Boolean",
    AnyValue::String(_) => "String",
    AnyValue::StringOwned(_) => "StringOwned",
    AnyValue::UInt8(_) => "UInt8",
    AnyValue::UInt16(_) => "UInt16",
    AnyValue::UInt32(_) => "UInt32",
    AnyValue::UInt64(_) => "UInt64",
    AnyValue::Int8(_) => "Int8",
    AnyValue::Int16(_) => "Int16",
    AnyValue::Int32(_) => "Int32",
    AnyValue::Int64(_) => "Int64",
    AnyValue::Int128(_) => "Int128",
    AnyValue::Float32(_) => "Float32",
    AnyValue::Float64(_) => "Float64",
    #[cfg(feature = "dtype-date")]
    AnyValue::Date(_) => "Date",
    #[cfg(feature = "dtype-datetime")]
    AnyValue::Datetime(..) => "Datetime",
    #[cfg(feature = "dtype-datetime")]
    AnyValue::DatetimeOwned(..) => "DatetimeOwned",
    #[cfg(feature = "dtype-time")]
    AnyValue::Time(_) => "Time",
    #[cfg(feature = "dtype-duration")]
    AnyValue::Duration(..) => "Duration",
    AnyValue::Binary(_) => "Binary",
    AnyValue::BinaryOwned(_) => "BinaryOwned",
    AnyValue::List(_) => "List",
    #[cfg(feature = "dtype-array")]
    AnyValue::Array(..) => "Array",
    #[cfg(feature = "dtype-struct")]
    AnyValue::Struct(..) => "Struct",
    #[cfg(feature = "dtype-struct")]
    AnyValue::StructOwned(..) => "StructOwned",
    #[cfg(feature = "dtype-categorical")]
    AnyValue::Categorical(..) => "Categorical",
    #[cfg(feature = "dtype-categorical")]
    AnyValue::CategoricalOwned(..) => "CategoricalOwned",
    #[cfg(feature = "dtype-categorical")]
    AnyValue::Enum(..) => "Enum",
    #[cfg(feature = "dtype-categorical")]
    AnyValue::EnumOwned(..) => "EnumOwned",
    #[cfg(feature = "dtype-decimal")]
    AnyValue::Decimal(..) => "Decimal",
    // _ => "Unknown",
  }
}

fn jl_datetime<'scope>(handle: &impl Target<'scope>, s: i64, unit: &'static str, tz: Option<&str>) -> JlrsResult<ValueRet> {
  let s = unsafe { Value::new(handle, s).as_value() };
  let unit = Symbol::new(handle, unit).as_value();
  let tz = match tz {
    Some(tz) => unsafe { JuliaString::new(handle, tz).as_value() },
    None => Value::nothing(handle),
  };
  let _jl_datetime = inline_static_ref!(JL_DATETIME_FUNCTION, Value, "Polars.FFI._jl_datetime", handle);
  match unsafe { _jl_datetime.call(&handle, [s, unit, tz]) } {
    Ok(v) => Ok(v.leak()),
    Err(e) => Err(JuliaPolarsError::function_call("_jl_datetime", e))?,
  }
}

fn jl_date<'scope>(handle: &impl Target<'scope>, d: i32) -> JlrsResult<ValueRet> {
  let d = unsafe { TypedValue::new(handle, d).as_value() };
  let _jl_date = inline_static_ref!(JL_DATE_FUNCTION, Value, "Polars.FFI._jl_date", handle);
  match unsafe { _jl_date.call(handle, [d]) } {
    Ok(v) => Ok(v.leak()),
    Err(e) => Err(JuliaPolarsError::function_call("_jl_date", e))?,
  }
}

fn jl_time<'scope>(handle: &impl Target<'scope>, t: i64) -> JlrsResult<ValueRet> {
  let t = unsafe { TypedValue::new(handle, t).as_value() };
  let _jl_time = inline_static_ref!(JL_TIME_FUNCTION, Value, "Polars.FFI._jl_time", handle);
  match unsafe { _jl_time.call(handle, [t]) } {
    Ok(v) => Ok(v.leak()),
    Err(e) => Err(JuliaPolarsError::function_call("_jl_time", e))?,
  }
}

fn jl_period<'scope>(handle: &impl Target<'scope>, d: i64, unit: &'static str) -> JlrsResult<ValueRet> {
  let d = unsafe { TypedValue::new(handle, d).as_value() };
  let unit = Symbol::new(handle, unit).as_value();
  let _jl_period = inline_static_ref!(JL_PERIOD_FUNCTION, Value, "Polars.FFI._jl_period", handle);
  match unsafe { _jl_period.call(handle, [d, unit]) } {
    Ok(v) => Ok(v.leak()),
    Err(e) => Err(JuliaPolarsError::function_call("_jl_period", e))?,
  }
}
