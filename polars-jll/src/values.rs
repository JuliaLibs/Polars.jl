use polars::prelude::*;
use jlrs::{data::{managed::{ccall_ref::{CCallRef, CCallRefRet}, value::{typed::TypedValue, ValueRet}}, types::construct_type::ConstructType}, error::JlrsError, inline_static_ref, prelude::*, weak_handle};

use crate::{polars_value_type_t, utils::leak_value, ValueTypeRet};

#[derive(Debug, OpaqueType)]
#[allow(non_camel_case_types)]
pub struct polars_value_t {
  pub(crate) inner: AnyValue<'static>,
}

pub type AnyValueRet = CCallRefRet<polars_value_t>;
pub type AnyValueRef<'data> = CCallRef<'data, polars_value_t>;
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
          unsafe { TypedValue::new(&handle, v).as_value() }.leak()
        }
        let jl_str = |s: &str| unsafe { JuliaString::new(&handle, s).as_value() }.leak();
        let jl_datetime = |s: i64, ns: u64, tz: Option<&str>| {
          let s = unsafe { TypedValue::new(&handle, s).as_value() };
          let ns = unsafe { TypedValue::new(&handle, ns).as_value() };
          let tz = match tz {
            Some(tz) => unsafe { JuliaString::new(&handle, tz).as_value() },
            None => Value::nothing(&handle),
          };
          let _jl_datetime = inline_static_ref!(JL_DATETIME_FUNCTION, Value, "Polars.FFI._jl_datetime", handle);
          match unsafe { _jl_datetime.call(&handle, [s, ns, tz]) } {
            Ok(v) => Ok(v.leak()),
            Err(e) => Err(JlrsError::exception(format!("Error calling _jl_datetime: {:?}", e))),
          }
        };
        match &self.inner {
          AnyValue::Null => Ok(Value::nothing(&handle).leak()),
          AnyValue::Boolean(v) => Ok(jl_value(&handle, *v)),
          AnyValue::String(v) => Ok(jl_str(v)),
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
          AnyValue::Date(v) => Ok(jl_value(&handle, *v)),
          AnyValue::Datetime(v, unit, tz) => {
            let (s, ns) = match unit {
              polars::prelude::TimeUnit::Milliseconds => (*v / 1_000, (*v % 1_000) * 1_000_000),
              polars::prelude::TimeUnit::Microseconds => (*v / 1_000_000, (*v % 1_000_000) * 1_000),
              polars::prelude::TimeUnit::Nanoseconds => (*v / 1_000_000_000, (*v % 1_000_000_000)),
            };
            let tz = tz.map(|s| s.as_str());
            Ok(jl_datetime(s, ns as _, tz)?)
          },
          AnyValue::DatetimeOwned(v, unit, tz) => {
            let (s, ns) = match unit {
              polars::prelude::TimeUnit::Milliseconds => (*v / 1_000, (*v % 1_000) * 1_000_000),
              polars::prelude::TimeUnit::Microseconds => (*v / 1_000_000, (*v % 1_000_000) * 1_000),
              polars::prelude::TimeUnit::Nanoseconds => (*v / 1_000_000_000, (*v % 1_000_000_000)),
            };
            let tz = tz.as_ref().map(|s| s.as_str());
            Ok(jl_datetime(s, ns as _, tz)?)
          },
          AnyValue::Time(v) => Ok(jl_value(&handle, *v)),
          AnyValue::Duration(v, _) => Ok(jl_value(&handle, *v)),
          _ => {
            Err(JlrsError::exception("Cannot convert complex Polars value to Julia value"))?
          },
        }
      }
      Err(_) => panic!("Could not create weak handle to Julia."),
    }
  }
}
