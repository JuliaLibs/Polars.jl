module FFI

import Dates
_jl_datetime(s, unit, time_zone=nothing) = Dates.DateTime(1970) + _jl_period(s, unit)
_jl_date(d) = Dates.Date(1970) + Dates.Day(d)
_jl_time(t) = Dates.Time(0) + Dates.Nanosecond(t)
_jl_period(t, unit) = if unit === :ms
  Dates.Millisecond(t)
elseif unit === :μs
  Dates.Microsecond(t)
elseif unit === :ns
  Dates.Nanosecond(t)
end

using JlrsCore.Wrap
# using libpolars_jll
# export libpolars_jll

@static if Sys.isapple()
  const libpolars_soname = "libpolars.dylib"
elseif Sys.iswindows()
  const libpolars_soname = "libpolars.dll"
else
  const libpolars_soname = "libpolars.so"
end
const libpolars_local = joinpath(@__DIR__, "../target/debug", libpolars_soname)
@static if isfile(libpolars_local)
  const libpolars = libpolars_local
else
  # fallback to soname so the system loader can find the library on the library search path
  const libpolars = libpolars_soname
end
@wrapmodule(libpolars, :julia_module_polars_init_fn)

function __init__()
  @initjlrs
end

end # module FFI
