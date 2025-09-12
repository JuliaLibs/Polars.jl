module FFI

import Dates
_jl_datetime(s, ns, time_zone=nothing) = Dates.unix2datetime(s + ns/1000)


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
