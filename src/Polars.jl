module Polars

using JlrsCore.Wrap
# using libpolars_jll
# export libpolars_jll

const libpolars_local = joinpath(@__DIR__, "../target/debug/libpolars.dylib")
@static if isfile(libpolars_local)
  const libpolars = libpolars_local
end
@wrapmodule(libpolars, :julia_module_polars_init_fn)

function __init__()
  @initjlrs
end

end # module Polars
