module Polars

include("ffi.jl")

export version

version()::String = FFI.polars_version()

end # module Polars
