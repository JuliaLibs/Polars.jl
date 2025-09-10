module Polars

include("ffi.jl")

export version
import .FFI: polars_error_t

version()::String = FFI.polars_version()

message(err::polars_error_t)::String = FFI.polars_error_message(err)

end # module Polars
