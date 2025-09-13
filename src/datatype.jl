module DataTypes

import ..FFI: polars_value_type_t

abstract type DataType end
struct Null <: DataType end
struct Boolean <: DataType end
struct Int8 <: DataType end
struct Int16 <: DataType end
struct Int32 <: DataType end
struct Int64 <: DataType end
struct UInt8 <: DataType end
struct UInt16 <: DataType end
struct UInt32 <: DataType end
struct UInt64 <: DataType end
struct Float32 <: DataType end
struct Float64 <: DataType end
struct Decimal{P, S} <: DataType end
# unit can be :ns, :μs, :ms
struct DateTime{U} <: DataType
  time_zone::Union{String, Nothing}
end
struct Date <: DataType end
# unit must be :μs
struct Time{U} <: DataType end
# unit can be :ns, :μs, :ms
struct Duration{U} <: DataType end
struct List{T<:DataType} <: DataType
  inner::T
end
struct Array{T<:DataType, N} <: DataType
  inner::T
end
struct Unknown <: DataType
  tag::Symbol
  inner::polars_value_type_t
end

Decimal(P, S)::DataType = Decimal(Int(P), Int(S))
Decimal(P::Int, S::Int)::DataType = Decimal{P, S}()
Array(dt::DataType, n::Int)::DataType = Array{typeof(dt), n}(dt)
Base.precision(_::Decimal{P, S}) where {P, S} = P
scale(_::Decimal{P, S}) where {P, S} = S
timeunit(::DateTime{U}) where {U} = U
timeunit(::Time{U}) where {U} = U
timeunit(::Duration{U}) where {U} = U
arraysize(::Array{T, N}) where {T<:DataType, N} = N

function DataType(sym::Symbol; kwargs...)::DataType
  if sym === :Null
    return Null()
  elseif sym === :Boolean
    return Boolean()
  elseif sym === :Int8
    return Int8()
  elseif sym === :Int16
    return Int16()
  elseif sym === :Int32
    return Int32()
  elseif sym === :Int64
    return Int64()
  elseif sym === :UInt8
    return UInt8()
  elseif sym === :UInt16
    return UInt16()
  elseif sym === :UInt32
    return UInt32()
  elseif sym === :UInt64
    return UInt64()
  elseif sym === :Float32
    return Float32()
  elseif sym === :Float64
    return Float64()
  elseif sym === :Decimal
    # TODO: default precision and scale
    precision = something(get(kwargs, :precision, nothing), 10)
    scale = something(get(kwargs, :scale, nothing), 2)
    println("Decimal: precision=$precision, scale=$scale")
    return Decimal(precision, scale)
  elseif sym === :Datetime
    unit = kwargs[:time_unit]
    time_zone = get(kwargs, :time_zone, nothing)
    return DateTime{unit}(time_zone)
  elseif sym === :Date
    return Date()
  elseif sym === :Time
    return Time{:μs}()
  elseif sym === :Duration
    unit = kwargs[:time_unit]
    return Duration{unit}()
  elseif sym === :List
    inner = convert(DataType, kwargs[:inner])
    return List(inner)
  elseif sym === :Array
    inner = convert(DataType, kwargs[:inner])
    n = convert(Int, kwargs[:size])
    return Array(inner, n)
  else
    throw(ArgumentError("Unimplemented data type symbol: $sym"))
  end
end
function type(dtype::DataType)::Symbol
  if dtype isa Null
    return :Null
  elseif dtype isa Boolean
    return :Boolean
  elseif dtype isa Int8
    return :Int8
  elseif dtype isa Int16
    return :Int16
  elseif dtype isa Int32
    return :Int32
  elseif dtype isa Int64
    return :Int64
  elseif dtype isa UInt8
    return :UInt8
  elseif dtype isa UInt16
    return :UInt16
  elseif dtype isa UInt32
    return :UInt32
  elseif dtype isa UInt64
    return :UInt64
  elseif dtype isa Float32
    return :Float32
  elseif dtype isa Float64
    return :Float64
  elseif dtype isa Decimal
    return :Decimal
  elseif dtype isa DateTime
    return :Datetime
  elseif dtype isa Date
    return :Date
  elseif dtype isa Time
    return :Time
  elseif dtype isa Duration
    return :Duration
  elseif dtype isa List
    return :List
  elseif dtype isa Array
    return :Array
  elseif dtype isa Unknown
    return dtype.tag
  else
    throw(ArgumentError("Unimplemented data type: $dtype"))
  end
end
function kwargs(dtype::DataType)::NamedTuple
  if dtype isa Decimal
    return (; precision=precision(dtype), scale=scale(dtype))
  elseif dtype isa DateTime
    return (; time_unit=timeunit(dtype), time_zone=dtype.time_zone)
  elseif dtype isa Time
    return (; time_unit=timeunit(dtype),)
  elseif dtype isa Duration
    return (; time_unit=timeunit(dtype),)
  elseif dtype isa List
    return (; inner=dtype.inner,)
  elseif dtype isa Array
    return (; inner=dtype.inner, size=arraysize(dtype),)
  elseif dtype isa Unknown
    return ()
  else
    return ()
  end
end

end # module DataTypes
