# rm -rf ~/.julia/compiled/v1.11/Polars && cargo build && julia --project -e 'using Test; include("test/runtests.jl")'
using Polars, Test, JlrsCore, Dates

@testset "Basic tests" begin
  println("Polars version: ", Polars.version())
  @test Polars.version() == "0.50.0"
end

@testset "DataFrame tests" begin
  df = Polars.DataFrame()
  @test Polars.height(df) == 0
  @test size(df) == 0
  Polars.write_parquet(df, "test_empty.parquet")
  df2 = Polars.read_parquet("test_empty.parquet")
  @test Polars.height(df2) == 0
  show(df2)
end

@testset "DataFrame with data tests" begin
  df = Polars.read_parquet("test.parquet")
  @test Polars.height(df) == 3
  show(df)
  function test_col(name, expected_dtype)
    col = df[name]
    @test Polars.name(col) == name
    @test Polars.size(col) == 3
    dtype = Polars.dtype(col)
    @test typeof(dtype) == expected_dtype
  end
  test_col("col_null", Polars.DataTypes.Null)
  test_col("col_bool", Polars.DataTypes.Boolean)
  test_col("col_int8", Polars.DataTypes.Int8)
  test_col("col_int16", Polars.DataTypes.Int16)
  test_col("col_int32", Polars.DataTypes.Int32)
  test_col("col_int64", Polars.DataTypes.Int64)
  test_col("col_uint8", Polars.DataTypes.UInt8)
  test_col("col_uint16", Polars.DataTypes.UInt16)
  test_col("col_uint32", Polars.DataTypes.UInt32)
  test_col("col_uint64", Polars.DataTypes.UInt64)
  test_col("col_float32", Polars.DataTypes.Float32)
  test_col("col_float64", Polars.DataTypes.Float64)
  test_col("col_decimal", Polars.DataTypes.Decimal{9, 3})
  test_col("col_datetime", Polars.DataTypes.DateTime{:μs})
  test_col("col_date", Polars.DataTypes.Date)
  test_col("col_time", Polars.DataTypes.Time{:μs})
  test_col("col_duration", Polars.DataTypes.Duration{:μs})
  test_col("col_list_int32", Polars.DataTypes.List{Polars.DataTypes.Int32})
  test_col("col_array_float64", Polars.DataTypes.Array{Polars.DataTypes.Float64, 1})
end

@testset "Column tests" begin
  col = Polars.Column("mycol")
  @test Polars.name(col) == "mycol"
  @test size(col) == 0
  @test Polars.null_count(col) == 0
  dtype = Polars.dtype(col)
  println("Column dtype: ", dtype)
  df = Polars.DataFrame([col])
  @test Polars.height(df) == 0
  show(df)
  col = Polars.get_column(df, "mycol")
  @test Polars.name(col) == "mycol"
  dtype = Polars.dtype(col)
  println("Column dtype: ", dtype)
  @test_throws JlrsCore.JlrsError Polars.get_column(df, "nonexistent")
end

@testset "DataType tests" begin
  col = Polars.Column("mycol")
  dtype = Polars.dtype(col)
  println("Column dtype: ", dtype)
  # @test typeof(dtype) == Polars.DataType
  println(Polars.DataTypes.Decimal{10, 2}())
end

@testset "AnyValue tests" begin
  df = Polars.read_parquet("test.parquet")
  @test Polars.height(df) == 3
  @test getindex.(Ref(df["col_null"]), 1:3) == [nothing, nothing, nothing]
  @test getindex.(Ref(df["col_bool"]), 1:3) == [true, false, true]
  @test getindex.(Ref(df["col_int32"]), 1:3) == [1, 2, 3]
  @test getindex.(Ref(df["col_float64"]), 1:3) == [1.0, 2.0, 3.0]
  # @test getindex.(Ref(df["col_decimal"]), 1:3) == [Decimal(1, 0), Decimal(2, 0), Decimal(3, 0)]
  @test getindex.(Ref(df["col_string"]), 1:3) == ["a", "b", "c"]
  @test getindex.(Ref(df["col_datetime"]), 1:3) == [DateTime(2023, 1, 1), DateTime(2023, 1, 2), DateTime(2023, 1, 3)]
  @test getindex.(Ref(df["col_date"]), 1:3) == [Date(2023, 1, 1), Date(2023, 1, 2), Date(2023, 1, 3)]
  @test getindex.(Ref(df["col_time"]), 1:3) == [Time(12, 0), Time(13, 0), Time(14, 0)]
  @test getindex.(Ref(df["col_duration"]), 1:3) == [Microsecond(1000), Microsecond(2000), Microsecond(3000)]
  col = df["col_null"]
  @test_throws JlrsCore.JlrsError col[4]
end
