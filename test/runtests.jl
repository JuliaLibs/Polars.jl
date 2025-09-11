# rm -rf ~/.julia/compiled/v1.11/Polars && cargo build && julia --project -e 'using Test; include("test/runtests.jl")'
using Polars, Test, JlrsCore

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
  col = df["decimal"]
  @test Polars.name(col) == "decimal"
  @test Polars.size(col) == 3
  println(Polars.dtype(col))
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
