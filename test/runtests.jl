# rm -rf ~/.julia/compiled/v1.11/Polars && cargo build && julia --project -e 'using Test; include("test/runtests.jl")'
using Polars, Test

@testset "Basic tests" begin
  println("Polars version: ", Polars.version())
  @test Polars.version() != ""
  err = Polars.polars_error_t("hello")
  println(err)
  @test Polars.message(err) == "hello"
end

@testset "DataFrame tests" begin
  df = Polars.polars_dataframe_t()
  @test Polars.height(df) == 0
  @test size(df) == 0
  Polars.write_parquet(df, "test.parquet")
  df2 = Polars.read_parquet("test.parquet")
  @test Polars.height(df2) == 0
  show(df2)
end

@testset "Column tests" begin
  col = Polars.polars_column_t("mycol")
  @test size(col) == 0
  df = Polars.polars_dataframe_t([col])
  @test Polars.height(df) == 0
  show(df)
end
