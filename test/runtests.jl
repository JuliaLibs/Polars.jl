# rm -rf ~/.julia/compiled/v1.11/Polars cargo build && julia --project -e 'using Test; include("test/runtests.jl")'
using Polars, Test

@testset "Basic tests" begin
  println("Polars version: ", Polars.version())
  @test Polars.version() != ""
  err = Polars.FFI.polars_error_t("hello")
  println(err)
  @test Polars.message(err) == "hello"
end
