test_that("计算工作日", {
  resp <- tribble(
    ~x, ~y,
    "2022-1-1", "2022-1-10",
    "2022-2-1", "2022-2-10",
    "2022-3-1", "2022-3-10",
    "2022-4-1", "2022-4-10",
    "2022-5-1", "2022-5-10",
    "2022-6-1", "2022-6-10",
    "2022-7-1", "2022-7-10",
    "2022-8-1", "2022-8-10",
    "2022-9-1", "2022-9-10",
    "2022-10-1", "2022-10-10",
    "2022-11-1", "2022-11-10",
    "2022-12-1", "2022-12-10") |>
    ds_add_workdays("x", "y")
  resp$`@workdays`[[1]] |> testthat::expect_equal(5)
  resp$`@workdays`[[2]] |> testthat::expect_equal(4)
  resp$`@workdays`[[3]] |> testthat::expect_equal(8)
  resp$`@workdays`[[4]] |> testthat::expect_equal(5)
  resp$`@workdays`[[5]] |> testthat::expect_equal(5)
  resp$`@workdays`[[6]] |> testthat::expect_equal(7)
  resp$`@workdays`[[7]] |> testthat::expect_equal(6)
  resp$`@workdays`[[8]] |> testthat::expect_equal(8)
  resp$`@workdays`[[9]] |> testthat::expect_equal(7)
  resp$`@workdays`[[10]] |> testthat::expect_equal(3)
  resp$`@workdays`[[11]] |> testthat::expect_equal(8)
  resp$`@workdays`[[12]] |> testthat::expect_equal(7)
})

test_that("计算工作日: 重命名", {
  resp <- tribble(
    ~x, ~y,
    "2022-1-1", "2022-1-10",
    "2022-2-1", "2022-2-10",
    "2022-3-1", "2022-3-10",
    "2022-4-1", "2022-4-10",
    "2022-5-1", "2022-5-10",
    "2022-12-1", "2022-12-10",
    "2022-1-1", "2022-1-1",
    "2022-1-10", "2022-1-10") |>
    ds_add_workdays("x", "y", "工作日数")
  resp$工作日数[[1]] |> testthat::expect_equal(5)
  resp$工作日数[[2]] |> testthat::expect_equal(4)
  resp$工作日数[[3]] |> testthat::expect_equal(8)
  resp$工作日数[[4]] |> testthat::expect_equal(5)
  resp$工作日数[[5]] |> testthat::expect_equal(5)
  resp$工作日数[[6]] |> testthat::expect_equal(7)
  resp$工作日数[[7]] |> testthat::expect_equal(0)
  resp$工作日数[[8]] |> testthat::expect_equal(1)
})
