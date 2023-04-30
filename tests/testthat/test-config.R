library(dplyr)
library(tibble)

test_that("加载YAML配置文件", {
  p <- tempdir()
  config_init(p)
  config_load(p)
  get_path("IMPORT") |>
    fs::path_file() |>
    testthat::expect_equal("IMPORT")
  get_path("CACHE") |>
    fs::path_file() |>
    testthat::expect_equal("CACHE")
  
  remove_dir(p)
})

test_that("补充配置项", {
  p <- tempdir()
  fs::dir_create(p)
  config_init(p, option = list("RISK" = "./RISK"))
  config_load(p)
  
  get_path("RISK") |>
    fs::path_file() |>
    testthat::expect_equal("RISK")
  
  ## 补写
  config_write(p, option = list("ABC" = "./DEF"))
  get_path("ABC") |>
    fs::path_file() |>
    testthat::expect_equal("DEF")
  
  remove_dir(p)
})
