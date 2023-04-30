library(dplyr, warn.conflicts = F)
library(tibble, warn.conflicts = F)

test_that("写入一个简单的文件", {
  sample_init()
  
  list("dsName" = "车数据") |> ds_meta_write(dsName = "车数据")
  ds_meta("车数据")$dsName |> testthat::expect_equal("车数据")
  
  list("partColumns" = c("cyl", "am")) |> ds_meta_write(dsName = "车数据")
  x <- ds_meta("车数据") |> names()
  testthat::expect_equal(c("dsName", "partColumns") %in% x, c(T, T))
  
  list("dsName" = "我的车") |> ds_meta_write(dsName = "车数据")
  ds_meta("车数据")$dsName |> testthat::expect_equal(c("我的车"))
  
  sample_remove()
})

test_that("按<bySchema>写入数据", {
  sample_init()
  
  ds_init(
    "MY_RISK_DATA",
    bySchema = list(
      list(fieldName = "数据来源", fieldType = "string"),
      list(fieldName = "来源ID",   fieldType = "string"),
      list(fieldName = "模型名称", fieldType = "string"),
      list(fieldName = "模型类别", fieldType = "string")
    ),
    keyColumns = "来源ID",
    partColumns = c("@action", "模型类别", "模型名称")
  )
  
  ## 补充5个系统字段
  ds_schema("MY_RISK_DATA") |> nrow() |> testthat::expect_equal(9)
})

test_that("按<样本数据>写入数据", {
  sample_init()
  
  ds_init(
    "MY_CARS",
    data = mtcars
  )
  
  ## 自动选择@action为根分区
  ds_meta("MY_CARS")$partColumns |> testthat::expect_equal("@action")
  ## 补充5个系统字段
  nrow(ds_schema("MY_CARS")) |> testthat::expect_equal(ncol(mtcars) + 5)
})
