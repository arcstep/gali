## 从数据框推断 ----
test_that("原子类型推断", {
  ## 从对象
  schema_name(T) |> testthat::expect_equal("bool")
  schema_name(1) |> testthat::expect_equal("numeric")
  schema_name(function() return(1)) |> testthat::expect_equal("func")
  schema_name(expression(1+1)) |> testthat::expect_equal("expr")
  
  ## 从列表
  schema_from_tibble(tibble(
    a = c(TRUE),
    b = 1L,
    c = 3.14,
    d = "ABC"
  ))$fieldType |>
    testthat::expect_identical(c("bool", "integer", "numeric", "string"))
})

test_that("<fs>类型推断", {
  schema_from_tibble(fs::dir_info(tempdir()))$fieldType[1:4] |>
    testthat::expect_identical(c("path", "enum", "bytes", "permission"))
})

test_that("<lubridate>类型推断", {
  schema_from_tibble(tibble(
    a = now(),
    b = as_date(now()),
    c = as_datetime(now()),
    d = as_date("2022-1-1") - as_date("2021-1-1")
  ))$fieldType |>
    testthat::expect_identical(c("datetime", "date", "datetime", "difftime"))
})

test_that("<schema_from_meta> 从元数据转换shcema", {
  schema_from_meta(meta = list(
    list(fieldName = "a", fieldType = "bool"),
    list(fieldName = "b", fieldType = "integer"),
    list(fieldName = "c", fieldType = "integer"),
    list(fieldName = "d", fieldType = "string")
  ))$rType |>
    testthat::expect_identical(c("logical", "integer", "integer", "character"))
  
  #
  schema_from_meta(meta = list(
    list(fieldName = "a", fieldType = "date"),
    list(fieldName = "b", fieldType = "datetime"),
    list(fieldName = "c", fieldType = "enum"),
    list(fieldName = "d", fieldType = "string")
  ))$rType |>
    testthat::expect_identical(c("Date", "POSIXct", "factor", "character"))
})

test_that("<schema_to_meta> 从schema生成元数据", {
  schema_to_meta(
    tibble(
      fieldName = c("a", "b", "c"),
      fieldType = c("bool", "integer", "string"),
    )
  ) |> testthat::expect_identical(list(
    list(fieldName = "a", fieldType = "bool"),
    list(fieldName = "b", fieldType = "integer"),
    list(fieldName = "c", fieldType = "string")))
})

## 按schema转换数据框 ----
test_that("<schema_to_tibble> 一般类型", {
  resp <- schema_to_tibble(
    tibble(
      a = 1:5,
      b = c("A", "B", "C", "D", "F"),
      c = 1671321600L,
    ),
    bySchema = schema_from_meta(list(
      list(fieldName = "a", fieldType = "integer"),
      list(fieldName = "b", fieldType = "string"),
      list(fieldName = "c", fieldType = "datetime"))))
  resp$c |>
    testthat::expect_identical(rep(c(as_datetime("2022-12-18")), nrow(resp)))
})

## 数据框要符合schema限定 ----
test_that("<schema_check> 检查数据框是否适合写入数据集", {
  tibble(a = 1:3, b = c(1,2,3)) |>
    schema_check() |>
    is_tibble() |>
    testthat::expect_true()
  
  ##
  tibble(a = 1:3, b = list(1,2,3)) |>
    schema_check() |>
    testthat::expect_error("Schema Check Failed")
  
  ##
  schema_from_tibble(tibble(
    a = c(TRUE),
    b = 1L,
    c = list(v = 3.14),
    d = "ABC")) |>
    testthat::expect_error("Schema Check Failed")
})

## 按schema读写arrow文件 ----
test_that("读写一般类型", {
  f <- tempdir()
  remove_dir(f)
  
  d <- tibble(a = 1:3)
  ds <- schema_from_tibble(d) |> schema_to_arrow()
  
  d |>
    schema_check() |>
    arrow::write_dataset(f, format = "parquet")
  x <- arrow::open_dataset(f) |> collect(schema = ds)
  x |> nrow() |> testthat::expect_equal(nrow(d))
})

test_that("读写<fs>类型", {
  f1 <- tempdir()
  remove_dir(f1)
  fs::dir_create(f1)
  fs::file_touch(fs::path_join(c(f1, "1.txt")))
  d <- fs::dir_info(f1)
  ds <- schema_from_tibble(d) |> schema_to_arrow()
  
  f <- tempdir()
  remove_dir(f)
  d |>
    schema_check() |>
    mutate(path = as.character(path)) |>
    mutate(size = as.integer(size)) |>
    arrow::write_dataset(f, format = "parquet")
  x <- arrow::open_dataset(f) |>
    collect(schema = ds) |>
    schema_to_tibble(schema_from_tibble(d))
  "fs_path" %in% class(x$path) |> testthat::expect_true()
  "fs_bytes" %in% class(x$size) |> testthat::expect_true()
})

test_that("读写<lubridate>类型", {
  d <- tibble(
    a = now(),
    b = as_date(now()),
    c = as_datetime(now()),
    d = as_date("2022-1-1") - as_date("2021-1-1"),
    `@lasteModifiedAt` = lubridate::now(tzone = "Asia/Shanghai")
  )
  ds <- schema_from_tibble(d) |> schema_to_arrow()
  
  f <- tempdir()
  remove_dir(f)
  d |>
    schema_check() |>
    arrow::write_dataset(f, format = "parquet")
  x <- arrow::open_dataset(f, schema = ds) |>
    collect() |>
    schema_to_tibble(schema_from_tibble(d))
  "Date" %in% class(x$b) |> testthat::expect_true()
  "POSIXct" %in% class(x$a) |> testthat::expect_true()
  "difftime" %in% class(x$d) |> testthat::expect_true()
})

test_that("读写<duration>和<difftime>类型", {
  d1 <- tibble(
    t = as_date("2022-1-1") - as_date("2021-1-1"),
    `@lasteModifiedAt` = lubridate::now(tzone = "Asia/Shanghai")
  )
  d2 <- d1 |> mutate(t |> as.duration() |> as.difftime())
  ds <- schema_from_tibble(d2) |> schema_to_arrow()
  
  f <- tempdir()
  remove_dir(f)
  d2 |>
    schema_check() |>
    arrow::write_dataset(f, format = "parquet")
  x <- arrow::open_dataset(f, schema = ds) |>
    collect()
  "difftime" %in% class(x$t) |> testthat::expect_true()
})


