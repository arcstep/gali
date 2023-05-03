test_that("读取任务元数据", {
  sample_init()
  contents <- task_load("1-A")
  yml <- contents$code[[1]] |> stringr::str_sub(4, -4)
  meta <- yaml::read_yaml(text = yml)
  meta$title |> testthat::expect_equal("My ABC")

  sample_remove()
})

test_that("修改参数后保存RMD任务文件", {
  sample_init()
  task_yaml("1-A")$params$a |> testthat::expect_equal(3)
  # purrr::list_modify(task_yaml("1-A"), params = list(c = 5)) |> str()
  task_save(list(a = 7L), "1-A")
  task_yaml("1-A")$params$a |> testthat::expect_equal(7L)
  
  sample_remove()
})

test_that("运行任务脚本", {
  sample_init()
  task_render("1-A", quiet = T)
  task_run("1-A")$value |> testthat::expect_equal(6L)

  sample_remove()
})

test_that("使用source执行外部R源文件", {
  sample_init()
  
  ## 使得Rmd文件能够找到rootPath文件夹
  task_save(list("rootPath" = rootPath), "2-C")
  ##
  c("2-B", "2-C") |> purrr::walk(~ task_render(.x))
  task_run("2-C")$value |> testthat::expect_equal("lele XX")

  sample_remove()
})

