library(tibble, warn.conflicts = FALSE)
library(tidyr, warn.conflicts = FALSE)
library(dplyr, warn.conflicts = FALSE)
library(lubridate, warn.conflicts = FALSE)
library(gali, warn.conflicts = FALSE)

rootPath <- tempdir()
sample_dataset_init <- function() {
  ##
  ds_init(
    "student",
    type = "导入数据",
    data = tibble("name" = "adi", "age" = 5L))
  ##
  ds_init(
    "score",
    type = "导入数据",
    bySchema = list(
      list(fieldName = "name", fieldType = "string"),
      list(fieldName = "english", fieldType = "integer"),
      list(fieldName = "chinese", fieldType = "integer"),
      list(fieldName = "math", fieldType = "integer")))
}

sample_init <- function() {
  sample_remove()
  rootPath |> config_init()
  fs::dir_copy(testthat::test_path("_IMPORT"), get_path("IMPORT"), overwrite = TRUE)
  fs::dir_copy(testthat::test_path("_TASK"), get_path("TASK"), overwrite = TRUE)
  sample_dataset_init()
}

sample_remove <- function() {
  if(fs::dir_exists(rootPath)) rootPath |> fs::dir_delete()
}
