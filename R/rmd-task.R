#' @title RMD脚本路径
#' @family rmd scripts functions
#' @export
task_path <- function(rmdName, type = c("rmd", "output", "script"), topic = "TASK") {
  switch(
    type[[1]],
    "rmd" = get_path(topic, paste0(rmdName, ".Rmd")),
    "output" = get_path(topic, "_output", rmdName),
    "script" = get_path(topic, "_scripts", paste0(rmdName, ".R")),
    stop("Unkonwn RMD Script Type: ", type[[1]])
  )
}

#' @title RMD脚本清单
#' @family rmd scripts functions
#' @export
task_all <- function(type = c("rmd", "output", "script"), topic = "TASK") {
  switch(
    type[[1]],
    "rmd" = fs::dir_info(get_path(topic), recurse = T, type = "file", glob = "*.Rmd"),
    "output" = fs::dir_info(get_path(topic, "_output"), recurse = T, type = "file", glob = "*.html"),
    "script" = fs::dir_info(get_path(topic, "_scripts"), recurse = T, type = "file", glob = "*.R"),
    stop("Unkonwn RMD Script Type: ", type[[1]])
  ) |>
    mutate(taskName = stringr::str_remove_all(path, glue("{get_path(topic)}/|.Rmd$|.R$|.html$|.htm$"))) |>
    select(taskName, size, modification_time, path) |>
    rename(lastModifiedAt = modification_time)
}

#' @title 加载RMD文件为多个段落
#' @family rmd scripts functions
#' @export
task_load <- function(rmdName, topic = "TASK") {
  lastFlag <- "md"
  lines <- task_path(rmdName, "rmd", topic = topic) |>
    readLines()
  
  flags <- lines |>
    stringr::str_match_all("(^---)|(^```\\s*\\{)|(^```\\s*$)") |>
    purrr::map_chr(function(x) {
      if(length(x) > 0) {
        if(!is.na(x[[2]])) {
          if(lastFlag == "md") {
            ## meta-begin
            lastFlag <<- "YM"
          } else {
            ## meta-end
            lastFlag <<- id_gen("MD-")
          }
          "YM"
        } else if(!is.na(x[[3]])) {
          ## script-begin
          lastFlag <<- id_gen("SC-")
          lastFlag
        } else if(!is.na(x[[4]])) {
          ## script-end
          preFlag <- lastFlag
          lastFlag <<- id_gen("MD-")
          preFlag
        } else {
          NA
        }
      } else {
        lastFlag
      }
    })
  
  tibble(id = flags, content = lines) |>
    nest_by(id) |>
    mutate(code = purrr::map_chr(data, function(x) x |> paste(collapse = "\n"))) |>
    select(-data) |>
    ungroup() |>
    mutate(id = factor(id, levels = unique(flags))) |>
    arrange(id) |>
    mutate(type = stringr::str_sub(id, 1, 2)) |>
    select(type, id, code)
}

#' @title 加载RMD文件的Yaml元数据
#' @family rmd scripts functions
#' @export
task_yaml <- function(rmdName = NULL, contents = "", topic = "TASK") {
  if(!is.null(rmdName)) {
    contents <- task_load(rmdName, topic)
  }
  yml <- contents$code[[1]] |> stringr::str_sub(4, -4)
  yaml::read_yaml(text = yml)
}

#' @title 直接运行RMD文件包含的有效脚本
#' @family rmd scripts functions
#' @export
task_render <- function(rmdName, topic = "TASK", pure = FALSE, quiet = TRUE) {
  rmdPath <- task_path(rmdName, "rmd", topic)
  scriptPath <- task_path(rmdName, "script", topic)
  outputDir <- task_path(rmdName, "output", topic)
  
  ## render pure script
  create_dir(scriptPath |> fs::path_dir())
  knit(rmdPath, scriptPath, tangle = T, quiet = quiet)
  
  ## render output
  if(!pure) {
    create_dir(outputDir)
    rmarkdown::render(rmdPath, output_dir = outputDir, quiet = quiet)
  }
}

#' @title 直接运行RMD文件包含的有效脚本
#' @family rmd scripts functions
#' @export
task_run <- function(rmdName, topic = "TASK") {
  scriptPath <- task_path(rmdName, "script", topic)
  if(!fs::file_exists(scriptPath)) {
    task_render(rmdName, topic, pure = T)
  }
  callr::r(function(p) source(p), args = list(p = scriptPath))
}

#' @title 保存RMD脚本
#' @family rmd scripts functions
#' @export
task_save <- function(newParams = list(), rmdName, topic = "TASK") {
  contents <- task_load(rmdName, topic)
  ##
  yml <- task_yaml(contents = contents)
  meta <- purrr::list_modify(yml, params = newParams) |> yaml::as.yaml()
  newContents <- rbind(
    tibble(
      type = "YM",
      id = "YM",
      code = paste("---", meta, "---", sep = "\n")),
    contents |> slice(-1))
  ## write to Rmd
  writeLines(newContents$code, con = task_path(rmdName, "rmd", topic = topic))
}

