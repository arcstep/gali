#' @title 条件过滤：取反
#' @family utils function
#' @export
`%nin%` <- Negate(`%in%`)

#' @title 遇到空值填充默认值
#' @description 当值为NULL、空向量、空列表、空字符串时设置默认值
#' @family utils function
#' @export
`%empty%` <- function(a, b) {
  if (is_empty(a)) b else a
}

#' @title 遇到非空值填充默认值
#' @family utils function
#' @export
`%not-empty%` <- function(a, b) {
  if (!is_empty(a)) b else a
}

#' @title 删除文件夹
#' @family utils function
#' @export
remove_dir <- function(path) {
  if(fs::dir_exists(path)) {
    fs::dir_delete(path)
  }
}

#' @title 创建文件夹
#' @family utils function
#' @export
create_dir <- function(path) {
  if(!fs::dir_exists(path)) {
    fs::dir_create(path)
  }
}

batch.Env <- new.env()
batch.Env$seq <- 1L

gen_seq <- function() {
  batch.Env$seq <- (batch.Env$seq + 1) %% 1e6
  sprintf("%06d", batch.Env$seq)
}

#' @title 生成增量批次号
#' @description
#' 由于1秒内几乎不可能产生几十万个以上的批次，因此循环的seq和时间戳，可获得增量批次号
#' @family utils function
#' @export
id_gen <- function(prefix = "SN") {
  lubridate::now() |>
    as.character.Date(format = paste(prefix, "%Y-%m-%d-%H%M%S", sep = "")) |>
    paste(gen_seq(), sep = "-")
}

#' @title 解析批次号
#' @family utils function
#' @export
id_parse <- function(str) {
  str |>
    stringr::str_match("(\\w+)(\\d{4})-(\\d{2})-(\\d{2})-(\\d{6})-(\\d{6})") |>
    as.data.frame() |>
    as_tibble() |>
    rename(id = V1, prefix = V2, year = V3, month = V4, day = V5, time = V6, rand = V7)
}

#
warning_if_not_in <- function(e, s, label = "") {
  invalidElements <- e |> purrr::discard(~ .x %in% s)
  if(!rlang::is_empty(invalidElements)) {
    warning("<", invalidElements |> paste(collapse = ", "), "> ",
            label, " Not-In: [", s |> paste(collapse = ", "), "]")
  }
  return(e)
}

#
stop_if_not_in <- function(e, s, label = "") {
  invalidElements <- e |> purrr::discard(~ .x %in% s)
  if(!rlang::is_empty(invalidElements)) {
    stop("<", invalidElements |> paste(collapse = ", "), "> ",
         label, " Not-In: [", s |> paste(collapse = ", "), "]")
  }
}
