#' @title 定义数据过滤任务
#' @description
#' 这个任务的目标是通过配置项实现函数[dplyr::filter()]的大部分功能。
#'
#' 允许为数据集增加多个阈值查询条件，缩小筛查范围。
#'
#' taskId、column、op、value等参数构造唯一的gali_ds_filter表达式，
#' 这将允许从UI生成或还原该操作。
#'
#' 只要包括以下逻辑判断：
#' \itemize{
#'  \item >, <, >=, <=, ==, !=
#'  \item %in%, %nin%
#'  \item %regex%, %not-regex%
#'  \item #time# >, #time# <
#'  \item #date# >, #date# <
#' }
#'
#' @param column 列名
#' @param op 阈值范围判断符号
#' @param value 阈值
#' @param s_dataName 内存中的数据框名称，默认为 @result
#' @param taskTopic 任务定义的主题文件夹
#' @family ds function
#' @export
ds_filter <- function(d, column, op, value = list(NULL)) {
  ## 校验参数合法性
  if(stringr::str_detect(op, "(>|<|>=|<=|==|!=|%in%|%nin%|%regex%|%not-regex%)", negate = TRUE)) {
    stop("Invalid filter OP: ", op)
  }
  
  ## 创建任务表达式
  if(op %in% c(">", "<", ">=", "<=", "==", "!=", "%in%")) {
    d |> filter(do.call(!!sym(op), args = list(!!sym(column), unlist(value))))
  } else if(stringr::str_detect(op, "^[@#% ]*time[@#% ]+(>|<|>=|<=|==)[ ]*$")) {
    myop <- stringr::str_replace(op, "[@#%]?time[@#% ]+", "") |> stringr::str_trim()
    param <- list()
    x1 <- d[[column]] |> as_datetime(tz = "Asia/Shanghai")
    x2 <- unlist(value) |> as_datetime(tz = "Asia/Shanghai")
    d |> filter(do.call(myop, args = list(x1, x2)))
  } else if(stringr::str_detect(op, "^[@#% ]*date[@#% ]+(>|<|>=|<=|==)[ ]*$")) {
    myop <- stringr::str_replace(op, "[@#%]?date[@#% ]+", "") |> stringr::str_trim()
    param <- list()
    x1 <- d[[column]] |> as_date()
    x2 <- unlist(value) |> as_date()
    d |> filter(do.call(myop, args = list(x1, x2)))
  } else if(op %in% c("%nin%")) {
    ## 将 %nin% 转换为可以惰性执行的 %in%
    d |> filter(!do.call("%in%", args = list(!!sym(column), unlist(value))))
  } else if(op %in% c("%regex%", "%not-regex%")) {
    ## 正则表达式不能惰性执行，需要提前collect数据
    d |> collect() |>
      filter(do.call(!!sym(op), args = list(!!sym(column), unlist(value))))
  } else {
    stop("<gali_ds_filter> Unknown OP: ", op)
  }
}

#' @title 头部数据
#' @family ds function
#' @export
ds_head <- function(d, n = 10) {
  d |> head(n)
}

#' @title 尾部数据
#' @family ds function
#' @export
ds_tail <- function(d, n = 10) {
  d |> tail(n)
}

#' @title 按最大值取N条记录
#' @family ds function
#' @export
ds_n_max <- function(d, orderColumn, n = 10, with_ties = FALSE) {
  mydata <- d |> collect()
  mydata |>
    slice_max(order_by = mydata[[orderColumn]], n = n, with_ties = with_ties)
}

#' @title 按最小值取N条记录
#' @family ds function
#' @export
ds_n_min <- function(d, orderColumn, n = 10, with_ties = FALSE) {
  mydata <- d |> collect()
  mydata |>
    slice_min(order_by = mydata[[orderColumn]], n = n, with_ties = with_ties)
}

#' @title 行排序
#' @family ds function
#' @export
ds_arrange <- function(d, columns = list(), desc = FALSE, by_group = FALSE) {
  if(desc) {
    d |> arrange(!!!(syms(columns) |> purrr::map(~ desc(d[[.x]]))), .by_group = by_group)
  } else {
    d |> arrange(!!!syms(columns), .by_group = by_group)
  }
}

#' @title 选择列，支持惰性计算
#' @family ds function
#' @export
ds_select <- function(d, columns = list(), showOthers = FALSE, pattern = NULL, negate = FALSE) {
  if(length(columns) == 0) {
    if(negate) {
      d |>
        select(
          !matches(pattern %empty% "^mamaxiannichifanman$"),
          if(showOthers) everything() else NULL)
    } else {
      d |>
        select(
          matches(pattern %empty% "^mamaxiannichifanman$"),
          if(showOthers) everything() else NULL)
    }
  } else {
    if(negate) {
      d |>
        select(!contains(columns |> unlist()),
               !matches(pattern %empty% "^mamaxiannichifanman$"),
               if(showOthers) everything() else NULL)
    } else {
      d |>
        select(contains(columns |> unlist()),
               matches(pattern %empty% "^mamaxiannichifanman$"),
               if(showOthers) everything() else NULL)
    }
  }
}

#' @title 选择非空列
#' @description
#' 如果指定的行范围内，选择列值不全部为NA的列，在观察数据时可以快速移除无效列
#' @family ds function
#' @export
ds_select_not_na <- function(d, rows = NULL) {
  n <- rows %empty% 100
  if(nrow(d) < n) {
    n <- nrow(d)
  }
  ds <- d[sample(nrow(d), n),]
  columns <- names(ds) |>
    purrr::keep(function(columnName) {
      ds[[columnName]] |> purrr::some(~ !is.na(.x))
    })
  d |> select(!!!syms(columns))
}

#' @title 选择空值列
#' @description
#' 如果指定的行范围内，选择列值全部为NA的列
#' @family ds function
#' @export
ds_select_na <- function(d, rows = NULL) {
  num_rows <- rows %empty% 100
  if(nrow(d) < num_rows) {
    num_rows <- nrow(d)
  }
  ds <- d[sample(nrow(d), num_rows),]
  columns <- names(ds) |>
    purrr::discard(function(columnName) {
      ds[[columnName]] |> purrr::some(~ !is.na(.x))
    })
  d |> select(!!!syms(columns))
}

#' @title 按数据集中已定义的列名
#' @family ds function
#' @export
ds_select_by <- function(d, dsName, topic = "CACHE") {
  ## 读取数据集配置
  meta <- ds_meta(dsName, topic)
  if(is_empty(meta)) {
    stop("Empty Dataset Metadata!!!")
  }
  ## 缺少schema定义
  if(is_empty(meta$schema)) {
    stop("No Schema Defined!!!")
  }
  
  ## 选择字段
  validColumns <- c(
    names(d)[names(d) %in% (meta$schema |> purrr::map("fieldName") |> unlist())],
    names(d) |> purrr::keep(~ stringr::str_detect(.x, "^@"))
  ) |> unique()
  d |> select(!!!syms(validColumns))
}

#' @title 列改名
#' @family ds function
#' @export
ds_rename <- function(d, newName, oldName) {
  mydata <- d |> collect()
  names(mydata)[names(mydata) == oldName] <- newName
  mydata
}

#' @title 计数统计
#' @family ds function
#' @export
ds_count <- function(d, columns = c(), sort = TRUE, name = "n") {
  d |>
    select(columns) |>
    collect() |>
    count(!!!syms(columns), sort = sort, name = name)
}

#' @title 去重
#' @family ds function
#' @export
ds_distinct <- function(d, columns = c()) {
  d |>
    select(columns) |>
    collect() |>
    distinct(!!!syms(columns))
}

#' @title 去除重复行
#' @description
#' 按键值列去除重复行，保留最先发现的行
#' @param ds 要确认的数据集
#' @param keyColumns 要确认的列名或其向量、列表
#' @family ds function
#' @export
ds_as_unique <- function(ds, keyColumns = c()) {
  if(nrow(ds) > 0 && length(keyColumns) > 0) {
    ds[!duplicated(ds[,keyColumns]),]
  } else {
    ds
  }
}

#' @title 列值重复统计
#' @family ds function
#' @export
ds_dup_count <- function(d, columns = c(), dupColumns = c(), sort = TRUE, name = "n") {
  d |>
    select(c(columns, dupColumns)) |>
    collect() |>
    distinct(!!!syms(c(columns, dupColumns))) |>
    count(!!!syms(columns), sort = sort, name = name)
}

#' @title 计数统计，并将结果追加到原数据集
#' @family ds function
#' @export
ds_add_count <- function(d, columns = c(), sort = FALSE, name = "n") {
  d |> collect() |> add_count(!!!syms(columns), sort = sort, name = name)
}

#' @title 从日期列提取年
#' @family ds function
#' @export
ds_as_year <- function(d, dateColumnName, yearName = "year") {
  if(nrow(d) > 0) {
    d[[yearName]] <- d[[dateColumnName]] |> year() |> as.integer()
    d
  } else {
    d
  }
}

#' @title 从日期列提取月
#' @family ds function
#' @export
ds_as_month <- function(d, dateColumnName, monthName = "month") {
  if(nrow(d) > 0) {
    d[[monthName]] <- d[[dateColumnName]] |> month() |> as.integer()
    d
  } else {
    d
  }
}


#' @title 转换字符串为时间日期类型
#' @family ds function
#' @export
ds_as_datetime <- function(ds, timestampColumn, datetimeColumn, tzone = "Asia/Shanghai")
{
  if (is.character(ds[[timestampColumn]])) {
    mutate(ds, `:=`(
      {{datetimeColumn}},
      as_datetime(as.integer(stringi::stri_sub(!!sym(timestampColumn), to = 10)), tz = tzone)))
  }
  else if (is.numeric(ds[[timestampColumn]])) {
    mutate(ds, `:=`(
      {{datetimeColumn}},
      as_datetime(as.integer(!!sym(timestampColumn)), tz = tzone)))
  }
  else {
    ds
  }
}

#' @title 内连接
#' @family joins function
#' @export
ds_inner_join <- function(x, y, x_by, y_by = x_by, ...) {
  if(length(x_by) <= 0 || length(x_by) != length(y_by)) {
    stop("Invalid By Setting for inner_join")
  }
  y1 <- c(names(y)[!(names(y) %in% names(x))], y_by) |> unique()
  names(y_by) <- x_by
  inner_join(x, y |> select(!!!syms(y1)), by = y_by, ...)
}

#' @title 外连接
#' @family joins function
#' @export
ds_full_join <- function(x, y, x_by, y_by = x_by, ...) {
  if(length(x_by) <= 0 || length(x_by) != length(y_by)) {
    stop("Invalid By Setting for full_join")
  }
  y1 <- c(names(y)[!(names(y) %in% names(x))], y_by) |> unique()
  names(y_by) <- x_by
  full_join(x, y |> select(!!!syms(y1)), by = y_by, ...)
}

#' @title 左连接
#' @family joins function
#' @export
ds_left_join <- function(x, y, x_by, y_by = x_by, ...) {
  if(length(x_by) <= 0 || length(x_by) != length(y_by)) {
    stop("Invalid By Setting for left_join")
  }
  y1 <- c(names(y)[!(names(y) %in% names(x))], y_by) |> unique()
  names(y_by) <- x_by
  left_join(x, y |> select(!!!syms(y1)), by = y_by, ...)
}

#' @title 右连接
#' @family joins function
#' @export
ds_right_join <- function(x, y, x_by, y_by = x_by, ...) {
  if(length(x_by) <= 0 || length(x_by) != length(y_by)) {
    stop("Invalid By Setting for right_join")
  }
  y1 <- c(names(y)[!(names(y) %in% names(x))], y_by) |> unique()
  names(y_by) <- x_by
  right_join(x, y |> select(!!!syms(y1)), by = y_by, ...)
}

#' @title 过滤连接
#' @family joins function
#' @export
ds_semi_join <- function(x, y, x_by, y_by = x_by, ...) {
  if(length(x_by) <= 0 || length(x_by) != length(y_by)) {
    stop("Invalid By Setting for semi_join")
  }
  names(y_by) <- x_by
  semi_join(x, y, by = y_by, ...)
}

#' @title 反滤连接
#' @family joins function
#' @export
ds_anti_join <- function(x, y, x_by, y_by = x_by, ...) {
  if(length(x_by) <= 0 || length(x_by) != length(y_by)) {
    stop("Invalid By Setting for anti_join")
  }
  names(y_by) <- x_by
  anti_join(x, y, by = y_by, ...)
}

#' @title 行合并
#' @family joins function
#' @export
ds_rbind <- function(dx = list()) {
  rbind(!!!(dx |> unlist()))
}

#' @title 列合并
#' @family joins function
#' @export
ds_cbind <- function(dx = list()) {
  cbind(!!!(dx |> unlist()))
}
