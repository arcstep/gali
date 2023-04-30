##
as_difftime0 <- purrr::partial(as.difftime, units = "secs")
# duration0 <- purrr::partial(arrow::duration, unit = "s")
# time0 <- purrr::partial(arrow::time32, unit = "s")
stamp0 <- purrr::partial(arrow::timestamp, unit = "us", timezone = "Asia/Shanghai")

## 所有类型
allTypes <- tibble::tribble(
  ~ rType,    ~asTibble,       ~ asArrow,        ~fieldType,
  ## atomic
  "logical",  as.logical,      arrow::boolean,   c("bool"),
  "character",as.character,    arrow::string,    c("string"),
  "integer",  as.integer,      arrow::int32,     c("integer"),
  "numeric",  as.double,       arrow::float64,   c("numeric"),
  "double",   as.double,       arrow::float64,   c("numeric"),
  ## factor
  "factor",   as.factor,       arrow::dictionary,c("enum"),
  ## fs
  "fs_path",  fs::as_fs_path,  arrow::string,    c("path"),
  "fs_bytes", fs::as_fs_bytes, arrow::int32,     c("bytes"),
  "fs_perms", fs::as_fs_perms, arrow::int32,     c("permission"),
  ## lubridate
  "integer",  lubridate::as_datetime,     stamp0,           c("timestamp"),
  "Date",     lubridate::as_date,         arrow::date32,    c("date"),
  "POSIXct",  lubridate::as_datetime,     stamp0,           c("datetime"),
  "POSIXt",   lubridate::as_datetime,     stamp0,           c("datetime"),
  "difftime", as_difftime0,     arrow::duration, c("difftime"),
  "Duration", as_difftime0,     arrow::duration, c("duration"),
  "Period",   lubridate::as.period,       arrow::duration,  c("period"),
  "list",       NA,            NA,               c("list"),
  "data.frame", NA,            NA,               c("data.frame"),
  "function",   NA,            NA,               c("func"),
  "expression", NA,            NA,               c("expr"),
)

#' @title 所有数据类型
#' @family schema-functions
#' @export
schema_types <- function() {
  allTypes$fieldType |> unique()
}

#' @title 自动识别R对象的FiledType
#' @family schema-functions
#' @export
schema_filedType <- function(val) {
  myRType <- c(class(val), typeof(val)) |> unique()
  validType <- myRType[myRType %in% allTypes$rType]
  if(length(validType) == 0) {
    stop("<", validType, "> Not-In valid rTypes , use schema_types() to check rTypes list.")
  }
  (filter(allTypes, rType == validType[[1]])$fieldType)[[1]]
}

#' @title 检查数据类型
#' @param d 用于检查的数据框，为空时罗列支持的字段类型
#' @family schema-functions
#' @export
schema_name <- function(d = NULL) {
  if(is.null(d)) return(NULL)
  
  rTypes <- allTypes |>
    mutate(fieldType = allTypes$fieldType |> purrr::map_chr(~ .x[[1]])) |>
    ds_as_unique("rType")
  ##
  myType <- c(class(d), typeof(d)) |> unique()
  ## 至少应找到一个支持的R类型
  validType <- myType[myType %in% allTypes$rType] |> unlist()
  
  (rTypes |> filter(rType == validType[[1]]))$fieldType
}

#' @title 检查数据框中的类型是否符合schema要求
#' @param d 用于检查的数据框，为空时罗列支持的字段类型
#' @family schema-functions
#' @export
schema_check <- function(d = NULL) {
  if(is.null(d)) {
    message("Field Type Supported: [",
            allTypes$rType |> paste(collapse = ","),
            "]")
  } else {
    seq_along(d) |>
      purrr::walk(function(i) {
        myType <- c(class(d[[i]]), typeof(d[[i]])) |> unique()
        ## 至少应找到一个支持的R类型
        notNAforTibble <- (allTypes |> filter(!is.na(asTibble)))$rType
        validType <- myType[myType %in% notNAforTibble]
        if(length(validType) == 0) {
          stop("[Schema Check Failed] Invalid type: ",
               names(d)[[i]], " <", paste(myType, collapse = ","), ">")
        }
      })
    return(d)
  }
}

#' @title 从数据框的R类型转换schema
#' @description
#' tibble data >> schema list
#'
#' 根据数据框获得fieldName，进一步推断rType，以及fieldType、asTibble函数和asArrow函数
#' 默认匹配shinyType
#'
#' 仅支持可写入数据集的类型转换，如果包括无法识别的列表、S4等类型将会报错
#' @family schema-functions
#' @export
schema_from_tibble <- function(d) {
  if(rlang::is_empty(d)) {
    stop("Empty tibble !!")
  }
  ## 保留shcemaType的默认值即可
  rTypes <- allTypes |>
    mutate(fieldType = allTypes$fieldType |> purrr::map_chr(~ .x[[1]])) |>
    ds_as_unique("rType")
  ##
  schema_check(d)
  ##
  seq_along(d) |>
    purrr::map_df(function(i) {
      myType <- c(class(d[[i]]), typeof(d[[i]])) |> unique()
      ## 至少应找到一个支持的R类型
      validType <- myType[myType %in% allTypes$rType] |> unlist()
      list("fieldName" = names(d)[[i]], "rType" = validType[[1]])
    }) |>
    left_join(rTypes, by = "rType") |>
    select(fieldName, fieldType, rType, asTibble, asArrow)
}

#' @title 从元数据生成schema数据框
#' @description
#' 根据fieldName和fieldType补充rType、asTibble函数和asArrow函数
#'
#' @param meta 提供fieldName和fieldType
#' @family schema-functions
#' @export
schema_from_meta <- function(meta = list()) {
  ## 以fieldType为键值重组类型列表
  sTypes <- allTypes |> unnest(fieldType) |> ds_as_unique("fieldType")
  ##
  meta |>
    purrr::map_df(~ list(fieldName = .x$fieldName, fieldType = .x$fieldType)) |>
    left_join(sTypes, by = c("fieldType")) |>
    select(fieldName, fieldType, rType, asTibble, asArrow)
}

#' @title 从chema数据框生成元数据
#' @param bySchema 提供fieldName和fieldType
#' @family schema-functions
#' @export
schema_to_meta <- function(bySchema = tibble::tibble()) {
  bySchema |> purrr::pmap(function(fieldType, fieldName, ...) {
    list(
      "fieldName" = fieldName,
      "fieldType" = fieldType
    )
  })
}

#' @title 按照shcema转换数据框
#' @description
#' schema list >> tibble data
#'
#' @family schema-functions
#' @export
schema_to_tibble <- function(d, bySchema = list()) {
  d[] <- d |>
    seq_along() |>
    purrr::map(~ bySchema$asTibble[[.x]](d[[.x]]))
  d
}

#' @title 将schema转换为arrow要求的对象
#' @description
#' schema list >> arrow object
#'
#' 读取arrow时
#'
#' @family schema-functions
#' @export
schema_to_arrow <- function(bySchema = tibble()) {
  bySchema |> purrr::pmap(function(fieldName, asArrow, ...) {
    arrow::field(fieldName, asArrow())
  }) |> arrow::schema()
}
