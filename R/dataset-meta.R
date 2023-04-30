#' @title 查看数据集元数据
#' @param dsName 数据集名称
#' @param topic 主题域
#' @family metadata function
#' @export
ds_meta <- function(dsName, topic = "CACHE") {
  path <- get_path(topic, dsName, ".metadata.yml")
  if(fs::file_exists(path)) {
    yaml::read_yaml(path)
  } else {
    stop("No Database Meta: [", topic, "] ", dsName)
  }
}

#' @title 写入数据集的Yaml配置
#' @description 支持补写元数据的配置项
#' @param dsName 数据集名称
#' @param meta 主元数据内容
#' @param ex 扩展的元数据内容
#' @param data 样本数据，用来分析数据结构
#' @param topic 数据集保存的主题目录，默认为CACHE
#' @param type 数据集类型
#' @family metadata function
#' @export
ds_meta_write <- function(dsName, meta = c(), ex = c(),  data = tibble(), topic = "CACHE", type = "__UNKNOWN__") {
  ## 自动创建数据集文件夹
  get_path(topic, dsName) |> fs::dir_create()
  
  ## 读取原有的元数据
  if(ds_exists(dsName, topic)) {
    datasetMeta <- ds_meta(dsName, topic)
  } else {
    datasetMeta <- list()
  }
  
  ## 自动生成额外的元数据项
  updateTimestamp <- lubridate::now()
  datasetMeta$datasetId <- digest::digest(fs::path_join(c(topic, dsName)), algo = "xxhash32")
  datasetMeta$topic <- topic |> paste(collapse = "/")
  datasetMeta$name <- dsName |> paste(collapse = "/")
  datasetMeta$type <- type
  datasetMeta$updateTime <- updateTimestamp |> as.integer()
  datasetMeta$updateTimeDesc <- lubridate::as_datetime(updateTimestamp, tz = "Asia/Shanghai") |> as.character()
  
  ## 使用meta参数对元数据做局部覆盖
  ## 部分名称在meta和ex中不允许被使用
  names(meta) |>
    purrr::walk(function(i) {
      if(i %nin% c("datasetId", "name", "topic", "type", "updateTime", "updateTimeDesc")) {
        datasetMeta[[i]] <<- meta[[i]]
      } else {
        warning(i, "is a built-in Keyword!!!")
      }
    })
  names(ex) |>
    purrr::walk(function(i) {
      if(i %nin% c("datasetId", "name", "topic", "type", "updateTime", "updateTimeDesc")) {
        datasetMeta[[i]] <<- ex[[i]]
      } else {
        warning(i, "is a built-in Keyword!!!")
      }
    })
  
  ## 如果元数据没有提供架构描述，则从样本数据中推断
  if(is_empty(meta$schema) && !is_empty(data)) {
    datasetMeta$schema <- ds_schema(data) |>
      purrr::pmap(function(fieldType, fieldName, ...) {
        list("fieldName" = fieldName, "fieldType" = fieldType)
      })
  }
  
  ## 保存YAML文件
  yaml::write_yaml(datasetMeta, get_path(topic, dsName, ".metadata.yml"))
  
  ##
  message("dataset metafile wrote: ", paste0(topic, "/", dsName, "/.metadata.yml"))
}

#' @title 查看数据集架构
#' @description
#' 查看数据集架构时，必须包含数据。
#'
#' @param ds 数据框或数据集名称
#' @family metadata function
#' @export
ds_schema <- function(ds, topic = "CACHE") {
  if(is_empty(ds)) {
    tibble()
  } else if(purrr::is_character(ds, 1)) {
    ## 如果参数是字符串，就将yaml配置中的架构转化为数据框
    meta <- ds_meta(ds, topic)
    meta$schema |> schema_from_meta()
  } else if("data.frame" %in% class(ds)) {
    ## 如果参数是数据框，就转换为schema，但仅取 <fieldName, fieldType>
    schema_from_tibble(ds)
  } else {
    stop("Invalid ds: <", ds |> str(), ">")
  }
}

#' @title 按数据集架构定义转换数据框
#' @description
#' 查看数据集架构时，必须包含数据。
#'
#' @param ds 数据框
#' @param dsName 数据集名称
#' @param topic 存储主题域
#' @family metadata function
#' @export
ds_as_schema <- function(ds, dsName, topic = "CACHE") {
  schema_to_tibble(
    ds |> collect(),
    bySchema = ds_schema(dsName, topic)
  )
}

#' @title 详细比较两个数据集结构
#' @param schema1 用于比较的数据集结构，可使用\code{\link{ds_schema}}获得
#' @param schema2 用于参考的数据集结构，可使用\code{\link{ds_schema}}获得
#' @family metadata function
#' @export
ds_diff_schema <- function(schema1, schema2) {
  full_join(schema1, schema2, by = c("fieldName")) |>
    mutate(equal = .data[["fieldType.x"]]==.data[["fieldType.y"]]) |>
    mutate(equal = ifelse(is.na(equal), FALSE, equal)) |>
    mutate(contained = is.na(.data[["fieldType.x"]]) | .data[["fieldType.x"]]==.data[["fieldType.y"]]) |>
    mutate(contained = ifelse(is.na(.data[["contained"]]), FALSE, .data[["contained"]]))
}

#' @title 详细比较两个数据集结构
#' @param ds1 用于比较的数据集结构，可使用\code{\link{ds_schema}}获得
#' @param ds2 用于参考的数据集结构，可使用\code{\link{ds_schema}}获得
#' @family metadata function
#' @export
ds_diff_dataset <- function(ds1, ds2) {
  if(is_empty(ds1)) {
    stop("ds1 is NULL and failed to compare")
  }
  if(is_empty(ds2)) {
    stop("ds1 is NULL and failed to compare")
  }
  ds_diff_schema(ds_schema(ds1), ds_schema(ds2))
}
