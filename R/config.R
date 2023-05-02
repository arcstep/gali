## 保存任务管理目录
TASK.ENV <- new.env(hash = TRUE, parent = emptyenv())

## 构造访问路径

#' @title 根据配置构造数据路径
#' @param topic 任务主题
#' @family config functions
#' @export
get_path <- function(topic, ...) {
  p <- list(...)
  tryCatch(
    fs::path_join(c(get_topic(topic), unlist(p))),
    error = function(e) stop("No Config Loaded for: <", topic, ">")
  )
}

## 获取内存配置项

#' @title 获取配置项的值
#' @param topic 任务主题
#' @family config functions
#' @export
get_topic <- function(topic) {
  TASK.ENV[[topic]]
}

#' @title 列举所有配置项
#' @family config functions
#' @export
get_topics <- function() {
  ls(envir = TASK.ENV)
}

#' @title 获得当前配置
#' @family config functions
#' @export
## 获取所有配置
get_config <- function() {
  as.list(TASK.ENV)
}

#' @title 设置配置项的值
#' @family config functions
#' @export
set_topic <- function(topic, path) {
  assign(topic, path, envir = TASK.ENV)
}

## 读写配置文件

#' @title 加载配置文件到内存
#' @description
#' 配置文件存在时，加载配置文件到运行环境。
#'
#' ... >> ... >> 加载配置项
#' @details
#' 使用\code{ROOT_PATH}时有一个关键约定：
#' 配置项必须以\code{./}开头，才能使用\code{ROOT_PATH}扩展其路径；
#' 否则，将被视为独立配置名。
#'
#' @param path 默认为配置文件所在的目录
#' @param yml 默认为config.yml
#' @family config functions
#' @export
config_load <- function(path = "./", yml = "config.yml") {
  ## 加载配置到内存
  if(fs::is_dir(path)) {
    path <- fs::path_join(c(path, yml))
  }
  topics <- config_yaml(path)
  if("ROOT_PATH" %in% names(topics)) {
    if(!fs::dir_exists(topics$ROOT_PATH)) {
      fs::dir_create(topics$ROOT_PATH)
    }
    root_path <- topics[["ROOT_PATH"]]
  } else {
    root_path <- NULL
  }
  
  ## 设置主题并自动创建主题目录
  message("\n ---- Loading Config ---- ")
  names(topics) |> purrr::walk(function(item) {
    set_topic(item, topics[[item]])
    if(item %nin% c("ROOT_PATH", "app")) {
      if(stringr::str_detect(topics[[item]], "^(\\.\\/)")) {
        set_topic(item, fs::path_join(c(root_path, topics[[item]])) |> fs::path_abs())
      }
      get_path(item) |> fs::dir_create()
    }
  })
  
  ## 如果存在IMPORT定义，则自动初始化导入状态数据集
  if("IMPORT" %in% get_topics()) {
    if(!ds_exists("__IMPORT_FILES__")) import_init()
  }
}

#' @title 创建或补写配置项到磁盘
#' @description
#' 多次运行时会增量补充。
#'
#' ... >> 写入配置项 >> 加载配置项
#' @param path YAML配置文件目录
#' @param yml 默认为config.yml
#' @param option 配置项
#' @family config functions
#' @export
config_write <- function(path = "./", yml = "config.yml", option = list()) {
  if(fs::dir_exists(path)) {
    ## 写入配置
    p <- fs::path_join(c(path, yml))
    if(fs::file_exists(p)) {
      xoption <- config_yaml(p)
    } else {
      xoption <- list(
        "ROOT_PATH" = fs::path_abs(path),
        "UPLOAD" = "./UPLOAD",
        "IMPORT" = "./IMPORT",
        "TASK" = "./TASK",
        "CACHE" = "./CACHE"
        )
    }
    names(option) |> purrr::walk(function(i) {
      xoption[[i]] <<- option[[i]]
    })
    xoption |> yaml::write_yaml(p)
    ## 加载配置
    config_load(path, yml)
    ## 返回内存中的所有配置
    get_config()
  } else {
    stop("Path Folder Not Exist: ", path)
  }
}
#' @title 初始化配置项
#' @description
#' 自动创建\code{ROOT_PATH}目录。
#'
#' 初始化配置文件夹 >> 写入配置项 >> 加载配置项
#'
#' 如果配置文件路径已经存在，则使用已有的配置文件。
#'
#' @param path YAML配置文件目录
#' @param yml 默认为config.yml
#' @param option 配置项
#' @family config functions
#' @export
config_init <- function(path = "./",
                        yml = "config.yml",
                        option = list()) {
  ## 创建配置文件目录
  if(!fs::dir_exists(path)) {
    fs::dir_create(path)
    # fs::file_touch(fs::path_join(c(path, "README.md")))
  }
  ## 写入配置
  config_write(path, yml, option)
}

#
config_yaml <- function(ymlPath) {
  if(!fs::file_exists(ymlPath)) {
    stop("No config.yml in: ", ymlPath, " [with WorkDir: ", getwd(), "]")
  }
  yaml::read_yaml(ymlPath)
}

