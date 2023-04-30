#' @title 列举Demo数据集
#' @family dataset function
#' @export
ds_data <- function(datasetName = NULL, packageName = NULL) {
  if(is.null(datasetName)) {
    data(package = packageName)$results |> as_tibble() |>
      select(Package, Item, Title)
  } else {
    get(datasetName, loadNamespace(packageName %empty% "datasets"))
  }
}

#' @title 初始化数据集配置
#' @description
#'
#' 1、通过dsName和topic识别数据集和配置文件位置。
#'
#' 2、数据集必须首先使用\code{ds_init}函数创建配置文件（或手工建立），
#' 然后才能对数据集做增删改查操作。
#'
#' 3、所有数据集的存储结构都使用@action作为第一层分区：
#' __APPEND__, __ARCHIVE__
#'
#' @param dsName 数据集名称
#' @param data 样本数据，用来分析数据结构
#' @param topic 数据集保存的主题目录，默认为CACHE
#' @param partColumns 支持分区存储，支持多列
#' @param keyColumns 要求在所有行中具有唯一值的列向量，支持多列
#' @param suggestedColumns 查看数据时推荐的显示列向量，支持多列，
#' @param titleColumn 标题列，不支持多列
#' @param drop 如果为TRUE，就先清理已有的数据集数据和定义
#' @param withData 将样本数据写入数据集
#' @param desc 对数据集的额外描述
#' @family dataset function
#' @export
ds_init <- function(dsName,
                    topic = "CACHE",
                    data = tibble(),
                    bySchema = list(),
                    partColumns = c(),
                    keyColumns = c(),
                    suggestedColumns = c(),
                    titleColumn = c(),
                    drop = F,
                    withData = F,
                    desc = "-",
                    type = "__UNKNOWN__") {
  ##
  if(drop) {
    ds_drop(dsName, topic)
  }
  ## 初始化时自动补齐元数据字段
  if(!rlang::is_empty(bySchema)) {
    metaData <- tibble(`@from` = "string",
                       `@deleted` = TRUE,
                       `@action` = "string",
                       `@batchId` = "string",
                       `@lastmodifiedAt` = now()) |>
      schema_from_tibble()
    bySchema <- rbind.fill(
      bySchema |> schema_from_meta(),
      metaData) |> ds_as_unique("fieldName") |> schema_to_meta()
  }
  if(!rlang::is_empty(data)) {
    data$`@from` <- ""
    data$`@deleted` <- FALSE
    data$`@action` <- "string"
    data$`@batchId` <- "string"
    data$`@lastmodifiedAt` <- now()
  }
  ## 写入配置文件
  meta <- list(
    "desc" = desc,
    "schema" = bySchema,
    "partColumns" = c("@action", partColumns) |> unique() |> unlist(),
    "keyColumns" = keyColumns |> unique(),
    "suggestedColumns" = suggestedColumns,
    "titleColumn" = titleColumn)
  ds_meta_write(dsName = dsName,
                meta = meta,
                data = data,
                topic = topic,
                type = type)
  if(withData && nrow(data) > 0) {
    ds_write(data, dsName, topic)
  }
}

#' @title 批量追加更新的数据
#' @description
#' 为了避免修改原数据文件，追加数据时不查询旧数据，提交归档时再统一处理。
#'
#' 当追加的数据量与归档数据相比规模较小时，这种模式将具有优势。
#' @param d 要写入的数据
#' @param dsName 数据集名称
#' @param topic 数据集保存的主题目录，默认为CACHE
#' @param toDelete 将数据标记为删除
#' @family dataset function
#' @export
ds_append <- function(d, dsName, topic = "CACHE", warn.ignore = F) {
  if(is_empty(d)) {
    if(!warn.ignore)
      warning("Empty Dataset to write >>> ", dsName, "/", topic)
  } else {
    if(nrow(d) == 0) {
      warning("No Content in New Dataset to write >>> ", dsName, "/", topic)
    } else {
      ## 默认从CACHE任务目录读写数据集
      path <- get_path(topic, dsName)
      
      ## 读取数据集配置
      meta <- ds_meta(dsName, topic)
      
      ## 缺少schema定义
      if(is_empty(meta$schema)) {
        stop("No Schema Defined!!!")
      }
      
      ## 缺少主键字段
      lengthRaw <- nrow(d)
      if(!is_empty(meta$keyColumns)) {
        if(FALSE %in% (meta$keyColumns %in% names(d))) {
          stop("No keyColumns in data: ", meta$keyColumns |> paste(collapse = ", "))
        } else {
          ##
          d <- d |> ds_as_unique(meta$keyColumns)
        }
      }
      
      ## 缺少分区字段
      if(!is_empty(meta$partColumns)) {
        if(FALSE %in% (meta$partColumns %in% c("@action", names(d)))) {
          stop("No partColumns in data: ", meta$partColumns |> paste(collapse = ", "))
        }
      }
      
      ## 可写入字段
      validColumns <- c(
        names(d)[names(d) %in% (meta$schema |> purrr::map("fieldName") |> unlist())],
        names(d) |> purrr::keep(~ stringr::str_detect(.x, "^@"))
      ) |> unique()
      ## 写入数据
      batch <- id_gen()
      x <- d |>
        select(!!!syms(validColumns)) |>
        mutate(`@action` = "__APPEND__") |>
        mutate(`@batchId` = batch) |>
        mutate(`@lastmodifiedAt` = lubridate::now(tzone = "Asia/Shanghai"))
      ## 如果没有提供删除标记，就补充这一列
      if("@deleted" %nin% names(d)) x[["@deleted"]] <- FALSE
      ## 按schema转换数据格式
      meta$schema |>
        schema_from_meta() |>
        purrr::pwalk(function(fieldName, asTibble, ...) {
          if(length(x[[fieldName]]) > 0) {
            x[[fieldName]] <<- asTibble(x[[fieldName]])
          }
        })
      ## 写入parquet文件组
      x |>
        ungroup() |>
        write_dataset(
          path = path,
          format = "parquet",
          write_statistics = TRUE,
          basename_template = paste0("append-", batch, "-{i}.parquet"),
          partitioning = meta$partColumns,
          version = "2.6",
          existing_data_behavior = "overwrite")
      lengthUnique <- nrow(x)
      if(lengthUnique < lengthRaw) {
        message("data append <", dsName, ">", " ", lengthRaw, "(unique ", lengthUnique, ") rows wrote!")
      } else {
        message("data append <", dsName, ">", " ", lengthUnique, " rows wrote!")
      }
      return(batch)
    }
  }
  return(NULL)
}

#' @title 写入数据
#' @description 先执行追加操作，然后直接归档整理。
#' @param d 要追加的数据
#' @param dsName 数据集名称
#' @param topic 数据集保存的主题目录，默认为CACHE
#' @family dataset function
#' @export
ds_write <- function(d, dsName, topic = "CACHE") {
  ds_append(d, dsName, topic)
  ds_submit(dsName, topic)
}

#' @title 批量追加删除的数据
#' @description 以追加标记文件的形式删除数据（不做物理删除）
#' @param d 要删除的数据
#' @param dsName 数据集名称
#' @param topic 数据集保存的主题目录，默认为CACHE
#' @family dataset function
#' @export
ds_delete <- function(d, dsName, topic = "CACHE") {
  meta <- ds_meta(dsName, topic)
  if(is_empty(meta$keyColumns)) {
    stop("Can't Delete without keyColumns Setting!!!")
  }
  d |>
    ds_as_deleted() |>
    ds_append(dsName, topic)
}

#' @title 数据集归档
#' @description 消除零散的追加文件
#' @param dsName 数据集名称
#' @param topic 主题域
#' @details
#' 归档操作可能耗时，应当集中处理。
#' 如果不归档，则可能因为零散文件较多，而影响数据读取速度
#' @export
ds_submit <- function(dsName, topic = "CACHE") {
  meta <- ds_meta(dsName, topic)
  path <- get_path(topic, dsName)
  
  ## 提前列出准备删除后的分区文件
  allFiles <- ds_files(dsName, topic)
  toRemoveFiles <- allFiles |> purrr::keep(~ stringr::str_detect(.x, "@action=__APPEND__"))
  force(toRemoveFiles)
  ## 找出更新数据的分区
  d <- ds_read_affected(dsName, topic) |> collect()
  
  if(is_empty(d)) {
    warning("Empty Dataset to submit >>> ", dsName, "/", topic)
  } else {
    if(nrow(d) == 0) {
      warning("No Content in New Dataset to submit >>> ", dsName, "/", topic)
    } else {
      ## 写入数据
      message("data submit <", dsName, "> ", nrow(d), " rows affected!")
      batch <- id_gen()
      d |>
        mutate(`@action` = "__ARCHIVE__") |>
        ungroup() |>
        write_dataset(
          path = path,
          format = "parquet",
          write_statistics = TRUE,
          basename_template = paste0("archive-", batch, "-{i}.parquet"),
          partitioning = meta$partColumns,
          version = "2.6",
          existing_data_behavior = "delete_matching")
      ## 删除已经处理过的文件和文件夹
      ## 如果在写入过程中新增文件，不会被删除
      ## 删除文件
      toRemoveFiles |> purrr::walk(function(p) {
        if(fs::file_exists(p)) fs::file_delete(p)
      })
    }
  }
}

#' @title 读取数据集的文件信息
#' @param dsName 数据集名称
#' @param topic 主题域
#' @family dataset function
#' @export
ds_files <- function(dsName, topic = "CACHE") {
  path <- get_path(topic, dsName)
  open_dataset(path)$files
}

#' @title 读取数据集
#' @param dsName 数据集名称
#' @param topic 主题域
#' @family dataset function
#' @export
ds_read_appended <- function(dsName, topic = "CACHE") {
  meta <- ds_meta(dsName, topic)
  path <- get_path(topic, dsName)
  
  ## 按照yaml配置中的schema读取数据集
  myschema <- schema_from_meta(meta$schema)
  d <- open_dataset(
    path,
    format = "parquet",
    schema = schema_to_arrow(myschema))
  ##
  if(length(d$files) == 0) return(tibble())
  ##
  resp <- d |>
    filter(`@action` == "__APPEND__") |>
    collect() |>
    arrange(desc(`@batchId`)) |>
    ds_as_unique(meta$keyColumns)
  ## 按元数据中的读取建议整理结果
  if(!is_empty(meta$suggestedColumns)) {
    resp |> select(!!!syms(meta$suggestedColumns), everything())
  } else {
    resp
  }
}

#' @title 读取数据集
#' @param dsName 数据集名称
#' @param topic 主题域
#' @param noDeleted 不返回标记为删除的数据
#' @family dataset function
#' @export
ds_read0 <- function(dsName, topic = "CACHE", noDeleted = T, meta = c("@from")) {
  d <- ds_read_archived(dsName, topic)
  if(nrow(d) > 0) {
    d |> filter(`@deleted` == !noDeleted) |>
      select(-matches("^@"), !!!syms(meta))
  } else {
    d
  }
}

#' @title 仅读取已归档数据
#' @description
#' 仅返回已归档数据
#' @param dsName 数据集名称
#' @param topic 主题域
#' @family dataset function
#' @export
ds_read_archived <- function(dsName, topic = "CACHE") {
  meta <- ds_meta(dsName, topic)
  path <- get_path(topic, dsName)
  
  ## 按照yaml配置中的schema读取数据集
  myschema <- schema_from_meta(meta$schema)
  d <- open_dataset(
    path,
    format = "parquet",
    schema = schema_to_arrow(myschema))
  ##
  if(length(d$files) == 0) return(tibble())
  ##
  resp <- d |>
    filter(`@action` == "__ARCHIVE__")
  ## 按元数据中的读取建议整理结果
  if(!is_empty(meta$suggestedColumns)) {
    resp <- resp |> select(!!!syms(meta$suggestedColumns), everything())
  }
  return(resp)
}

#' @title 仅读取已归档数据
#' @description
#' 仅返回已归档数据
#' @param dsName 数据集名称
#' @param topic 主题域
#' @family dataset function
#' @export
ds_read_affected <- function(dsName, topic = "CACHE") {
  meta <- ds_meta(dsName, topic)
  path <- get_path(topic, dsName)
  ##
  appended <- ds_read_appended(dsName, topic)
  if(nrow(appended) > 0) {
    ## 读取受影响数据
    myschema <- schema_from_meta(meta$schema)
    ## 找出受影响的归档数据文件
    parts <- meta$partColumns |> purrr::discard(~ .x == "@action")
    if(length(parts) > 0) {
      ## 若有分区，仅读取受影响的归档数据
      ## 所有可能存在的分区
      allParts <- appended |> ds_distinct(parts)
      affectedParts <- seq_len(nrow(allParts)) |>
        purrr::map_df(function(i) {
          hiveStr <- parts |> purrr::map(~ paste(.x, allParts[[.x]][[i]], sep = "="))
          dataPath <- sprintf("%s/@action=__ARCHIVE__/%s",
                              get_path(topic, dsName),
                              paste(hiveStr, collapse = "/"))
          if(fs::dir_exists(dataPath)) {
            list(path = dataPath |> fs::dir_ls(type = "file", recurse = T))
          } else {
            list(path = "-")
          }
        })
      if(nrow(affectedParts |> filter(path != "-")) > 0) {
        partsData <- seq_along(affectedParts$path) |> purrr::map_dfr(function(i) {
          if(affectedParts$path[[i]] != "-") {
            nrowFile <- nrow(affectedParts$path[[i]] |> arrow::read_parquet())
            allParts |>
              slice(i) |>
              mutate(`@file` = affectedParts$path[[i]]) |>
              mutate(`@action` = "__ARCHIVE__")
          } else {
            tibble()
          }
        })
        affected <- affectedParts |>
          filter(path != "-") |>
          purrr::pmap_dfr(function(path, ...) {
            open_dataset(path,
                         format = "parquet",
                         schema = schema_to_arrow(myschema),
                         partitioning = meta$partColumns) |>
              collect() |>
              mutate(`@file` = path)
          }) |>
          select(-contains(meta$partColumns)) |>
          left_join(partsData, by = "@file") |>
          select(-`@file`)
      } else {
        affected <- tibble()
      }
    } else {
      ## 无分区时直接返回全部归档数据
      d <- open_dataset(path,
                        format = "parquet",
                        schema = schema_to_arrow(myschema)) |>
        collect()
      if(nrow(d) == 0) {
        affected <- tibble()
      } else {
        affected <- d |>
          filter(`@action` == "__ARCHIVE__") |>
          collect()
      }
    }
    ## 返回最新数据
    if(nrow(affected) > 0) {
      rbind.fill(appended, affected) |>
        ds_as_unique(meta$keyColumns)
    } else {
      appended
    }
  } else {
    tibble()
  }
}

#' @title 判断数据集是否已经定义
#' @param dsName 数据集名称
#' @param topic 主题域
#' @family dataset function
#' @export
ds_exists <- function(dsName, topic = "CACHE") {
  get_path(topic, dsName, ".metadata.yml") |>
    fs::file_exists()
}

#' @title 读取数据集
#' @param dsName 数据集名称
#' @param topic 主题域
#' @param noDeleted 不返回标记为删除的数据
#' @family dataset function
#' @export
ds_read <- function(dsName, topic = "CACHE", noDeleted = TRUE) {
  meta <- ds_meta(dsName, topic)
  path <- get_path(topic, dsName)
  
  ## 按照yaml配置中的schema读取数据集
  myschema <- schema_from_meta(meta$schema)
  d <- open_dataset(path, format = "parquet", schema = schema_to_arrow(myschema))
  
  if(length(d$files) == 0) {
    return(tibble())
  }
  
  ## 如果存储结构不包含@action、@lastmodifiedAt等元字段就抛异常
  metaColumns <- c("@action", "@lastmodifiedAt", "@batchId", "@deleted")
  if(FALSE %in% (metaColumns %in% names(d))) {
    stop("Dataset storage lost some meata column: ", metaColumns |> paste(collapse = ", "))
  }
  
  ## 如果配置文件中存在主键
  if(length(meta$keyColumns) > 0) {
    ## 提取__APPEND__数据，且不与__ARCHIVE__数据重复（归档后但未删除）
    keys0 <- d |>
      filter(`@action` == "__APPEND__") |>
      select(meta$keyColumns, "@action", "@lastmodifiedAt", "@deleted")
    ## 如果__ARCHIVE__数据保存后，未及时删除__APPEND__，读取时将丢失这部分数据
    ## 这可能是因为ds_submit执行过程中可能因为ds_read或ds_write执行时间过长
    ##
    ## 这可以在ds_read中剔除，这需要增加以下的判断步骤，但需要额外付出计算性能
    ## 这需要增加以下的判断步骤，但需要额外付出计算性能
    keys0 <- keys0 |>
      anti_join(d |>
                  select(meta$keyColumns, "@action", "@lastmodifiedAt") |>
                  filter(`@action` == c("__ARCHIVE__")),
                by = c(meta$keyColumns, "@lastmodifiedAt"))
    ## 提取应保留的最近一次__APPEND__数据
    keys <- keys0 |>
      select(meta$keyColumns, "@action", "@lastmodifiedAt", "@deleted") |>
      semi_join(keys0 |>
                  group_by(!!!syms(meta$keyColumns)) |>
                  summarise(`@lastmodifiedAt` = max(`@lastmodifiedAt`), .groups = "drop") |>
                  collect(), by = c(meta$keyColumns, "@lastmodifiedAt"))
  } else {
    keys <- tibble()
  }
  
  if(!is_empty(keys)) {
    ## 旧数据要更新的记录
    arrchive_expired <- d |>
      filter(`@action` == "__ARCHIVE__") |>
      semi_join(keys |> filter(`@action` == "__APPEND__"), by = meta$keyColumns)
    ## 追加数据中过期的记录
    append_expired <- d |>
      filter(`@action` == "__APPEND__") |>
      anti_join(keys, by = c(meta$keyColumns, "@lastmodifiedAt"))
    ## 剔除：过期旧数据和过期新数据, 以及标记为删除的
    resp <- d |>
      anti_join(arrchive_expired, by = c(meta$keyColumns, "@lastmodifiedAt")) |>
      anti_join(append_expired, by = c(meta$keyColumns, "@lastmodifiedAt"))
  } else {
    ## 数据没有追加操作
    resp <- d
  }
  
  ## 处理标记为删除的数据
  if(noDeleted) {
    resp <- resp |> filter(!`@deleted`)
  }
  
  ## 按元数据中的读取建议整理结果
  if(!is_empty(meta$suggestedColumns)) {
    resp |>
      select(!!!syms(meta$suggestedColumns), everything())
  } else {
    resp
  }
}

#' @title 列举所有数据集
#' @param topic 主题域
#' @family dataset function
#' @export
ds_all <- function(topic = "CACHE") {
  root_path <- get_path(topic)
  if(fs::dir_exists(root_path)) {
    fs::dir_ls(root_path, type = "file", all = T, glob = "*.yml", recurse = T) |>
      purrr::map_df(function(path) {
        x <- yaml::read_yaml(path)
        list(
          "datasetId" = x$datasetId,
          "type" = x$type,
          "name" = x$name,
          "desc" = x$desc
        )
      })
  } else {
    tibble()
  }
}

#' @title 按名称和检索数据集
#' @param topic 主题域
#' @family dataset function
#' @export
ds_search <- function(nameMatch = ".*", topic = "CACHE") {
  d <- ds_all(topic)
  if(nrow(d) > 0) {
    d |> filter(stringr::str_detect(name, nameMatch))
  } else {
    d
  }
}

#' @title 移除数据集和数据定义
#' @param dsName 数据集名称
#' @param topic 主题域
#' @family dataset function
#' @export
ds_drop <- function(dsName, topic = "CACHE") {
  path <- get_path(topic, dsName) |> fs::path_expand()
  if(fs::dir_exists(path)) {
    fs::dir_delete(path)
    message("[REMOVED DIR] >>> ", path)
  } else {
    message("[Empty DIR] >>> ", path)
  }
}

#' @title 移除数据集分区数据
#' @description 可以整个移除，也可以组装目录后按分区移除
#' @param dsName 数据集名称
#' @param action 默认为__ARCHIVE__，也可以选择__APPEND__删除未归档数据
#' @param topic 主题域
#' @family dataset function
#' @export
ds_remove_parts <- function(dsName, ..., action = "__ARCHIVE__", topic = "CACHE") {
  parts <- list(...)
  path <- get_path(
    topic,
    dsName,
    paste0("@action=", action),
    names(parts) |> purrr::map_chr(~ paste(.x, parts[[.x]], sep = "="))
  ) |> fs::path_expand()
  if(fs::dir_exists(path)) {
    fs::dir_delete(path)
    message("[REMOVED DIR] >>> ", path)
  } else {
    message("[Empty DIR] >>> ", path)
  }
  
}


#' @title 设置数据来源
#' @family dataset function
#' @export
ds_as_from <-function(ds, sourceFrom) {
  d <- ds |> collect()
  d["@from"] <- sourceFrom
  d
}

#' @title 设置删除标记
#' @family dataset function
#' @export
ds_as_deleted <-function(ds, flag = TRUE) {
  d <- ds |> collect()
  d["@deleted"] <- flag
  d
}

#' @title 立即执行数据收集（结束惰性计算）
#' @family dataset function
#' @export
ds_collect <- function(d) {
  d |> collect()
}
