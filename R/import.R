#' @title 初始化导入状态数据集
#' @description
#' 导入素材中所有文件都将入库。
#'
#' 每次写入的导入素材文件，应当放置在同一个批次文件夹中。
#' 该批次文件夹应当包含时间要素，以方便按写入时间排序。
#'
#' 在同一次扫描中，如果不同批次文件夹中的有相同路径的导入素材文件，只关注最新文件
#' 如果有补充采集，则视情况将旧文件标记为“忽略”。
#' @family import function
#' @export
import_init <- function(dsName = "__IMPORT_FILES__", cacheTopic = "CACHE") {
  ## 任务数据样本
  sampleData <- tibble(
    "importTopic" = "string",
    "createdAt" = now(), # birth_time,
    "scanedAt" = now(),
    "batchFolder" = "schedual_001",
    "filePath" = fs::fs_path("AAA/my.csv"),
    "fileSize" = fs::fs_bytes(123), # fs::bytes
    "lastmodifiedAt" = now(), # modification_time
    "todo" = TRUE, # 待处理标志
    "doneAt" = now(),  # 任务处理完成时间
    "ignore" = TRUE, # 不做处理
    "year" = 2022L, # createdAt year
    "month" = 10L)  # createdAt month
  ds_init(
    dsName = dsName,
    topic = cacheTopic,
    data = sampleData,
    keyColumns = c("batchFolder", "filePath"),
    partColumns = c("year", "month"),
    type = "导入来源")
}

#' @title 按批次导入文件素材
#' @family import function
#' @export
import_changed <- function(importFolderFrom = "scheduleJob_2000-01-01", batchRegex = "scheduleJob_\\d{4}-\\d{2}-\\d{2}$", importTopic = "IMPORT") {
  batchFolders <- fs::dir_ls(get_path(importTopic), type = "dir", recurse = F) |>
    purrr::keep(~ stringr::str_detect(.x, batchRegex)) |>
    purrr::keep(~ .x > get_path(importTopic, importFolderFrom))
  
  ## 扫描任务脚本文件夹
  if(!is_empty(batchFolders)) {
    batchFolders |>
      sort() |>
      purrr::map_dfr(function(batchPath) {
        ## 提取批次文件夹名称
        batchFolderName <- stringr::str_remove(batchPath, paste0(get_path(importTopic), "/"))
        ## 提取所有素材文件
        batchPath |>
          fs::dir_info(type = "file", recurse = T) |>
          as_tibble() |>
          rename(lastmodifiedAt=modification_time, createdAt=birth_time, fileSize=size) |>
          mutate(filePath = stringr::str_remove(path, paste0(batchPath, "/"))) |>
          mutate(batchFolder = batchFolderName) |>
          mutate(importTopic = importTopic) |>
          mutate(scanedAt = now(tzone = "Asia/Shanghai")) |>
          select(importTopic, createdAt, scanedAt, lastmodifiedAt, batchFolder, filePath, fileSize) |>
          mutate(year = as.integer(lubridate::year(createdAt))) |>
          mutate(month = as.integer(lubridate::month(createdAt)))
      })
  } else {
    warning("No Batch Folder need to Import!!!")
    tibble()
  }
}

#' @title 将需要导入的批次文件夹入库保存
#' @description
#' 比较根文件夹下的所有子文件夹比较，扫描所有比importFolderFrom字符串小的
#' 
#' @family import function
#' @export
import_scan <- function(importFolderFrom = "scheduleJob_2000-01-01",
                        batchRegex = "scheduleJob_\\d{4}-\\d{2}-\\d{2}$",
                        importDataset = "__IMPORT_FILES__",
                        importTopic = "IMPORT",
                        cacheTopic = "CACHE") {
  changed_files <- import_changed(importFolderFrom, batchRegex, importTopic)
  
  if(!is_empty(changed_files)) {
    ## 筛查未曾入库或虽曾入库但已修改的素材文件
    existing <- ds_read0(dsName = importDataset, topic = cacheTopic) |>
      collect()
    if(is_empty(existing)) {
      newScan <- changed_files
    } else {
      newScan <- changed_files |>
        anti_join(existing, by = c("filePath", "lastmodifiedAt"))
    }
    ## 入库
    if(nrow(newScan) > 0) {
      newScan |>
        mutate(todo = TRUE, ignore = FALSE) |>
        ds_write(dsName = importDataset, topic = cacheTopic)
      message("New batchFolders: ", newScan$batchFolder |> unique() |> paste(collapse = ", "))
    } else {
      message("No new files to import!!")
    }
  }
}

#' @title 扫描所有未处理、未忽略的文件
#' @family import function
#' @export
import_search <- function(fileMatch = ".*",
                          batchMatch = ".*",
                          importDataset = "__IMPORT_FILES__",
                          cacheTopic = "CACHE",
                          ignoreFlag = FALSE,
                          todoFlag = TRUE) {
  filesToRead <- ds_read0(dsName = importDataset, topic = cacheTopic)
  if(!is_empty(filesToRead)) {
    d0 <- filesToRead |> filter(todo %in% todoFlag)
    d0 |> filter(ignore == ignoreFlag) |>
      collect() |>
      filter(stringr::str_detect(filePath, fileMatch)) |>
      filter(stringr::str_detect(batchFolder, batchMatch)) |>
      mutate(fileSize = fs::as_fs_bytes(fileSize)) |>
      mutate(batchDay = lubridate::ymd(batchFolder |> stringr::str_replace_all("[^0-9]", "-"), quiet = T))
  } else {
    tibble()
  }
}

#' @title 批量导入文件
#' @param filesMatched 导入素材文件名
#' @param dsName 导出目标数据集名称
#' @param keyColumns 数据集的关键字段
#' @family import function
#' @export
import_files <- function(filesMatched, dsName, partColumns = c(), keyColumns = c(), dsType = "导入数据", func = function(path){}) {
  nrows <- 0
  if(nrow(filesMatched) > 0) {
    dataCache <- tibble()
    filesMatched |>
      select(importTopic, batchFolder, filePath) |>
      purrr::pwalk(function(importTopic, batchFolder, filePath) {
        path <- get_path(importTopic, batchFolder, filePath)
        message("toImport: <", batchFolder, ">", filePath)
        tryCatch({
          ## 调用单个文件的导入函数
          d <- func(path)
          nrows <<- nrows + nrow(d)
          if(!rlang::is_empty(d) && nrow(d) > 0) {
            ## 确定数据框中包含主键
            if(length(keyColumns) > 0 && (TRUE %in% (keyColumns %nin% names(d)))) {
              stop("Keycolumns not in file: ", importTopic, "/", path)
            }
            ## 自动创建数据集
            if(!ds_exists(dsName)) {
              ds_init(
                dsName,
                data = d,
                type = dsType,
                partColumns = partColumns,
                keyColumns = keyColumns)
            }
            ##
            d |>
              ds_as_unique(keyColumns) |>
              ds_as_from(fs::path_join(c(batchFolder, filePath))) |>
              ds_append(dsName)
            # if(nrow(dataCache) > 0 && !identical(sort(names(dataCache)), sort(names(d1)))) {
            #   ## 异构时分别写入
            #   dataCache |> ds_append(dsName, warn.ignore = T)
            #   dataCache <<- tibble()
            #   #
            #   d1 |> ds_append(dsName)
            # } else {
            #   dataCache <<- dataCache |> rbind.fill(d1)
            #   ## 累计1000以上写入一次磁盘
            #   if(nrow(dataCache) >= 1000) {
            #     dataCache |> ds_append(dsName)
            #     dataCache <<- tibble()
            #   }
            # }
          }
        }, error = function(e) stop(paste0("filePath: ", path, " >> "), e))
        
      })
    if(nrow(dataCache) > 0) ds_append(dataCache, dsName)
    if(nrows > 0) ds_submit(dsName)
  }
  return(filesMatched)
}

##
#' @title 导入CSV文件
#' @export
import_csv_default <- function(path) {
  d <- readr::read_csv(path, show_col_types = FALSE, col_types = readr::cols(.default = "c")) |>
    select(!contains("..."))
  if(!rlang::is_empty(d)) {
    ##
    if(d[[1]][[1]] == "占位") {d <- d |> slice(-1)}
  }
  d
}

#' @title 预览多个文件
#' @param filesMatched 导入素材文件名
#' @family import function
#' @export
import_files_preview <- function(filesMatched, keyColumns = c(), func = function(path){}) {
  if(nrow(filesMatched) > 0) {
    d <- filesMatched |>
      select(importTopic, batchFolder, filePath) |>
      purrr::pmap_dfr(function(importTopic, batchFolder, filePath) {
        tryCatch({
          path <- get_path(importTopic, batchFolder, filePath)
          func(path) |> ds_as_from(fs::path_join(c(batchFolder, filePath)))
        }, error = function(e) stop(paste0("filePath: ", path, " >> "), e))
      })
    ## 全部提取后使主键唯一
    if(nrow(d) > 0) {
      if(!rlang::is_empty(d) && nrow(d) > 0) {
        ## 确定数据框中包含主键
        if(length(keyColumns) > 0 && (TRUE %in% (keyColumns %nin% names(d)))) {
          stop("Invalid Keycolumns: ", paste(keyColumns, collapse = ","))
        }
        ##
        d |> ds_as_unique(keyColumns)
      }
    }
  } else {
    tibble()
  }
}

#' @title 导入多个CSV文件
#' @family import function
#' @export
import_csv <- purrr::partial(import_files, func = import_csv_default)

#' @title 预览多个CSV文件
#' @family import function
#' @export
import_csv_preview <- purrr::partial(import_files_preview, func = import_csv_default)

#' @title 修改todo标记
#' @param filesMatched 导入素材文件名
#' @param cacheTopic 数据集存储主题文件夹
#' @param importDataset 导入素材库所在的数据集
#' @family import function
#' @export
import_todo_flag <- function(filesMatched,
                             todoFlag = FALSE,
                             cacheTopic = "CACHE",
                             importDataset = "__IMPORT_FILES__") {
  if(nrow(filesMatched) > 0) {
    filesMatched |>
      mutate(todo = todoFlag, doneAt = now(tzone = "Asia/Shanghai")) |>
      ds_write(importDataset, cacheTopic)
  }
}
