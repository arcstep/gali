#
workdays.Env <- new.env(parent = emptyenv())
workdays.Env$holidays <- c()
workdays.Env$workdays <- c()

#' @title 增加假日调整
#' @param fromDay 开始日期
#' @param toDay 截止日期
#' @family workday-funcs
#' @export
holiday_assign <- function(from, to = from) {
  workdays.Env$holidays <- c(
    workdays.Env$holidays,
    lubridate::as_date(from):lubridate::as_date(to) |> lubridate::as_date()
  )
}

#
#' @title 增加工作日调整
#' @param fromDay 开始日期
#' @param toDay 截止日期
#' @family workday-funcs
#' @export
workday_assign <- function(from, to = from) {
  workdays.Env$workdays <- c(
    workdays.Env$workdays,
    lubridate::as_date(from):lubridate::as_date(to) |> lubridate::as_date()
  )
}

## 2018年度
# 一、元旦：1月1日放假，与周末连休。
holiday_assign("2018-1-1")

# 二、春节：2月15日至21日放假调休，共7天。2月11日(星期日)、2月24日(星期六)上班。
holiday_assign("2018-2-15", "2018-2-21")
workday_assign("2018-2-11")
workday_assign("2018-2-24")
#
# 三、清明节：4月5日至7日放假调休，共3天。4月8日(星期日)上班。
holiday_assign("2018-4-5", "2018-4-7")
workday_assign("2018-4-8")
#
# 四、劳动节：4月29日至5月1日放假调休，共3天。4月28日(星期六)上班。
holiday_assign("2018-4-29", "2018-5-1")
workday_assign("2018-4-28")
#
# 五、端午节：6月18日放假，与周末连休。
holiday_assign("2018-6-18")
#
# 六、中秋节：9月24日放假，与周末连休。
holiday_assign("2018-9-24")
#
# 七、国庆节：10月1日至7日放假调休，共7天。9月29日(星期六)、9月30日(星期日)上班。
holiday_assign("2018-10-1", "2018-10-7")
workday_assign("2018-9-29", "2018-9-30")

## 2019年度
##
## 元旦
holiday_assign("2018-12-30", "2019-1-1")
## 春节
holiday_assign("2019-2-4", "2019-2-10")
## 清明
holiday_assign("2019-4-4", "2019-4-6")
## 五一
holiday_assign("2019-5-1")
## 端午
holiday_assign("2019-6-7", "2019-6-9")
## 中秋
holiday_assign("2019-9-13", "2019-9-15")
workday_assign("2019-9-29")
workday_assign("2019-10-12")
## 国庆
holiday_assign("2019-10-1", "2019-10-7")

## 2020年度
##
# 一、元旦：2020年1月1日放假，共1天。
holiday_assign("2020-1-1")
#
# 二、春节：1月24日至30日放假调休，共7天。1月19日(星期日)、2月1日(星期六)上班。
holiday_assign("2020-1-24", "2020-1-30")
workday_assign("2020-1-19")
workday_assign("2020-2-1")
#
# 三、清明节：4月4日至6日放假调休，共3天。
holiday_assign("2020-4-3", "2020-4-5")
#
# 四、劳动节：5月1日至5日放假调休，共5天。4月26日(星期日)、5月9日(星期六)上班。
holiday_assign("2020-5-1", "2020-5-5")
workday_assign("2020-4-26")
workday_assign("2020-5-9")
#
# 五、端午节：6月25日至27日放假调休，共3天。6月28日(星期日)上班。
holiday_assign("2020-6-25", "2020-6-27")
workday_assign("2020-6-28")
#
# 六、国庆节、中秋节：10月1日至8日放假调休，共8天。9月27日(星期日)、10月10日(星期六)上班。
holiday_assign("2020-10-1", "2020-10-8")
workday_assign("2020-9-27")
workday_assign("2020-10-10")

## 2021年度
# 一、元旦：2021年1月1日至3日放假，共3天。
holiday_assign("2021-1-1", "2021-1-3")
#
# 二、春节：2月11日至17日放假调休，共7天。2月7日（星期日）、2月20日（星期六）上班。
holiday_assign("2021-2-11", "2021-2-17")
workday_assign("2021-2-7")
workday_assign("2021-2-20")
#
# 三、清明节：4月3日至5日放假调休，共3天。
holiday_assign("2021-4-3", "2021-4-5")
#
# 四、劳动节：5月1日至5日放假调休，共5天。4月25日（星期日）、5月8日（星期六）上班。
holiday_assign("2021-5-1", "2021-5-5")
workday_assign("2021-4-25")
workday_assign("2021-5-08")
#
# 五、端午节：6月12日至14日放假，共3天。
holiday_assign("2021-6-12", "2021-6-14")
#
# 六、中秋节：9月19日至21日放假调休，共3天。9月18日（星期六）上班。
holiday_assign("2021-9-19", "2021-9-21")
workday_assign("2021-9-18")
#
# 七、国庆节：10月1日至7日放假调休，共7天。9月26日（星期日）、10月9日（星期六）上班。
holiday_assign("2021-10-1", "2021-10-7")
workday_assign("2021-9-26")
workday_assign("2021-10-9")

## 2022年度
# 一、元旦：2022年1月1日至3日放假，共3天。
holiday_assign("2022-1-1", "2022-1-3")
#
# 二、春节：1月31日至2月6日放假调休，共7天。1月29日（星期六）、1月30日（星期日）上班。
holiday_assign("2022-1-31", "2022-2-6")
workday_assign("2022-1-29")
workday_assign("2022-1-30")
#
# 三、清明节：4月3日至5日放假调休，共3天。4月2日（星期六）上班。
holiday_assign("2022-4-3", "2022-4-5")
workday_assign("2022-4-2")
#
# 四、劳动节：4月30日至5月4日放假调休，共5天。4月24日（星期日）、5月7日（星期六）上班。
holiday_assign("2022-4-30", "2022-5-4")
workday_assign("2022-4-24")
workday_assign("2022-5-7")
#
# 五、端午节：6月3日至5日放假，共3天。
holiday_assign("2022-6-3", "2022-6-5")
#
# 六、中秋节：9月10日至12日放假，共3天。
holiday_assign("2022-9-10", "2022-9-12")
#
# 七、国庆节：10月1日至7日放假调休，共7天。10月8日（星期六）、10月9日（星期日）上班。##
holiday_assign("2022-10-1", "2022-10-7")
workday_assign("2022-10-8", "2022-10-9")

## 2023年度
# 1月：元旦，2022年12月31日至2023年1月2日放假，共3天。（1月3日周二上班）
holiday_assign("2022-12-31", "2023-1-2")
workday_assign("2023-1-3")
# 2月：春节，2023年1月21日至27日放假调休，共7天。（1月28日和29日周六周日上班）
holiday_assign("2023-1-21", "2023-1-27")
workday_assign("2023-1-28", "2023-1-29")
# 4月：清明节，4月5日放假，共1天。（4月6日周四上班）
holiday_assign("2023-4-5")
workday_assign("2023-4-6")
# 5月：劳动节，5月1日至5月5日放假调休，共5天。（4月29日和30日周六周日上班）
holiday_assign("2023-5-1", "2023-5-5")
workday_assign("2023-4-29", "2023-4-30")
# 6月：端午节，6月22日至24日放假公休，共3天。（6月25日周日上班）
holiday_assign("2023-6-22", "2023-6-24")
workday_assign("2023-6-25")
# 9月：中秋节、国庆节：9月29日至10月6日放假调休，共8天。（10月7日和8日周五周六上班）
holiday_assign("2023-9-29", "2023-10-6")
workday_assign("2023-10-7", "2023-10-8")

##
workdays_zh_CN_marker <- function(fromDay, toDay) {
  ## 生成全年的时间序列
  days <- lubridate::as_date(fromDay):lubridate::as_date(toDay) |> lubridate::as_date()
  ## 生成全年的工作日标记序列
  ## 默认情况下，周一至周五为工作日，周六、周日为假日
  workmarkers <- days |> purrr::map_lgl(
    ~ lubridate::wday(.x, label = F) %in%  c(2:6))
  ## 按国务院发布的节假日，修改为节日放假
  workmarkers[days %in% workdays.Env$holidays] <- F
  ## 按国务院发布的节假日，调整为工作日
  workmarkers[days %in% workdays.Env$workdays] <- T
  ## 返回数据框
  tibble("day" = days |> lubridate::as_date(), "workday" = workmarkers)
}

#' @title 工作日清单
#' @description
#' 默认为周一至周五为工作日，再根据中国国务院发布的工作日调整政策修正
#'
#' @param fromDay 开始日期
#' @param toDay 截止日期
#' @family workday-funcs
#' @export
workdays_zh_CN <- function(fromDay = "2018-01-01", toDay = "2023-12-31") {
  workdays_zh_CN_marker(fromDay, toDay) |>
    mutate("seq_day" = ifelse(workday, 1, 0)) |>
    mutate("cum_day" = cumsum(seq_day))
}

#' @title 节假日清单
#' @param fromDay 开始日期
#' @param toDay 截止日期
#' @family workday-funcs
#' @export
holidays_zh_CN <- function(fromDay = "2018-01-01", toDay = "2023-12-31") {
  workdays_zh_CN_marker(fromDay, toDay) |>
    rename(holiday = workday) |>
    mutate(seq_day = ifelse(holiday, 0, 1)) |>
    mutate(cum_day = cumsum(seq_day))
}

#
ds_add_seqdays <- function(d, fromColumn, toColumn, newName, seqFunc) {
  ## 构造需要处理的数据集
  ds <- d |> ds_select(c(fromColumn, toColumn)) |>
    ds_rename("from", fromColumn) |>
    ds_rename("to", toColumn) |>
    mutate(from = lubridate::as_date(from)) |>
    mutate(to = lubridate::as_date(to))
  wd <- seqFunc(
    fromDay = ds$from |> purrr::discard(~ is.na(.x)) |> min(),
    toDay = ds$to |> purrr::discard(~ is.na(.x)) |> max()) |>
    select(day, seq_day, cum_day)
  ## 生成日期序列数据集
  ds0 <- ds |>
    distinct(from, to) |>
    left_join(wd, by = c("from"="day")) |>
    rename(`@day_from` = cum_day) |>
    left_join(wd |> select(-seq_day), by = c("to"="day")) |>
    rename(`@day_to` = cum_day) |>
    mutate(`@days` = as.integer(`@day_to` - `@day_from` + seq_day))
  ds |>
    left_join(ds0 |> select(from, to, `@days`), by = c("from", "to")) |>
    select(`@days`) |>
    ds_rename(newName, "@days") |>
    cbind(d) |>
    as_tibble()
}
#' @title 计算时间段内包含的工作日数量
#' @description
#' 计算数据集中指定两列占用的工作日数量
#'
#' @param d 要补充字段的数据集
#' @param fromColumn 开始日期字段名
#' @param toColumn 截止日期字段名
#' @param workdayName 工作日字段命名
#' @family workday-funcs
#' @export
ds_add_workdays <- function(d, fromColumn, toColumn, newName = "@workdays") {
  d |> ds_add_seqdays(seqFunc = workdays_zh_CN,
                      fromColumn = fromColumn,
                      toColumn = toColumn,
                      newName = newName)
}

#' @title 计算时间段内包含的假日数量
#' @description
#' 计算数据集中指定两列占用的假日数量
#'
#' @param d 要补充字段的数据集
#' @param fromColumn 开始日期字段名
#' @param toColumn 截止日期字段名
#' @param workdayName 工作日字段命名
#' @family workday-funcs
#' @export
ds_add_holidays <- function(d, fromColumn, toColumn, newName = "@workdays") {
  d |> ds_add_seqdays(seqFunc = holidays_zh_CN,
                      fromColumn = fromColumn,
                      toColumn = toColumn,
                      newName = newName)
}

