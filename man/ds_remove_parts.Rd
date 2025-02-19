% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/dataset.R
\name{ds_remove_parts}
\alias{ds_remove_parts}
\title{移除数据集分区数据}
\usage{
ds_remove_parts(dsName, ..., action = "__ARCHIVE__", topic = "CACHE")
}
\arguments{
\item{dsName}{数据集名称}

\item{action}{默认为__ARCHIVE__，也可以选择__APPEND__删除未归档数据}

\item{topic}{主题域}
}
\description{
可以整个移除，也可以组装目录后按分区移除
}
\seealso{
Other dataset function: 
\code{\link{ds_all}()},
\code{\link{ds_append}()},
\code{\link{ds_as_deleted}()},
\code{\link{ds_as_from}()},
\code{\link{ds_collect}()},
\code{\link{ds_data}()},
\code{\link{ds_delete}()},
\code{\link{ds_drop}()},
\code{\link{ds_exists}()},
\code{\link{ds_files}()},
\code{\link{ds_init}()},
\code{\link{ds_read0}()},
\code{\link{ds_read_affected}()},
\code{\link{ds_read_appended}()},
\code{\link{ds_read_archived}()},
\code{\link{ds_read}()},
\code{\link{ds_search}()},
\code{\link{ds_write}()}
}
\concept{dataset function}
