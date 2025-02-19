% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/dataset-plyr.R
\name{ds_as_unique}
\alias{ds_as_unique}
\title{去除重复行}
\usage{
ds_as_unique(ds, keyColumns = c())
}
\arguments{
\item{ds}{要确认的数据集}

\item{keyColumns}{要确认的列名或其向量、列表}
}
\description{
按键值列去除重复行，保留最先发现的行
}
\seealso{
Other ds function: 
\code{\link{ds_add_count}()},
\code{\link{ds_arrange}()},
\code{\link{ds_as_datetime}()},
\code{\link{ds_as_month}()},
\code{\link{ds_as_year}()},
\code{\link{ds_count}()},
\code{\link{ds_distinct}()},
\code{\link{ds_dup_count}()},
\code{\link{ds_filter}()},
\code{\link{ds_head}()},
\code{\link{ds_n_max}()},
\code{\link{ds_n_min}()},
\code{\link{ds_rename}()},
\code{\link{ds_select_by}()},
\code{\link{ds_select_na}()},
\code{\link{ds_select_not_na}()},
\code{\link{ds_select}()},
\code{\link{ds_tail}()}
}
\concept{ds function}
