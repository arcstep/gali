% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/schema.R
\name{schema_from_meta}
\alias{schema_from_meta}
\title{从元数据生成schema数据框}
\usage{
schema_from_meta(meta = list())
}
\arguments{
\item{meta}{提供fieldName和fieldType}
}
\description{
根据fieldName和fieldType补充rType、asTibble函数和asArrow函数
}
\seealso{
Other schema-functions: 
\code{\link{schema_check}()},
\code{\link{schema_filedType}()},
\code{\link{schema_from_tibble}()},
\code{\link{schema_name}()},
\code{\link{schema_to_arrow}()},
\code{\link{schema_to_meta}()},
\code{\link{schema_to_tibble}()},
\code{\link{schema_types}()}
}
\concept{schema-functions}
