# gali

#### 介绍
一个用于数据分析场景的数据库实现，基于R语言和Apache Parquet

#### 安装教程

该R包不遵循开源协议，属于「广州鸿蒙信息科技有限公司」内部使用的R包。
因此，也不支持cran安装。

有三种方式安装：

1. 下载zip包，在本地安装
2. 下载源码，在本地安装
3. 上传SSH公钥到git托管后台后使用remotes远程安装

```r
remotes::install_git("git@gitee.com:hongmeng-data/gali.git")
```

#### 使用说明

主要功能包括：

1. 组织数据分析目录
2. 读写 *Apache Parquet* 数据
