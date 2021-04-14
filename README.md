# 数据库字段差异比对工具

> 用途：本脚本用来对比不同环境下，数据库表，字段，类型不一致的情况。

## 使用说明

### 1.环境准备

安装python3环境。

### 2.安装依赖

```shell
pip install pymysql
pip install pandas
```

### 3.运行脚本对比差异

```shell
python mysql_field_diff.py [数据库] [环境1] [环境2]
```

参数说明：
- [数据库]可以使用参数：all | bpm | cms | file | fin | hr | logmgr | mdm | message | oauth2 | portal | quartz | task
- [环境1]可以使用参数：dev | uat | beta
- [环境2]可以使用参数：dev | uat | beta

例如：
```shell
python mysql_field_diff.py hr dev uat
```

## 配置

目录下，`conf.ini` 是配置文件，用来配置数据库信息，服务列表等。