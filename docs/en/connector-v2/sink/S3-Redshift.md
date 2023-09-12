# S3Redshift

> The way of S3Redshift is to write data into S3, and then use Redshift's COPY command to import data from S3 to Redshift.

## Description

Output data to AWS Redshift.

> Tips:
> We based on the [S3File](S3File.md) to implement this connector. So you can use the same configuration as S3File.
> We made some trade-offs in order to support more file types, so we used the HDFS protocol for internal access to S3 and this connector need some hadoop dependencies.
> It's only support hadoop version **2.6.5+**.

## Key features

- [x] [exactly-once](../../concept/connector-v2-features.md)
- [x] [cdc](../../concept/connector-v2-features.md)

By default, we use 2PC commit to ensure `exactly-once`

## Options

|                name                 |  type  | required |         default value          |
|-------------------------------------|--------|----------|--------------------------------|
| jdbc_url                            | string | yes      | -                              |
| jdbc_user                           | string | yes      | -                              |
| jdbc_password                       | string | yes      | -                              |
| path                                | string | yes      | -                              |
| tmp_path                            | string | yes      | /tmp/seatunnel                 |
| bucket                              | string | yes      | -                              |
| access_key                          | string | no       | -                              |
| access_secret                       | string | no       | -                              |
| hadoop_s3_properties                | map    | no       | -                              |
| changelog_mode                      | enum   | no       | APPEND_ONLY                    |
| changelog_buffer_flush_size         | int    | no       | 20000                          |
| changelog_buffer_flush_interval_ms  | int    | no       | 20000                          |
| redshift_table                      | string | yes      | -                              |
| redshift_table_primary_keys         | array  | no       | -                              |
| redshift_temporary_table_name       | string | no       | st_temporary_${redshift_table} |
| redshift_s3_iam_role                | string | no       | -                              |
| redshift_s3_file_commit_worker_size | int    | no       | 1                              |
| common-options                      |        | no       | -                              |

### jdbc_url

The JDBC URL to connect to the Redshift database.

### jdbc_user

The JDBC user to connect to the Redshift database.

### jdbc_password

The JDBC password to connect to the Redshift database.

### path [string]

The target dir path is required.

### tmp_path [string]

The temporary path is required.

### bucket [string]

The bucket address of s3 file system, for example: `s3n://seatunnel-test`, if you use `s3a` protocol, this parameter should be `s3a://seatunnel-test`.

### access_key [string]

The access key of s3 file system. If this parameter is not set, please confirm that the credential provider chain can be authenticated correctly, you could check this [hadoop-aws](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html)

### access_secret [string]

The access secret of s3 file system. If this parameter is not set, please confirm that the credential provider chain can be authenticated correctly, you could check this [hadoop-aws](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html)

### hadoop_s3_properties [map]

If you need to add a other option, you could add it here and refer to this [Hadoop-AWS](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html)

```
hadoop_s3_properties {
  "fs.s3a.aws.credentials.provider" = "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
 }
```

### changelog_mode [enum]

The changelog mode of the sink writer, support:
`APPEND_ONLY`: Only append data to the target table.
`APPEND_ON_DUPLICATE_UPDATE`: If the primary key exists, update(update/delete) the data, otherwise insert the data.
`APPEND_ON_DUPLICATE_UPDATE_AUTOMATIC`: If the primary key exists, update(update/delete) the data, otherwise insert the data. Automatically switch copy/merge mode between snapshot sync and incremental sync.

### changelog_buffer_flush_size [int]

Flush buffer to s3 size of redshift changelog

### changelog_buffer_flush_interval_ms [int]

Flush buffer to s3 interval of redshift changelog

### redshift_table [string]

The target table name of redshift changelog.

### redshift_table_primary_keys [array]

The primary keys of the buffer/target-table, only needed by `APPEND_ON_DUPLICATE_UPDATE*` and `APPEND_ON_DUPLICATE_DELETE*` changelog mode.

### redshift_temporary_table_name [string]

The temporary table of redshift changelog.

### redshift_s3_iam_role [string]

The s3 iam role of redshift changelog.

### redshift_s3_file_commit_worker_size [int]

The worker size of redshift changelog file commit.

### common options

Sink plugin common parameters, please refer to [Sink Common Options](common-options.md) for details.

## Example

For append only

```hocon

  S3Redshift {
    source_table_name = "test_xxx_1"

    # file config
    tmp_path = "/tmp/seatunnel/s3_redshift_xxx/"
    path = "/seatunnel/s3_redshift_xxx/"

    # s3 config
    fs.s3a.endpoint = "s3.cn-north-1.amazonaws.com.cn"
    bucket = "s3a://seatunnel-test-bucket"
    access_key = "xxxxxxxxxxxxxxxxx"
    secret_key = "xxxxxxxxxxxxxxxxx"
    fs.s3a.aws.credentials.provider = "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"

    # redshift config
    jdbc_url = "jdbc:redshift://xxx.amazonaws.com.cn:5439/xxx"
    jdbc_user = "xxx"
    jdbc_password = "xxxx"

    # cdc changelog config
    changelog_mode = "APPEND_ONLY"
    redshift_table = "your_target_table"
  }

```

Support write cdc changelog event(APPEND_ON_DUPLICATE_UPDATE/APPEND_ON_DUPLICATE_UPDATE_AUTOMATIC).

*Using Redshift COPY sql import s3 file into tmp table, and use Redshift DELETE/MERGE sql merge tmp table data into target table.*
- [Redshift TEMPORARY Table](https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_TABLE_NEW.html)
- [Redshift COPY SQL](https://docs.aws.amazon.com/redshift/latest/dg/r_COPY.html)
- [Redshift DELETE USING SQL](https://docs.aws.amazon.com/redshift/latest/dg/r_DELETE.html)
- [Redshift MERGE SQL](https://docs.aws.amazon.com/redshift/latest/dg/r_MERGE.html)

Config example:

```hocon
env {
  job.mode = "BATCH"
  checkpoint.interval = 20000
}

source {
  FakeSource {
    result_table_name = "test_xxx_1"
    row.num = 100000
    split.num = 10
    schema = {
      fields {
        id = "int"
        name = "string"
      }
    }
  }
}

sink {
  S3Redshift {
    source_table_name = "test_xxx_1"
    
    # file config
    tmp_path = "/tmp/seatunnel/s3_redshift_xxx/"
    path = "/seatunnel/s3_redshift_xxx/"

    # s3 config
    fs.s3a.endpoint = "s3.cn-north-1.amazonaws.com.cn"
    bucket = "s3a://seatunnel-test-bucket"
    access_key = "xxxxxxxxxxxxxxxxx"
    secret_key = "xxxxxxxxxxxxxxxxx"
    fs.s3a.aws.credentials.provider = "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"

    # redshift config
    jdbc_url = "jdbc:redshift://xxx.amazonaws.com.cn:5439/xxx"
    jdbc_user = "xxx"
    jdbc_password = "xxxx"
    
    # cdc changelog config
    changelog_mode = "APPEND_ON_DUPLICATE_UPDATE"
    changelog_buffer_flush_size = 50000
    redshift_table = "your_target_table"
    redshift_table_primary_keys = ["id"]
  }
}
```

## Changelog

### 2.3.0-beta 2022-10-20

### next version

- Support write cdc changelog event

