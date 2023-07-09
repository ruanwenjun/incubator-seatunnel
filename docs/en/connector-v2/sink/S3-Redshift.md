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

- [x] file format type
  - [x] text
  - [x] csv
  - [x] parquet
  - [x] orc
  - [x] json

## Options

|                name                |  type   | required |                       default value                       |
|------------------------------------|---------|----------|-----------------------------------------------------------|
| jdbc_url                           | string  | yes      | -                                                         |
| jdbc_user                          | string  | yes      | -                                                         |
| jdbc_password                      | string  | yes      | -                                                         |
| execute_sql                        | string  | false    | -                                                         |
| path                               | string  | yes      | -                                                         |
| bucket                             | string  | yes      | -                                                         |
| access_key                         | string  | no       | -                                                         |
| access_secret                      | string  | no       | -                                                         |
| hadoop_s3_properties               | map     | no       | -                                                         |
| file_name_expression               | string  | no       | "${transactionId}"                                        |
| file_format_type                   | string  | no       | "text"                                                    |
| filename_time_format               | string  | no       | "yyyy.MM.dd"                                              |
| field_delimiter                    | string  | no       | '\001'                                                    |
| row_delimiter                      | string  | no       | "\n"                                                      |
| partition_by                       | array   | no       | -                                                         |
| partition_dir_expression           | string  | no       | "${k0}=${v0}/${k1}=${v1}/.../${kn}=${vn}/"                |
| is_partition_field_write_in_file   | boolean | no       | false                                                     |
| sink_columns                       | array   | no       | When this parameter is empty, all fields are sink columns |
| is_enable_transaction              | boolean | no       | true                                                      |
| batch_size                         | int     | no       | 1000000                                                   |
| changelog_mode                     | enum    | no       | APPEND_ONLY                                               |
| changelog_buffer_flush_size        | int     | no       | 20000                                                     |
| changelog_buffer_flush_interval_ms | int     | no       | 20000                                                     |
| redshift_table                     | string  | no       | -                                                         |
| redshift_table_primary_keys        | array   | no       | -                                                         |
| redshift_temporary_table_name      | string  | no       | st_temporary_${redshift_table}                            |
| redshift_s3_iam_role               | string  | no       | -                                                         |
| common-options                     |         | no       | -                                                         |

### jdbc_url

The JDBC URL to connect to the Redshift database.

### jdbc_user

The JDBC user to connect to the Redshift database.

### jdbc_password

The JDBC password to connect to the Redshift database.

### execute_sql

The SQL to execute after the data is written to S3.

eg:

```sql

COPY target_table FROM 's3://yourbucket${path}' IAM_ROLE 'arn:XXX' REGION 'your region' format as json 'auto';
```

`target_table` is the table name in Redshift.

`${path}` is the path of the file written to S3. please confirm your sql include this variable. and don't need replace it. we will replace it when execute sql.

IAM_ROLE is the role that has permission to access S3.

format is the format of the file written to S3. please confirm this format is same as the file format you set in the configuration.

please refer to [Redshift COPY](https://docs.aws.amazon.com/redshift/latest/dg/r_COPY.html) for more details.

please confirm that the role has permission to access S3.

### path [string]

The target dir path is required.

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

### file_name_expression [string]

`file_name_expression` describes the file expression which will be created into the `path`. We can add the variable `${now}` or `${uuid}` in the `file_name_expression`, like `test_${uuid}_${now}`,
`${now}` represents the current time, and its format can be defined by specifying the option `filename_time_format`.

Please note that, If `is_enable_transaction` is `true`, we will auto add `${transactionId}_` in the head of the file.

### file_format_type [string]

We supported as the following file types:

`text` `csv` `parquet` `orc` `json`

Please note that, The final file name will end with the file_format's suffix, the suffix of the text file is `txt`.

### filename_time_format [string]

When the format in the `file_name_expression` parameter is `xxxx-${now}` , `filename_time_format` can specify the time format of the path, and the default value is `yyyy.MM.dd` . The commonly used time formats are listed as follows:

| Symbol |    Description     |
|--------|--------------------|
| y      | Year               |
| M      | Month              |
| d      | Day of month       |
| H      | Hour in day (0-23) |
| m      | Minute in hour     |
| s      | Second in minute   |

See [Java SimpleDateFormat](https://docs.oracle.com/javase/tutorial/i18n/format/simpleDateFormat.html) for detailed time format syntax.

### field_delimiter [string]

The separator between columns in a row of data. Only needed by `text` and `csv` file format.

### row_delimiter [string]

The separator between rows in a file. Only needed by `text` and `csv` file format.

### partition_by [array]

Partition data based on selected fields

### partition_dir_expression [string]

If the `partition_by` is specified, we will generate the corresponding partition directory based on the partition information, and the final file will be placed in the partition directory.

Default `partition_dir_expression` is `${k0}=${v0}/${k1}=${v1}/.../${kn}=${vn}/`. `k0` is the first partition field and `v0` is the value of the first partition field.

### is_partition_field_write_in_file [boolean]

If `is_partition_field_write_in_file` is `true`, the partition field and the value of it will be written into data file.

For example, if you want to write a Hive Data File, Its value should be `false`.

### sink_columns [array]

Which columns need be written to file, default value is all the columns get from `Transform` or `Source`.
The order of the fields determines the order in which the file is actually written.

### is_enable_transaction [boolean]

If `is_enable_transaction` is true, we will ensure that data will not be lost or duplicated when it is written to the target directory.

Please note that, If `is_enable_transaction` is `true`, we will auto add `${transactionId}_` in the head of the file.

Only support `true` now.

### batch_size [int]

The maximum number of rows in a file. For SeaTunnel Engine, the number of lines in the file is determined by `batch_size` and `checkpoint.interval` jointly decide. If the value of `checkpoint.interval` is large enough, sink writer will write rows in a file until the rows in the file larger than `batch_size`. If `checkpoint.interval` is small, the sink writer will create a new file when a new checkpoint trigger.

### changelog_mode [enum]

The changelog mode of the sink writer, support [`APPEND_ONLY`、`APPEND_ON_DUPLICATE_UPDATE`、`APPEND_ON_DUPLICATE_DELETE`].

### changelog_buffer_flush_size [int]

Flush buffer to s3 size of redshift changelog

### changelog_buffer_flush_interval_ms [int]

Flush buffer to s3 interval of redshift changelog

### redshift_table [string]

The target table name of redshift changelog.

### redshift_table_primary_keys [array]

The primary keys of the buffer/target-table, only needed by `APPEND_ON_DUPLICATE_UPDATE` and `APPEND_ON_DUPLICATE_DELETE` changelog mode.

### redshift_temporary_table_name [string]

The temporary table of redshift changelog.

### redshift_s3_iam_role [string]

The s3 iam role of redshift changelog.

### common options

Sink plugin common parameters, please refer to [Sink Common Options](common-options.md) for details.

## Example

For text file format

```hocon

  S3Redshift {
    jdbc_url = "jdbc:redshift://xxx.amazonaws.com.cn:5439/xxx"
    jdbc_user = "xxx"
    jdbc_password = "xxxx"
    execute_sql="COPY table_name FROM 's3://test${path}' IAM_ROLE 'arn:aws-cn:iam::xxx' REGION 'cn-north-1' removequotes emptyasnull blanksasnull maxerror 100 delimiter '|' ;"
    access_key = "xxxxxxxxxxxxxxxxx"
    secret_key = "xxxxxxxxxxxxxxxxx"
    bucket = "s3a://seatunnel-test"
    tmp_path = "/tmp/seatunnel"
    path="/seatunnel/text"
    row_delimiter="\n"
    partition_dir_expression="${k0}=${v0}"
    is_partition_field_write_in_file=true
    file_name_expression="${transactionId}_${now}"
    file_format_type = "text"
    filename_time_format="yyyy.MM.dd"
    is_enable_transaction=true
    hadoop_s3_properties {
       "fs.s3a.aws.credentials.provider" = "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    }
  }

```

For parquet file format

```hocon

  S3Redshift {
    jdbc_url = "jdbc:redshift://xxx.amazonaws.com.cn:5439/xxx"
    jdbc_user = "xxx"
    jdbc_password = "xxxx"
    execute_sql="COPY table_name FROM 's3://test${path}' IAM_ROLE 'arn:aws-cn:iam::xxx' REGION 'cn-north-1' format as PARQUET;"
    access_key = "xxxxxxxxxxxxxxxxx"
    secret_key = "xxxxxxxxxxxxxxxxx"
    bucket = "s3a://seatunnel-test"
    tmp_path = "/tmp/seatunnel"
    path="/seatunnel/parquet"
    row_delimiter="\n"
    partition_dir_expression="${k0}=${v0}"
    is_partition_field_write_in_file=true
    file_name_expression="${transactionId}_${now}"
    file_format_type = "parquet"
    filename_time_format="yyyy.MM.dd"
    is_enable_transaction=true
    hadoop_s3_properties {
       "fs.s3a.aws.credentials.provider" = "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    }
  }

```

For orc file format

```hocon

  S3Redshift {
    jdbc_url = "jdbc:redshift://xxx.amazonaws.com.cn:5439/xxx"
    jdbc_user = "xxx"
    jdbc_password = "xxxx"
    execute_sql="COPY table_name FROM 's3://test${path}' IAM_ROLE 'arn:aws-cn:iam::xxx' REGION 'cn-north-1' format as ORC;"
    access_key = "xxxxxxxxxxxxxxxxx"
    secret_key = "xxxxxxxxxxxxxxxxx"
    bucket = "s3a://seatunnel-test"
    tmp_path = "/tmp/seatunnel"
    path="/seatunnel/orc"
    row_delimiter="\n"
    partition_dir_expression="${k0}=${v0}"
    is_partition_field_write_in_file=true
    file_name_expression="${transactionId}_${now}"
    file_format_type = "orc"
    filename_time_format="yyyy.MM.dd"
    is_enable_transaction=true
    hadoop_s3_properties {
       "fs.s3a.aws.credentials.provider" = "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    }
  }

```

Support write cdc changelog event(APPEND_ON_DUPLICATE_UPDATE/APPEND_ON_DUPLICATE_DELETE).

*Using Redshift COPY sql import s3 file into tmp table, and use Redshift MERGE sql merge tmp table data into target table.*
- [Redshift TEMPORARY Table](https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_TABLE_NEW.html)
- [Redshift COPY SQL](https://docs.aws.amazon.com/redshift/latest/dg/r_COPY.html)
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
    file_format_type = "text"
    field_delimiter = "\t"
    row_delimiter = "\n"
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

