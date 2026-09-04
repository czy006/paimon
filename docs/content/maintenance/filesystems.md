---
title: "Filesystems"
weight: 1
type: docs
aliases:
- /maintenance/filesystems.html
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Filesystems

Apache Paimon utilizes the same pluggable file systems as Apache Flink. Users can follow the
[standard plugin mechanism](https://nightlies.apache.org/flink/flink-docs-stable/docs/deployment/filesystems/plugins/)
to configure the plugin structure if using Flink as compute engine. However, for other engines like Spark
or Hive, the provided opt jars (by Flink) may get conflicts and cannot be used directly. It is not convenient
for users to fix class conflicts, thus Paimon provides the self-contained and engine-unified
FileSystem pluggable jars for user to query tables from Spark/Hive side.

## Supported FileSystems

| FileSystem                   | URI Scheme | Pluggable | Description                                                            |
|:-----------------------------|:-----------|-----------|:-----------------------------------------------------------------------|
| Local File System            | file://    | N         | Built-in Support                                                       |
| HDFS                         | hdfs://    | N         | Built-in Support, ensure that the cluster is in the hadoop environment |
| Aliyun OSS                   | oss://     | Y         |                                                                        |
| S3                           | s3://      | Y         |                                                                        |
| Tencent Cloud Object Storage | cosn://    | Y         |                                                                        |
| Microsoft Azure Storage      | abfs://    | Y         |                                                                        |
| Huawei OBS                   | obs://     | Y         |                                                                        |
| Google Cloud Storage         | gs://      | Y         |                                                                        |

## Dependency

We recommend you to download the jar directly: [Download Link]({{< ref "project/download#filesystem-jars" >}}).

You can also manually build bundled jar from the source code.

To build from source code, [clone the git repository]({{< github_repo >}}).

Build shaded jar with the following command.

```bash
mvn clean install -DskipTests
```

You can find the shaded jars under
`./paimon-filesystems/paimon-${fs}/target/paimon-${fs}-{{< version >}}.jar`.

## HDFS

You don't need any additional dependencies to access HDFS because you have already taken care of the Hadoop dependencies.

### HDFS Configuration

For HDFS, the most important thing is to be able to read your HDFS configuration.

{{< tabs "hdfs conf" >}}

{{< tab "Flink" >}}

You may not have to do anything, if you are in a hadoop environment. Otherwise pick one of the following ways to
configure your HDFS:

1. Set environment variable `HADOOP_HOME` or `HADOOP_CONF_DIR`.
2. Configure `'hadoop-conf-dir'` in the paimon catalog.
3. Configure Hadoop options through prefix `'hadoop.'` in the paimon catalog.

The first approach is recommended.

If you do not want to include the value of the environment variable, you can configure `hadoop-conf-loader` to `option`.

{{< /tab >}}

{{< tab "Hive/Spark" >}}

HDFS Configuration is available directly through the computation cluster, see cluster configuration of Hive and Spark for details.

{{< /tab >}}

{{< /tabs >}}

### Hadoop-compatible file systems (HCFS)

All Hadoop file systems are automatically available when the Hadoop libraries are on the classpath.

This way, Paimon seamlessly supports all of Hadoop file systems implementing the `org.apache.hadoop.fs.FileSystem`
interface, and all Hadoop-compatible file systems (HCFS).

- HDFS
- Alluxio (see configuration specifics below)
- XtreemFS
- …

The Hadoop configuration has to have an entry for the required file system implementation in the `core-site.xml` file.

For Alluxio support add the following entry into the core-site.xml file:

```xml
<property>
  <name>fs.alluxio.impl</name>
  <value>alluxio.hadoop.FileSystem</value>
</property>
```

### Kerberos

{{< tabs "Kerberos" >}}

{{< tab "Flink" >}}

Configure the following options in your catalog configuration:

- security.kerberos.login.keytab: Absolute path to a Kerberos keytab file that contains the user credentials.
  Please make sure it is copied to each machine.
- security.kerberos.login.principal: Kerberos principal name associated with the keytab.

And configure the following option in the program's java property:

- java.security.krb5.conf: Absolute path to the Kerberos configuration file.
  Please make sure it is copied to each machine.

{{< /tab >}}

{{< tab "Spark" >}}

It is recommended to use [Spark Kerberos Keytab](https://spark.apache.org/docs/latest/security.html#using-a-keytab).

{{< /tab >}}

{{< tab "Hive" >}}

An intuitive approach is to configure Hive's kerberos authentication.

{{< /tab >}}

{{< tab "Trino/JavaAPI" >}}

Configure the following three options in your catalog configuration:

- security.kerberos.login.keytab: Absolute path to a Kerberos keytab file that contains the user credentials.
  Please make sure it is copied to each machine.
- security.kerberos.login.principal: Kerberos principal name associated with the keytab.
- security.kerberos.login.use-ticket-cache: True or false, indicates whether to read from your Kerberos ticket cache.

For JavaAPI:
```
SecurityContext.install(catalogOptions);
```

{{< /tab >}}

{{< /tabs >}}

### HDFS HA

Ensure that `hdfs-site.xml` and `core-site.xml` contain the necessary [HA configuration](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HDFSHighAvailabilityWithNFS.html).

### HDFS ViewFS

Ensure that `hdfs-site.xml` and `core-site.xml` contain the necessary [ViewFs configuration](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/ViewFs.html).

## OSS

{{< stable >}}

Download [paimon-oss-{{< version >}}.jar](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-oss/{{< version >}}/paimon-oss-{{< version >}}.jar).

{{< /stable >}}

{{< unstable >}}

Download [paimon-oss-{{< version >}}.jar](https://repository.apache.org/snapshots/org/apache/paimon/paimon-oss/{{< version >}}/).

{{< /unstable >}}

{{< tabs "oss" >}}

{{< tab "Flink" >}}

{{< hint info >}}
If you have already configured [oss access through Flink](https://nightlies.apache.org/flink/flink-docs-stable/docs/deployment/filesystems/oss/) (Via Flink FileSystem),
here you can skip the following configuration.
{{< /hint >}}

Put `paimon-oss-{{< version >}}.jar` into `lib` directory of your Flink home, and create catalog:

```sql
CREATE CATALOG my_catalog WITH (
    'type' = 'paimon',
    'warehouse' = 'oss://<bucket>/<path>',
    'fs.oss.endpoint' = 'oss-cn-hangzhou.aliyuncs.com',
    'fs.oss.accessKeyId' = 'xxx',
    'fs.oss.accessKeySecret' = 'yyy'
);
```

{{< /tab >}}

{{< tab "Spark" >}}

{{< hint info >}}
If you have already configured oss access through Spark (Via Hadoop FileSystem), here you can skip the following configuration.
{{< /hint >}}

Place `paimon-oss-{{< version >}}.jar` together with `paimon-spark-{{< version >}}.jar` under Spark's jars directory, and start like

```shell
spark-sql \
  --conf spark.sql.catalog.paimon=org.apache.paimon.spark.SparkCatalog \
  --conf spark.sql.catalog.paimon.warehouse=oss://<bucket>/<path> \
  --conf spark.sql.catalog.paimon.fs.oss.endpoint=oss-cn-hangzhou.aliyuncs.com \
  --conf spark.sql.catalog.paimon.fs.oss.accessKeyId=xxx \
  --conf spark.sql.catalog.paimon.fs.oss.accessKeySecret=yyy
```

{{< /tab >}}

{{< tab "Hive" >}}

{{< hint info >}}
If you have already configured oss access through Hive (Via Hadoop FileSystem), here you can skip the following configuration.
{{< /hint >}}

NOTE: You need to ensure that Hive metastore can access `oss`.

Place `paimon-oss-{{< version >}}.jar` together with `paimon-hive-connector-{{< version >}}.jar` under Hive's auxlib directory, and start like

```sql
SET paimon.fs.oss.endpoint=oss-cn-hangzhou.aliyuncs.com;
SET paimon.fs.oss.accessKeyId=xxx;
SET paimon.fs.oss.accessKeySecret=yyy;
```

And read table from hive metastore, table can be created by Flink or Spark, see [Catalog with Hive Metastore]({{< ref "flink/sql-ddl" >}})
```sql
SELECT * FROM test_table;
SELECT COUNT(1) FROM test_table;
```


{{< /tab >}}
{{< tab "Trino" >}}

From version 0.8, paimon-trino uses trino filesystem as basic file read and write system. We strongly recommend you to use jindo-sdk in trino.

You can find [How to config jindo sdk on trino](https://github.com/aliyun/alibabacloud-jindodata/blob/master/docs/user/4.x/4.6.x/4.6.12/oss/presto/jindosdk_on_presto.md) here.
Please note that:
* Use paimon to replace hive-hadoop2 when you decompress the plugin jar and find location to put in.
* You can specify the `core-site.xml` in `paimon.properties` on configuration [hive.config.resources](https://trino.io/docs/current/connector/hive.html#hdfs-configuration).
* Presto and Jindo use the same configuration method.

{{< /tab >}}
{{< /tabs >}}

If you environment has jindo sdk dependencies, you can use Jindo Fs to connect OSS. Jindo has better read and write efficiency.

{{< stable >}}
Download [paimon-jindo-{{< version >}}.jar](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-jindo/{{< version >}}/paimon-jindo-{{< version >}}.jar).
{{< /stable >}}
{{< unstable >}}
Download [paimon-jindo-{{< version >}}.jar](https://repository.apache.org/snapshots/org/apache/paimon/paimon-jindo/{{< version >}}/).
{{< /unstable >}}

## S3

{{< stable >}}

Download [paimon-s3-{{< version >}}.jar](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-s3/{{< version >}}/paimon-s3-{{< version >}}.jar).

{{< /stable >}}

{{< unstable >}}

Download [paimon-s3-{{< version >}}.jar](https://repository.apache.org/snapshots/org/apache/paimon/paimon-s3/{{< version >}}/).

{{< /unstable >}}

{{< tabs "s3" >}}

{{< tab "Flink" >}}

{{< hint info >}}
If you have already configured [s3 access through Flink](https://nightlies.apache.org/flink/flink-docs-stable/docs/deployment/filesystems/s3/) (Via Flink FileSystem),
here you can skip the following configuration.
{{< /hint >}}

Put `paimon-s3-{{< version >}}.jar` into `lib` directory of your Flink home, and create catalog:

```sql
CREATE CATALOG my_catalog WITH (
    'type' = 'paimon',
    'warehouse' = 's3://<bucket>/<path>',
    's3.endpoint' = 'your-endpoint-hostname',
    's3.access-key' = 'xxx',
    's3.secret-key' = 'yyy'
);
```

{{< /tab >}}

{{< tab "Spark" >}}

{{< hint info >}}
If you have already configured s3 access through Spark (Via Hadoop FileSystem), here you can skip the following configuration.
{{< /hint >}}

Place `paimon-s3-{{< version >}}.jar` together with `paimon-spark-{{< version >}}.jar` under Spark's jars directory, and start like

```shell
spark-sql \
  --conf spark.sql.catalog.paimon=org.apache.paimon.spark.SparkCatalog \
  --conf spark.sql.catalog.paimon.warehouse=s3://<bucket>/<path> \
  --conf spark.sql.catalog.paimon.s3.endpoint=your-endpoint-hostname \
  --conf spark.sql.catalog.paimon.s3.access-key=xxx \
  --conf spark.sql.catalog.paimon.s3.secret-key=yyy
```

{{< /tab >}}

{{< tab "Hive" >}}

{{< hint info >}}
If you have already configured s3 access through Hive ((Via Hadoop FileSystem)), here you can skip the following configuration.
{{< /hint >}}

NOTE: You need to ensure that Hive metastore can access `s3`.

Place `paimon-s3-{{< version >}}.jar` together with `paimon-hive-connector-{{< version >}}.jar` under Hive's auxlib directory, and start like

```sql
SET paimon.s3.endpoint=your-endpoint-hostname;
SET paimon.s3.access-key=xxx;
SET paimon.s3.secret-key=yyy;
```

And read table from hive metastore, table can be created by Flink or Spark, see [Catalog with Hive Metastore]({{< ref "flink/sql-ddl" >}})
```sql
SELECT * FROM test_table;
SELECT COUNT(1) FROM test_table;
```

{{< /tab >}}

{{< tab "Trino" >}}

Paimon use shared trino filesystem as basic read and write system.

Please refer to [Trino S3](https://trino.io/docs/current/object-storage/file-system-s3.html) to config s3 filesystem in trino.

{{< /tab >}}

{{< /tabs >}}

### S3 Compliant Object Stores

The S3 Filesystem also support using S3 compliant object stores such as MinIO, Tencent's COS and IBM’s Cloud Object
Storage. Just configure your endpoint to the provider of the object store service.

```yaml
s3.endpoint: your-endpoint-hostname
```

### Configure Path Style Access

Some S3 compliant object stores might not have virtual host style addressing enabled by default, for example when using Standalone MinIO for testing purpose.
In such cases, you will have to provide the property to enable path style access.

```yaml
s3.path.style.access: true
```

### S3A Performance

[Tune Performance](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/performance.html) for `S3AFileSystem`.

If you encounter the following exception:
```shell
Caused by: org.apache.http.conn.ConnectionPoolTimeoutException: Timeout waiting for connection from pool.
```
Try to configure this in catalog options: `fs.s3a.connection.maximum=1000`.

## S3 Native (Experimental)

`paimon-s3-native` is an alternative S3 filesystem implemented directly on the AWS SDK v2 with
**no Hadoop dependency**. Compared to `paimon-s3` (Hadoop S3A + AWS SDK v1) it offers
multipart uploads with bounded concurrent part transfers, vectored reading for columnar
formats (`pread`-based, parallel range GETs), and a smaller dependency surface.

{{< unstable >}}

Download [paimon-s3-native-{{< version >}}.jar](https://repository.apache.org/snapshots/org/apache/paimon/paimon-s3-native/{{< version >}}/).

{{< /unstable >}}

{{< hint warning >}}
`paimon-s3-native` and `paimon-s3` both register the `s3://` scheme and **must not be deployed
at the same time** — drop exactly one of the two jars into your engine's classpath. Keeping both
leads to a duplicate-scheme error during `FileIO` discovery. Rolling back to `paimon-s3` is
simply swapping the jar back; the option keys below are compatible.
{{< /hint >}}

Deployment and configuration follow the [S3](#s3) section above (Flink `lib/`, Spark `jars/`,
Hive `auxlib`) — just use `paimon-s3-native-{{< version >}}.jar` instead of `paimon-s3-{{< version >}}.jar`.
The `s3.*` option keys (`s3.endpoint`, `s3.access-key`, `s3.secret-key`, `s3.path-style-access`,
`s3.region`, ...) are identical; common `fs.s3a.*` keys (`fs.s3a.endpoint`,
`fs.s3a.access.key`, `fs.s3a.multipart.size`, `fs.s3a.connection.maximum`, ...) are accepted as
aliases, and unrecognized `fs.s3a.*` keys are reported with a warning and ignored.

Additional options:

| Option | Default | Description |
| :- | :- | :- |
| `s3.upload.min.part.size` | 5mb | Multipart part size (5MB–5GB); parts upload concurrently. |
| `s3.upload.max.concurrent.uploads` | CPU cores | Maximum in-flight part uploads per stream. |
| `s3.read.buffer.size` | 256kb | Input stream read-ahead buffer (minimum 256KB). |
| `s3.connection.max` | 50 | HTTP connection pool size for both clients. |
| `s3.retry.max-num-retries` | 3 | Retries per request (exponential backoff, 100ms base / 20s cap). |
| `s3.upload.tmp.dir` | `java.io.tmpdir` | Local directory buffering parts before upload. |
| `s3.multipart.threshold` | 1.5 | Multipart starts only above part size x this factor; smaller objects use a single PutObject. |
| `s3.checksum-enabled` | false | Send MD5 Content-MD5 for parts and whole objects. |
| `s3.write.storage-class` | none | Storage class for new objects (e.g. `GLACIER`, `INTELLIGENT_TIERING`). |
| `s3.write.tags` | none | Tags for new objects, `k1:v1,k2:v2`. |
| `s3.acl` | none | Canned ACL for new objects. |
| `s3.delete.batch-size` | 1000 | Keys per DeleteObjects request (1-1000). |
| `s3.delete.num-threads` | CPU cores | Concurrent DeleteObjects requests during bulk deletes. |
| `s3.sse.type` | none | Server-side encryption: `none`, `s3`, `kms`, `dsse-kms` or `custom` (SSE-C, HTTPS-only). |
| `s3.sse.key` | none | KMS key id/alias, or the base64 AES-256 key for `custom`. |
| `s3.sse.md5` | none | Base64 MD5 of the SSE-C key (required with `custom`). |

Notes:

- Buckets expose S3A-compatible directory markers (0-byte `key/` objects), so tooling
  interoperates with data written by `paimon-s3`.
- Rename is copy+delete and not atomic, exactly like S3A; objects larger than 5GB are copied
  via multipart `UploadPartCopy`.
- Region resolution follows the AWS SDK default chain (`s3.region` option, `AWS_REGION`
  environment variable, `~/.aws/config`, EC2 metadata) and fails fast when nothing resolves —
  set `s3.region` explicitly for S3-compatible stores.

## Google Cloud Storage

{{< stable >}}

Download [paimon-gs-{{< version >}}.jar](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-gs/{{< version >}}/paimon-gs-{{< version >}}.jar).

{{< /stable >}}

{{< unstable >}}

Download [paimon-gs-{{< version >}}.jar](https://repository.apache.org/snapshots/org/apache/paimon/paimon-gs/{{< version >}}/).

{{< /unstable >}}

{{< tabs "gs" >}}

{{< tab "Flink" >}}

{{< hint info >}}
If you have already configured [gcs access through Flink](https://nightlies.apache.org/flink/flink-docs-release-2.0/docs/deployment/filesystems/gcs/) (Via Flink FileSystem),
here you can skip the following configuration.
{{< /hint >}}

Put `paimon-gs-{{< version >}}.jar` into `lib` directory of your Flink home, and create catalog:

```sql
CREATE CATALOG my_catalog WITH (
    'type' = 'paimon',
    'warehouse' = 'gs://<bucket>/<path>',
    'fs.gs.auth.type' = 'SERVICE_ACCOUNT_JSON_KEYFILE',
    'fs.gs.auth.service.account.json.keyfile' = '/path/to/service-account-.json'
);
```

{{< /tab >}}

{{< /tabs >}}

## Microsoft Azure Storage

{{< stable >}}

Download [paimon-azure-{{< version >}}.jar](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-azure/{{< version >}}/paimon-azure-{{< version >}}.jar).

{{< /stable >}}

{{< unstable >}}

Download [paimon-azure-{{< version >}}.jar](https://repository.apache.org/snapshots/org/apache/paimon/paimon-azure/{{< version >}}/).

{{< /unstable >}}

{{< tabs "azure" >}}

{{< tab "Flink" >}}

{{< hint info >}}
If you have already configured [azure access through Flink](https://nightlies.apache.org/flink/flink-docs-release-2.0/docs/deployment/filesystems/azure/) (Via Flink FileSystem),
here you can skip the following configuration.
{{< /hint >}}

Put `paimon-azure-{{< version >}}.jar` into `lib` directory of your Flink home, and create catalog:

```sql
CREATE CATALOG my_catalog WITH (
   'type' = 'paimon',
   'warehouse' = 'wasb://,<container>@<account>.blob.core.windows.net/<path>',
   'fs.azure.account.key.Account.blob.core.windows.net' = 'yyy'
);
```

{{< /tab >}}

{{< tab "Spark" >}}

{{< hint info >}}
If you have already configured azure access through Spark (Via Hadoop FileSystem), here you can skip the following configuration.
{{< /hint >}}

Place `paimon-azure-{{< version >}}.jar` together with `paimon-spark-{{< version >}}.jar` under Spark's jars directory, and start like

```shell
spark-sql \
  --conf spark.sql.catalog.paimon=org.apache.paimon.spark.SparkCatalog \
  --conf spark.sql.catalog.paimon.warehouse=wasb://,<container>@<account>.blob.core.windows.net/<path> \
  --conf fs.azure.account.key.Account.blob.core.windows.net=yyy \
```

{{< /tab >}}

{{< /tabs >}}

## OBS

{{< stable >}}

Download [paimon-obs-{{< version >}}.jar](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-obs/{{< version >}}/paimon-obs-{{< version >}}.jar).

{{< /stable >}}

{{< unstable >}}

Download [paimon-obs-{{< version >}}.jar](https://repository.apache.org/snapshots/org/apache/paimon/paimon-obs/{{< version >}}/).

{{< /unstable >}}

{{< tabs "obs" >}}

{{< tab "Flink" >}}

{{< hint info >}}
If you have already configured [obs access through Flink](https://nightlies.apache.org/flink/flink-docs-stable/docs/deployment/filesystems/s3/) (Via Flink FileSystem),
here you can skip the following configuration.
{{< /hint >}}

Put `paimon-obs-{{< version >}}.jar` into `lib` directory of your Flink home, and create catalog:

```sql
CREATE CATALOG my_catalog WITH (
    'type' = 'paimon',
    'warehouse' = 'obs://<bucket>/<path>',
    'fs.obs.endpoint' = 'obs-endpoint-hostname',
    'fs.obs.access.key' = 'xxx',
    'fs.obs.secret.key' = 'yyy'
);
```

{{< /tab >}}

{{< tab "Spark" >}}

{{< hint info >}}
If you have already configured obs access through Spark (Via Hadoop FileSystem), here you can skip the following configuration.
{{< /hint >}}

Place `paimon-obs-{{< version >}}.jar` together with `paimon-spark-{{< version >}}.jar` under Spark's jars directory, and start like

```shell
spark-sql \
  --conf spark.sql.catalog.paimon=org.apache.paimon.spark.SparkCatalog \
  --conf spark.sql.catalog.paimon.warehouse=obs://<bucket>/<path> \
  --conf spark.sql.catalog.paimon.fs.obs.endpoint=obs-endpoint-hostname \
  --conf spark.sql.catalog.paimon.fs.obs.access.key=xxx \
  --conf spark.sql.catalog.paimon.fs.obs.secret.key=yyy
```

{{< /tab >}}

{{< tab "Hive" >}}

{{< hint info >}}
If you have already configured obs access through Hive ((Via Hadoop FileSystem)), here you can skip the following configuration.
{{< /hint >}}

NOTE: You need to ensure that Hive metastore can access `obs`.

Place `paimon-obs-{{< version >}}.jar` together with `paimon-hive-connector-{{< version >}}.jar` under Hive's auxlib directory, and start like

```sql
SET paimon.fs.obs.endpoint=obs-endpoint-hostname;
SET paimon.fs.obs.access.key=xxx;
SET paimon.fs.obs.secret.key=yyy;
```

And read table from hive metastore, table can be created by Flink or Spark, see [Catalog with Hive Metastore]({{< ref "flink/sql-ddl" >}})
```sql
SELECT * FROM test_table;
SELECT COUNT(1) FROM test_table;
```

{{< /tab >}}

{{< /tabs >}}
