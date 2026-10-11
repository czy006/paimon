<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# 分区冷数据归档使用指南（开发分支）

> 更新日期：2026-10-11。开发基线：`release-1.3.2`，分支 `codex/archive-partitions-132`，构建使用 JDK 11。
> 本 worktree 已提供 Spark `CALL archive_partitions` 和 core `PartitionArchiver`，可按本文在隔离环境构建和调用。入口、安全检查、复制校验、pin 和提交确认已经实现；生产放行仍取决于部署、完整生命周期和资源预算验收。官方 Paimon 1.3.2 二进制不包含此新增入口，必须使用本开发分支构建产物。

本地提交后的新一轮实现审查及修正结果见 [代码审查记录](tasks/evidence/AP-REVIEW.md)。表类型和catalog发布协议的当前边界以第6节为准。

DV、HASH_DYNAMIC、KEY_DYNAMIC 和 POSTPONE_MODE 的当前限制及后续扩展前提见 [DV与动态分桶限制记录](archive-partitions-dv-dynamic-limitations.md)。该记录是未实施的可行性方案，不表示当前 CALL 已支持这些类型。

## 1. 功能用途与当前可用范围

目标是将不再变更的完整分区从热存储复制到在线冷存储，例如本地文件系统 → MinIO，再通过快照原子替换文件引用。迁移数据文件时不合并、排序或重新编码，因此不需要执行数据 compaction。仍有全量复制、校验和 manifest 维护成本。

下图表示当前实现的引用迁移原理。

```text
                    表根：schema / snapshot / manifest 不搬迁
                                       │
                  ┌────────────────────┴────────────────────┐
             保留的旧 snapshot/tag                   新 COMPACT snapshot
                  │                                        │
                  ▼                             ┌──────────┴──────────┐
             热端原分区文件                     热端未归档文件      冷端新副本
                  │                                                   ▲
                  └──── 原字节复制 → 关闭写流 → 长度及 SHA-256 校验 ─────┘
```

新快照中的 `COMPACT` 是现有元数据引用替换的提交类型，不表示运行了数据压实。每个分区的全部 bucket 一次提交；多个分区不是一个事务。旧文件仍供保留的历史引用读取，只能在引用不再受保护后按 Paimon 生命周期清理，不能归档后直接删除。

| 能力 | 当前状态 |
|---|---|
| URI 词法安全校验 | 已实现并验证；不等于已验证根目录所有权、登记和 IO 可用性 |
| 本地热数据 + 独立本地冷目录 | 原探针9例；当前真实CALL28例通过 |
| 本地热数据 + MinIO 冷对象 | 原探针9例；当前真实CALL28例通过，隔离MinIO容器 |
| snapshot/tag、混合查询、迁移后同分区写入、跨冷热 PK 更新/删除 | 在上述 data-only 测试矩阵内验证通过 |
| CALL、表/历史/目录安全检查、pin、提交确认及停止规则 | 已实现；维护冻结和恢复仍需运维协调 |
| sidecar/embedded index 过滤 | 本地和MinIO真实CALL已验证bitmap；不支持无法通过reader路径往返校验的sidecar位置 |
| 跨介质 GC/drop、流消费/缓存、分布式 Spark、其他介质或迁移方向 | 尚需独立验收，不能从 CALL 查询通过推断生产生命周期已放行 |

完整设计见 [Phase 1 spec](archive-partitions-phase1-spec.md)，真实入口证据见 [AP-CALL实现与验证](tasks/evidence/AP-CALL.md)，原探针证据见 [混合存储实测](tasks/evidence/AP-PROBE-mixed-storage.md)，实施状态见 [任务清单](tasks/todo.md)。

## 2. 当前可以使用：复现本地与 MinIO 验证

### 2.1 环境与依赖

使用包含新增测试文件的开发 worktree；只检出官方 `release-1.3.2` tag 不会获得这些测试或新功能。下面路径是本机已验证的安装位置，其他机器应替换为自身路径。

```shell
cd /Users/conradjam/.codex/worktrees/archive-partitions-132/paimon
env JAVA_HOME=/Library/Java/JavaVirtualMachines/temurin-11.jdk/Contents/Home \
  /Users/conradjam/software/apache-maven-3.9.16/bin/mvn -v
```

确认输出为 JDK 11；本次实际为 Temurin 11.0.32.1、Maven 3.9.16、macOS arm64。`spark3` 测试实际使用 Spark 3.5.6 / Scala 2.12、`local[2]`，不运行 Spark 4 profile。源码兼容目标仍为 Java 8，不能据此改用其他 JDK 跳过验证。

首次运行或依赖模块已修改时，先安装 reactor 依赖。相关模块包含 shaded JAR，必须清理旧打包产物以避免旧类遮蔽新注册表；这里的 `-DskipTests` 仅用于准备依赖，不算测试通过证据：

```shell
env JAVA_HOME=/Library/Java/JavaVirtualMachines/temurin-11.jdk/Contents/Home \
  /Users/conradjam/software/apache-maven-3.9.16/bin/mvn -B -ntp \
  -Pspark3 \
  -pl paimon-spark/paimon-spark-3.5 -am \
  -DskipTests clean install
```

Spark 3.5连接器输出为 `paimon-spark/paimon-spark-3.5/target/paimon-spark-3.5-1.3.2.jar`；文件名沿用开发基线版本，内容是本分支自定义构建，不能与官方同名包混用。安装到Spark运行环境时避免同时加载旧Paimon连接器。

### 2.2 普通文件系统测试（不需要 Docker）

```shell
env JAVA_HOME=/Library/Java/JavaVirtualMachines/temurin-11.jdk/Contents/Home \
  /Users/conradjam/software/apache-maven-3.9.16/bin/mvn -B -ntp \
  -Pspark3 -pl paimon-spark/paimon-spark-ut \
  -Dtest=ArchivePartitionsProcedureTest \
  -DwildcardSuites=org.apache.paimon.spark.procedure.ExpirePartitionsProcedureTest \
  test
```

应执行真实CALL Java本地测试28例，以及已有Scala procedure回归11例。测试创建临时表根和独立冷目录，不接受业务表路径参数。原data-only探针可单独用-Dtest=LocalPartitionRelocationTest回归。

### 2.3 MinIO 混合存储测试

先确保 Docker 已启动且当前用户能访问其 socket。以下是本机 Docker Desktop 的实际配置；socket、API 和镜像覆盖是本机环境适配，不是所有机器的固定要求。

```shell
env JAVA_HOME=/Library/Java/JavaVirtualMachines/temurin-11.jdk/Contents/Home \
  DOCKER_HOST=unix:///Users/conradjam/.docker/run/docker.sock \
  /Users/conradjam/software/apache-maven-3.9.16/bin/mvn -B -ntp \
  -Pspark3 -pl paimon-spark/paimon-spark-ut \
  -Dtest=ArchivePartitionsS3ITCase \
  -Dapi.version=1.44 \
  -Dpaimon.test.minio.image=alpine/minio:RELEASE.2025-10-15T17-29-55Z \
  test-compile surefire:test@integration-tests
```

真实CALL的MinIO Java IT继承本地28个场景，已实际通过；具体结果见审查证据。Testcontainers创建隔离容器、随机bucket/凭据/端口，不连接既有业务MinIO；元数据及热数据在本地临时目录，冷对象在测试bucket。夹具清理自有资源，不能手工清空用户bucket。原探针类为MixedStoragePartitionRelocationS3ITCase。

仓库默认 MinIO 镜像在本机拉取失败，因此显式使用已有 ARM 镜像。Docker API 兼容性问题通过本机已验证的 API 1.44 解决；保留容器启动检查和 Ryuk 清理，不用关闭安全检查来制造通过结果。镜像配置可按受信任的测试环境调整，调整后需重新记录版本和执行结果。

### 2.4 测试验证了什么

每个后端均覆盖 append fixed（2 buckets）、append unaware（`bucket=-1`）、PK fixed（2 buckets），分别使用 Parquet、ORC、Avro，总计 18 个参数化场景。

- 核对完整行多重集，保留 append 重复行；不是只比较 `COUNT(*)`。
- 单次迁移恰好新增一个 `COMPACT`，`delta_record_count=0`，迁移前后 `total_record_count` 不变；未迁移分区元数据不变。
- 从持久化快照重新读取元数据，除物理名字和 `externalPath` 外保持源字段；复制前后长度与 SHA-256 相等。
- 新 SparkSession 查询最新、旧 snapshot 和旧 tag；后续写入造成 latest 与历史不同后，再次确认时间旅行结果。
- 归档分区后续新增热文件形成同分区混合存储；PK 热端更新/删除冷端同主键，最新结果及历史结果均正确。
- 精确删除测试自有冷对象后，最新查询必须报该文件缺失，历史 tag 仍可读；测试在 `finally` 恢复对象并再次核对。证明读者确实访问冷端，不会自动回退热副本。

真实CALL另覆盖多分区/复合键/非法参数/共享根、bitmap embedded/sidecar查询，PK partial-update/aggregation，默认/显式/非法idle时间。检查Maven成功和paimon-spark/paimon-spark-ut/target/surefire-reports两个CALL类的XML：tests=28、failures/errors/skipped=0；原探针每类9例。容器启动或编译成功不等于场景通过。

原探针只处理 `extraFiles` 为空的数据文件；CALL 测试通过真实 SQL 入口执行，索引场景独立验证。`write-only=true` 仅用于隔离测试，不能代替生产维护冻结。业务调用使用 procedure，不接入测试 helper。

## 3. 部署与表准备

以下配置需在隔离部署先验证；生产使用还需完成相应部署与生命周期验收。

### 3.1 让所有参与者都能访问冷热路径

在 Spark catalog **首次初始化之前**配置，例如在作业启动参数提供下列配置；这里只展示非敏感项：

```text
spark.sql.extensions=org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions
spark.sql.catalog.paimon=org.apache.paimon.spark.SparkCatalog
spark.sql.catalog.paimon.warehouse=file:///srv/paimon-warehouse
spark.sql.catalog.paimon.resolving-file-io.enabled=true
spark.sql.catalog.paimon.s3.endpoint=http://minio.example.internal:9000
spark.sql.catalog.paimon.s3.path.style.access=true
```

示例 `file:///` warehouse 仅适合单机或所有参与进程确实共享同一文件系统的环境。多机器上同名本地目录不是共享存储；集群不能照抄该路径或把 MinIO endpoint 设置成仅 driver 可访问的 `localhost`。生产应按部署要求使用 TLS 和受限网络。

通过部署的密钥机制提供 catalog 的 `s3.access.key`、`s3.secret.key` 等认证配置；不要把真实密钥写进 SQL 文件、版本库、目标 URI 或日志。该分支 MinIO 的 endpoint/path-style 键可参考 [官方文件系统说明](https://paimon.apache.org/docs/1.3/maintenance/filesystems/#s3-compliant-object-stores)。

同时满足以下条件：

- 安装与 Spark/Scala、Paimon 版本匹配的连接器，以及可被该运行环境发现的 S3 FileIO 插件；不能只向 Flink 目录放 JAR 后认为 Spark 可用。
- driver、executor、所有查询端和 expire/orphan/drop 执行端均能解析并访问冷热 URI。`resolving-file-io.enabled` 默认关闭，只配置归档 driver 不够。
- 验证实际表的 FileIO、完整 CatalogContext、权限、网络和随机读取能力；不要仅凭配置字符串推断成功，也不要只用 table options 重建上下文以“继承”catalog 凭据。
- catalog 已加载后再 `SET` 不保证刷新既有 FileIO；部署时从配置正确的新 catalog/进程开始并实际验证。

### 3.2 登记本表独占冷目录

先查看现有属性，确认历史外部根和写入策略：

```sql
SHOW TBLPROPERTIES paimon.db.orders;
```

目标根必须专属于一张表，例如 `s3://cold-bucket/paimon/db/orders`，或独立本地目录 `file:///srv/paimon-cold/db/orders`。不能使用 bucket 根、warehouse 根、表目录自身、与表目录互为祖先的路径或与其他表共享的前缀。`DROP TABLE` 可能递归删除登记根；目录为空不证明拥有独占所有权。

**仅当该表尚无其他登记根时**，准备配置示例为：

```sql
ALTER TABLE paimon.db.orders SET TBLPROPERTIES (
  'data-file.external-paths' = 's3://cold-bucket/paimon/db/orders',
  'data-file.external-paths.strategy' = 'none'
);
```

已有登记根时，应先保留完整原列表，再按逗号分隔追加所需根，不能用上述单值覆盖原列表。即使当前数据不在某旧根，历史引用及清理仍可能依赖登记；不要自行移除它。登记需要持久写入 schema，不能只用 session 临时 options。procedure 不会静默执行 `ALTER TABLE`。

`strategy=none` 保持普通后续写入使用表内路径，**它本身不会归档已有文件**。其他策略改变写入选址，不是该归档方案的替代调用。规范化绝对 URI 不得带 query、fragment、userinfo 或越界路径；本地使用 `file:///绝对路径`。符号链接、别名和真实存储所有权仍需另行检查。

## 4. 维护窗口与 CALL

### 4.1 调用前的必要条件

1. 确认第 6 节白名单和历史门槛，准备受限权限、足够冷端容量及临时双份数据空间。
2. 冻结目标分区所有写入，包括迟到 append、upsert、delete、overwrite、compaction 和 rescale；最保守做法是暂停该表所有写入任务，避免其他分区提交触发维护。
3. 暂停该表 snapshot/partition 过期、orphan 清理及其他后台清理，并等待已在执行的任务结束。复制期间尚未发布的冷对象不受当前快照保护。
4. 不并发修改 schema/布局/branch/tag，不 rollback/drop；同一张表同一窗口只运行一个归档 CALL。
5. 记录待归档分区、迁移前 snapshot/tag 与固定预期查询结果。确认源保留策略足以覆盖既有读者；普通批查询不会无限保护旧文件。

实现会在复制前创建并确认本次操作专属、无 TTL 的源 pin tag，在明确结束后只释放自己的 pin。UNKNOWN、INCOMPLETE 或维护违约时保留 pin 并停止后续分区。pin 不替代维护冻结，也不保证数字 snapshot 文件永远保留；本阶段没有一个所有 writer/cleaner 都遵守的原生“冻结 API”。

### 4.2 参数和示例

入口为 `CALL paimon.sys.archive_partitions`：三个必填STRING参数，加一个可选STRING参数：

| 参数 | 含义与限制 |
|---|---|
| `table` | 该 catalog 内的表标识，例如 `db.orders`；不是表目录 URI |
| `partitions` | 以分号分隔的完整分区 spec；一个分区的多个键以逗号分隔，键集合必须与表分区键完全一致 |
| `target_path` | 已持久登记的单个独占目标根，不是 bucket 子目录或逗号分隔列表 |
| `partition_idle_time` | 可选，默认1d；例1d、12h、30m。非负，正值至少1ms；0s显式关闭活跃检查。省略、NULL或空白使用1d |

假设 `orders` 按字符串 `dt` 分区，归档两个日期：

```sql
-- 使用本开发分支构建的连接器，并完成维护冻结后执行。
CALL paimon.sys.archive_partitions(
  table       => 'db.orders',
  partitions  => 'dt=20240101;dt=20240102',
  target_path => 's3://cold-bucket/paimon/db/orders',
  partition_idle_time => '7d'
);
```

活跃口径沿用compact：从同一source snapshot的manifest汇总PartitionEntry.lastFileCreationTime，表示整个分区所有bucket的最后文件创建时间。只有该时间不晚于“调用检查时刻减去partition_idle_time”才移动，恰好到达阈值也符合条件；1d表示24小时。它不是分区字段日期、对象mtime，也不是最后一次SELECT时间。旧日期分区若刚回灌，仍然活跃。原字节归档保留文件creationTime，归档本身不会刷新活跃时间；后续写入/真实compaction生成新文件会刷新。

未达到阈值返回NOOP code=PARTITION_ACTIVE及last_file_creation_time/idle_cutoff，不创建pin、冷对象或新快照；同CALL其他分区继续判定。活跃检查是准入过滤，维护窗口仍须冻结writer和清理，不能据此认为系统自动封存了分区。

若表按 `dt,region` 分区，必须传完整键，例如 `partitions => 'dt=20240101,region=cn;dt=20240101,region=us'`。值必须能够按表实际分区类型解析；DATE 分区不要照抄字符串日期格式。对包含无法唯一解析的分隔符/转义的键值拒绝，不自行假定 URL 编码可作为分区转义。

不支持部分键、范围条件、通配符、空参数隐式全表、`forceCopy` 或按 bucket 单独调用。完整分区经类型转换后去重，保留首次输入顺序。不存在的分区为失败，不把不存在等同于成功空操作。

### 4.3 结果与幂等语义

返回为 `ROW<result STRING>`，每个去重后的输入分区一行；各状态和字段在 result 文本内。

| `state` | 含义 | 应对 |
|---|---|---|
| `ARCHIVED` | 已确认发布并满足完整归档检查 | 核对查询、快照及路径，再进入收尾 |
| `NOOP` | 无需替换；已在目标根的有效引用跳过 | 不应产生新快照、对象或 pin；仍核对所选分区 |
| `FAILED` | 该分区明确失败，例如不存在、校验失败 | 根据错误修复，先确认提交状态，不自行删对象 |
| `UNKNOWN` | 已尝试提交，但无法确认是否发布 | 保留维护窗口、目标对象和源 pin，按第 7 节核实 |
| `INCOMPLETE` | 发布已确认，但发现额外热文件或维护违约 | 不能当作整个分区归档成功，重新建立冻结并核实 |
| `NOT_ATTEMPTED` | 前面 UNKNOWN/维护违约导致后续停止 | 前序安全恢复后再规划 |

`ARCHIVED` 结果含 operation、source/committed snapshot、data/extra 文件数和字节数、skipped 等固定字段。文件数不是 bucket 数；字节数是有效载荷，不是总网络流量。pin 释放失败另外报告 `cleanup_required`，不把已经确认发布的结果改称未提交。

普通分区失败可以继续其他分区；参数、部署或表级门槛错误直接令 CALL 报错。多分区可能部分成功，调用方必须逐行检查，不能只看 SQL 是否返回。

幂等依据已发布的引用，而不是“目标有同名文件”。目标采用新 UUID 名字，不覆盖，不复用无法证明来源和内容的残留；缺失被引用的冷文件会失败，不自动回退热端。当前不设计跨进程复制进度续传，失败重试可能重新传输。

## 5. 如何检查 snapshot 和查询

以下查询使用本分支已有 Spark SQL 能力。示例表必须真实存在；`10` 和 `before_archive` 应替换为事先记录并仍受保护的 snapshot/tag。它们不是某次真实业务归档的结果。

```sql
SELECT snapshot_id, commit_kind, total_record_count, delta_record_count
FROM paimon.db.`orders$snapshots`
ORDER BY snapshot_id;

SELECT partition, bucket, file_path, record_count, file_size_in_bytes
FROM paimon.db.`orders$files`;

SELECT * FROM paimon.db.orders WHERE dt = '20240101';
SELECT * FROM paimon.db.orders VERSION AS OF 10 WHERE dt = '20240101';
SELECT * FROM paimon.db.orders VERSION AS OF 'before_archive' WHERE dt = '20240101';
```

时间旅行语法见 [官方 Spark 查询说明](https://paimon.apache.org/docs/1.3/spark/sql-query/#batch-time-travel)。`$files` 应检查 `file_path`；本分支没有可直接查询的 `external_path` 系统表列。

维护冻结有效、单分区非 NOOP 的纯引用替换应只新增一个 `COMPACT`，其 `delta_record_count=0`、`total_record_count` 不变；当前所选分区的文件路径指向冷根，未迁移文件仍在原位置。文件数、bucket 与元数据需结合实际计划核对，不能只观察 latest snapshot ID 增大。

对比迁移前固定的完整行多重集和迁移后结果，另验证分区/非分区谓词与历史读取；PK 表应检查实际逻辑行，不将 `total_record_count`、文件 `record_count` 等物理计数等同于去重后的 `COUNT(*)`。历史 snapshot 数字入口如已按策略过期，应按保留的 tag 查询，不承诺数字入口永久有效。

测试已在新 SparkSession 验证读取；缓存表、长生命周期 reader、streaming reader 的刷新语义尚未验收，不能靠一次新 session 查询推断所有线上读者均正常。跨进程/集群上线需独立验收。

## 6. 使用限制与不支持项

下面是当前入口的保守白名单；已验证的存储和表模式以第 1–2 节及任务证据为准。

当前支持三种普通Paimon分区表；这不是Spark原生Parquet/ORC表，也不是Iceberg/Hudi表：

| 表类型 | bucket配置 | PK merge engine |
|---|---|---|
| 无主键append，固定分桶 | 正整数，HASH_FIXED | 不适用 |
| 无主键append，不分桶 | -1，BUCKET_UNAWARE，物理bucket-0 | 不适用 |
| 有主键PK，固定分桶 | 正整数，HASH_FIXED | deduplicate、partial-update、aggregation |

必须通过Paimon SparkCatalog调用，且snapshot由文件系统协议管理。启用catalog外部版本管理（supportsVersionManagement=true，例如RESTCatalog）的表目前拒绝：现有历史枚举/发布确认采用文件系统snapshot，不能把目录里可见的snapshot当成REST权威发布。FileSystemCatalog的本地/MinIO场景已实测；其他同协议catalog仍需实际部署验收。执行者还需有权限枚举当前catalog内表及分支历史根，检查目录独占性。

first-row、PK动态分桶（HASH_DYNAMIC/KEY_DYNAMIC）及bucket=-2的POSTPONE_MODE均不支持。first-row涉及lookup和level-0读取语义，尚未完成独立归档验证，当前明确拒绝。partial-update/aggregation新增专项用例验证归档后同主键热端更新与旧tag读取，不能仅从默认deduplicate场景推断。

| 范围 | 要求 / 排除 |
|---|---|
| 表和分区 | 普通分区 FileStoreTable；完整分区，标准 partition/bucket 布局；不支持非分区表、特殊链式/format/system 表或自定义 `data-file.path-directory` |
| bucket | append 的 HASH_FIXED/BUCKET_UNAWARE，PK 的 HASH_FIXED；拒绝动态 bucket、KEY_DYNAMIC 和 POSTPONE_MODE |
| branch | 只允许 main、无额外 branch、无 `scan.fallback-branch` |
| DV / 独立索引 | 当前及受保护历史不得启用 DV，保留 snapshot/tag 不得有独立 indexManifest |
| 行语义 | 禁用row tracking/data evolution，所选文件无firstRowId/writeCols；PK只接受上表三种merge engine，first-row拒绝 |
| changelog | 当前及相关历史 `changelog-producer=none`，无保留的 snapshot/tag changelog 或独立 changelog |
| 文件索引 | 接受无 extraFiles 或一个经过头部识别的 `.index`；未知/多 sidecar 拒绝。提交前校验 reader 路径往返；本地和MinIO的bitmap embedded/sidecar查询已验证 |
| 外部副作用 | 排除 Iceberg 元数据镜像、自定义 commit/tag 回调、metastore 分区映射/tag-to-partition、tag success-file 等；详见 spec §4 |
| 消费者 | Phase 1 入口仅 Spark；Flink 集成不在本阶段。FILE_MONITOR/文件增删订阅不支持；常规增量流和缓存仍需专门验收 |
| 存储 | 冷端必须在线、可立即读取且支持所需随机访问；不支持需 restore 才可读取的离线/归档存储级别 |
| 并发和生命周期 | 必须遵守维护窗口；任意并发写、schema/branch/tag 修改、清理或 drop 不属于支持条件 |

历史门槛检查的是仍被保留 snapshot/tag/所选文件引用的 schema 和元数据。读取检查失败要拒绝，不能当作没有历史。临时关闭一个当前选项并不能消除旧 DV/changelog/镜像依赖；不要删除业务历史或绕过检查来满足条件。普通 BINARY/VARBINARY 列不等于其他分支的 managed BLOB 功能，不应一概禁止。

其他重要边界：

- **不是永久冷热策略。** `strategy=none` 下后续写入仍可能到热端，同一分区可混合存储；后续 compaction 也可能让数据重新变热。没有自动定时归档、expire-action 接入或自动回迁。
- **不是降低文件数的优化。** 不减少小文件数量或 LSM 层次；冷端延迟、GET/读回流量和可能的跨网络费用需测量，不能宣称查询一定提速。
- **需要临时双份空间。** 迁移前后源副本和冷副本同时存在；历史 tag、长保留策略、失败副本和未释放 pin 会延长占用。复制器读源一次、写冷端并完整读回；原探针还会另读源计算摘要。
- **路径兼容有边界。** Paimon 的转义分区目录可用于 data-only 归档；若 sidecar 的复制路径不能按本版本 reader 元数据解析回同一路径（例如带空格的冷根），操作在提交前失败。不要通过手工修改 manifest 绕过此检查。
- **资源上限是保守门槛。** 当前每份快照最多 100000 个 manifest ADD/DELETE 条目，单 CALL 累计校验最多 1000000 条历史条目，snapshot/tag/分区及所有权枚举设 10000 门槛，计划载荷最多 1 TiB；manifest list 不超过 16 MiB，单 manifest 不超过 64 MiB，索引头不超过 1 MiB。限额在相应读取/复制前检查，并缓存已校验的不可变历史。实际峰值与默认限额的生产适用性仍需测量。
- **冷对象缺失就会影响读取。** 本地热副本存在不代表最新快照可自动恢复。不要在生产重复第 2 节的破坏性故障注入。
- **测试不代表所有后端。** 尚未验证 S3 → 另一 bucket、反向迁移、OSS/HDFS、真实云 S3、跨进程崩溃或分布式 Spark；目标介质和 catalog 发布协议需要独立验收。
- **不是备份。** 源/冷对象、元数据、权限和登记根需要统一生命周期管理；DROP、对象生命周期规则或手工清理都可能破坏读取。不要给活跃冷前缀设置独立于 Paimon 引用的自动过期策略。

## 7. 失败、恢复与收尾

提交超时、网络中断或异常并不证明提交没有发生。不能把“SQL 报错”转换成删除冷对象、调用 abort 或盲目重跑。

### 7.1 UNKNOWN 或进程崩溃

1. 保持写入/清理冻结，保留全部相关冷对象与源 pin；停止后续分区。保存 operation、commit user/identifier、源快照、目标映射和已知错误，不记录密钥。
2. 由具备 catalog 访问能力的运维人员，从重新加载的权威元数据核查本次提交身份、`COMPACT` 快照及实际 after 文件引用；仅发现 latest ID 改变不算确认。
3. 若确认发布，核对完整查询、冷文件可读性和是否存在额外热文件；若确认未发布，也先确认该操作不再活动，再规划新的尝试。
4. 无法确认就保持 `UNKNOWN` 和保护，交由人工恢复；不要删除对象或所有 archive tag 来“解除阻塞”。当前尚无已实现的自动恢复命令。

SOURCE_PROTECTION_UNCONFIRMED表示创建/采集源保护时未能确认本次pin；没有发布，但应保持维护窗口并人工核实pin，不继续后续分区。复制期间会重检保留snapshot/tag及pin的完整快照/无TTL属性；发布后发现保护变化或无法验证时，保留committed_snapshot并返回INCOMPLETE。只有实际尝试了本次pin删除，后续确认其不存在才能视为清理成功。

### 7.2 INCOMPLETE、pin 释放失败与冷对象缺失

- `INCOMPLETE`：已发布的冷引用仍需保留；查明并停止漏停的 writer/维护任务，重新建立冻结后再为剩余文件规划操作，不能把整分区视为已冷却。
- `cleanup_required`：先确认发布/查询及操作身份，只处理本次操作自有 pin，不动用户 tag 或其他操作的 pin；未解决前记录额外空间保留风险。
- 冷对象缺失或损坏：停止进一步清理，保存文件映射和快照证据。只有明确源对象身份、字节一致性及引用关系后，才由经过验证的恢复流程处理；本文不提供面向业务对象的直接覆盖/删除命令。

### 7.3 正常收尾

逐分区检查结果和第 5 节读验证，确认无 UNKNOWN/维护违约，确认本次 pin 的处理结果，并保留操作记录。随后按既定保留策略恢复维护和写入；有未解决的保护/清理状态时应先人工确认恢复边界。

不要立即删除源数据。保留 snapshot/tag/消费者引用仍可能需要它们；普通清理和 orphan 清理何时可以运行取决于操作已结束、保护关系和实际保留窗口。跨介质 GC/drop 验收尚未完成，当前测试通过不能替代该门槛。未发布残留应在确认无活跃操作/引用后走经验证的协调清理，而不是任意递归删除冷根。

## 8. 常见问题与文档验收状态

| 现象 | 检查方向 |
|---|---|
| 找不到 `archive_partitions` procedure | 确认使用本分支构建产物；清理并重新构建 shaded 模块，检查实际加载类是否仍来自旧连接器 |
| 冷路径 scheme 无法解析、查询权限错误 | 检查实际 reader/清理端的 ResolvingFileIO、插件、endpoint、凭据和网络；catalog 加载后的 SET 未必生效 |
| MinIO 测试找不到 Docker | 检查 Docker 启动状态、真实 socket 与 API 兼容性，不关闭启动/清理检查 |
| 容器镜像拉取失败 | 使用可访问且受信任的测试镜像覆盖，记录实际镜像版本并重新执行测试 |
| 归档后源空间没有释放 | 通常需先检查保留历史/tag/pin 和清理进度；不能靠删除源文件解决 |
| 后续新文件又出现在热端 | `strategy=none` 与非永久冷热策略的预期边界，先区分归档后新写入和维护窗口违约 |

本文参数、配置键、返回语义和查询列对照当前实现。真实 CALL 的本地/MinIO执行证据及审查记录见任务清单；仍须按 [AP-17 / AP-18](tasks/todo.md) 补齐完整生命周期和部署放行，不把本地测试通过等同于所有生产环境已验收。
