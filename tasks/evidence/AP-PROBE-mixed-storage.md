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

# AP-PROBE: 本地与 MinIO 混合存储实测

日期：2026-10-11。结论：**受控 data-only 分区原字节复制 + COMPACT 引用替换可在本地/MinIO 混合存储上保持 snapshot 和 Spark 查询正确性；18 个参数化测试通过。** 这是 src/test 内直接组合已有 API 的方案兼容性验证，不是尚未实现的 archive_partitions/CALL 的验收，不改变 AP-01–18 的生产安全门槛。

## 代码与实际环境

- 基线 release-1.3.2，HEAD c05f7d1f1b1e5d37e64edab0f2978124d90b64f7；分支 codex/archive-partitions-132。
- Worktree：/Users/conradjam/.codex/worktrees/archive-partitions-132/paimon；原 checkout 未修改。
- Maven3.9.16 + Temurin JDK11.0.32.1，macOS arm64；命令局部 JAVA_HOME，不改全局配置。
- spark3 profile；Surefire 报告实际 classpath 是 **Spark3.5.6 / Scala2.12**，不是根 POM 中 test.spark.version=3.3.0 所指的其他测试矩阵。Spark local[2]，不是跨主机集群。
- Docker29.8.2；Testcontainers1.19.1，随机 bucket/凭据/映射端口，没有连接用户的既有 MinIO 服务。
- 实际 MinIO 镜像：alpine/minio:RELEASE.2025-10-15T17-29-55Z，arm64，image ID sha256:cf23643a6cf9ce159c57643ceb88279e431262282428c9e0bf3a7ef1a97e84b4。
- [公共测试场景](../../paimon-spark/paimon-spark-ut/src/test/java/org/apache/paimon/spark/PartitionRelocationTestBase.java)、[无 Docker 本地测试](../../paimon-spark/paimon-spark-ut/src/test/java/org/apache/paimon/spark/LocalPartitionRelocationTest.java)、[MinIO IT](../../paimon-spark/paimon-spark-ut/src/test/java/org/apache/paimon/spark/MixedStoragePartitionRelocationS3ITCase.java)。没有修改 POM、现有生产类或注册 Spark procedure。

两种测试都把 schema/snapshot/manifest 和未迁移的热分区保留在本地临时表根；冷目录分别为独立本地目录或 MinIO 私有前缀。配置 resolving-file-io.enabled=true，登记 data-file.external-paths 并保持 strategy=none；write-only=true 保证测试期间不自动压实/过期。S3 endpoint/凭据在 Spark catalog 首次初始化前配置，也传入重新打开表时使用的完整 CatalogContext。

## 实际测试矩阵

共同方法：preservesSnapshotsAndQueries(format, buckets, primaryKey)，每个类 9 个真实参数化 invocation，不以测试方法数代替执行数。

| 表模式 | bucket 配置 | 格式 | 本地 cold | MinIO cold |
|---|---|---|---|---|
| append fixed | 2，按 id mod 分桶 | Parquet / ORC / Avro | 3/3 | 3/3 |
| append unaware | -1 | Parquet / ORC / Avro | 3/3 | 3/3 |
| PK fixed | 2，主键 pt,id | Parquet / ORC / Avro | 3/3 | 3/3 |
| 合计 | | | 9通过 / 0失败 / 0错误 / 0跳过 | 9通过 / 0失败 / 0错误 / 0跳过 |

每个 invocation 都执行真实 Spark INSERT/SELECT，PK 还执行 UPDATE 语义的 INSERT 和 DELETE。先核对固定预期的完整行多重集，append 含重复行，不能仅比 COUNT。

## Snapshot 与文件证据

| 场景 | 源 snapshot → 替换 snapshot | 迁移 cold 文件数 | 迁移前后逻辑行数 | 后续写入后的逻辑行数 |
|---|---|---|---|---|
| append fixed，三格式、两后端 | 2 → 3 | 3 | 6 → 6 | 7 |
| append unaware，三格式、两后端 | 2 → 3 | 3 | 6 → 6 | 7 |
| PK fixed，三格式、两后端 | 3 → 4 | 4 | 4 → 4 | 4，包含更新和删除 |

这些数值来自 final-mixed-matrix.log 的18条成功探针记录，不是计划值。SQL 系统表查询同时验证新增快照的 (snapshot_id, commit_kind, delta_record_count) 为 append 的 (3, COMPACT, 0) 或 PK 的 (4, COMPACT, 0)。

每例断言：

1. 全部分区 bucket 在一次提交中替换，仅新增一个 COMPACT；totalRecordCount 不变，deltaRecordCount=0，commitUser/identifier 符合该探针身份。
2. DELTA 有 cold 文件数个 DELETE 和同样数量的 ADD，全部属于 cold 分区；hot 分区完整 manifest entry 不变。
3. 从新 snapshot 重新扫描、反序列化的每个 replacement，恢复原物理名字/externalPath 后与源 ManifestEntry 完全相等，包含分区、bucket、totalBuckets、stats、level、sequence、schema 和其他 metadata。源 snapshot 对象及其 live entries 不变。
4. data 文件采用新 basename，保持格式后缀，按标准 partition/bucket 布局复制；关闭输出流后目标长度、SHA-256 与源相等，源仍存在。extraFiles 必须为空，不能冒充 sidecar 支持。
5. 只调用 FileIO.copyFile、metadata 克隆与 FileStoreCommit 的 COMPACT 增量；没有解码重编码或调用 data compactor。探针的摘要会再次读取源文件，尚非 AP-03 生产复制器的一次源读取实现。

## 查询与反例

- 用全新 SparkSession 查询最新表，完整行多重集等于明确的预期；cold/hot 分区及 id=5 过滤查询均正确。
- 查询旧 snapshot 和 before_relocation tag；新增 id=6 后再次核对，确保历史查询没有错误地读到 latest。
- 继续往 cold 分区写入 id=6，新文件仍在本地；重新扫描同一分区确实同时存在 externalPath 为空/非空的文件，查询能合并它们。
- PK 迁移后再写热端 id=1,v=111，并删除冷端 id=5。最新结果为 (1,111,cold)、(2,20,cold)、(3,30,hot)、(6,60,cold)；迁移前/迁移后/新增 id=6 后的历史快照仍有各自原值，旧 tag 保持迁移前的4行。
- 每例精确删除一个本例拥有的冷对象。最新完整查询必须以该文件名的 FileNotFoundException 根因失败，旧 tag 仍可查询。finally 从保留源复制恢复该对象，重新核对 SHA-256 和完整最新结果。这证明 cold 引用真正参与读取，没有使用热副本静默回退。

## 命令与执行结果

以下均从上述 worktree 执行；没有 -DskipTests、skip 静态检查或 mock S3。

本地独立测试和已有 Spark procedure 回归：

```shell
env JAVA_HOME=/Library/Java/JavaVirtualMachines/temurin-11.jdk/Contents/Home \
  /Users/conradjam/software/apache-maven-3.9.16/bin/mvn -B -ntp \
  -Pspark3 -pl paimon-spark/paimon-spark-ut \
  -Dtest=LocalPartitionRelocationTest \
  -DwildcardSuites=org.apache.paimon.spark.procedure.ExpirePartitionsProcedureTest \
  test
```

最终 combined 验证（恢复 mutation 后，显式选择两个类）：

```shell
env JAVA_HOME=/Library/Java/JavaVirtualMachines/temurin-11.jdk/Contents/Home \
  DOCKER_HOST=unix:///Users/conradjam/.docker/run/docker.sock \
  /Users/conradjam/software/apache-maven-3.9.16/bin/mvn -B -ntp \
  -Pspark3 -pl paimon-spark/paimon-spark-ut \
  -Dtest=LocalPartitionRelocationTest,MixedStoragePartitionRelocationS3ITCase \
  -Dapi.version=1.44 \
  -Dpaimon.test.minio.image=alpine/minio:RELEASE.2025-10-15T17-29-55Z \
  test-compile surefire:test
```

正式 Java IT execution 入口（不用 Scala wildcard 来替代）：

```shell
env JAVA_HOME=/Library/Java/JavaVirtualMachines/temurin-11.jdk/Contents/Home \
  DOCKER_HOST=unix:///Users/conradjam/.docker/run/docker.sock \
  /Users/conradjam/software/apache-maven-3.9.16/bin/mvn -B -ntp \
  -Pspark3 -pl paimon-spark/paimon-spark-ut \
  -Dtest=MixedStoragePartitionRelocationS3ITCase \
  -Dapi.version=1.44 \
  -Dpaimon.test.minio.image=alpine/minio:RELEASE.2025-10-15T17-29-55Z \
  test-compile surefire:test@integration-tests
```

本机 socket/image 覆盖是环境适配，不是普遍必需项。实际运行还传入临时 -Dlog4j.configurationFile=/tmp/paimon-mixed-storage.Pq81qV/diagnostic-log4j2.properties，仅打开探针/容器 INFO 日志；省略它不改变断言。local-review 实际先执行 spotless:apply 再 test；最终独立IT使用已编译类直接 surefire:test@integration-tests。初次编译已经过 Checkstyle、Spotless 和 Java/Scala 编译检查。

| 日志，均在 /tmp/paimon-mixed-storage.Pq81qV/ | 实际结果 |
|---|---|
| local-red-runtime.log | 未实现 metadata replacement 的测试夹具，1例预期失败：期望 snapshot3，实际2；0错误/跳过 |
| local-green.log | 首个本地1例 + 既有 Scala11例通过 |
| minio-green.log | 真实 MinIO 首个1例通过，integration-tests execution |
| local-matrix.log | 本地9例 + Scala11例通过 |
| local-review.log | 加强历史/跨冷热主键断言后，本地9例 + Scala11例通过；exit0，49.762s |
| commit-mutation.log | ignoreEmptyCommit(true) 改为 false，9例全部在额外snapshot断言失败；0错误/跳过 |
| final-mixed-matrix.log | 恢复 true 后 local9 + MinIO9 = 18例通过；0失败/错误/跳过；exit0，31.096s |
| minio-integration-matrix.log | 完整 MinIO9例在 integration-tests execution 通过；0失败/错误/跳过；exit0，17.211s |
| license.log | 最终根目录 apache-rat:check 通过；Unapproved=0、unknown=0；exit0 |

Surefire XML：paimon-spark/paimon-spark-ut/target/surefire-reports/TEST-org.apache.paimon.spark.LocalPartitionRelocationTest.xml 和 TEST-org.apache.paimon.spark.MixedStoragePartitionRelocationS3ITCase.xml；Scala 报告在同目录。临时日志和 target 报告可能被清理或重跑覆盖，本文件保留实际结果摘要。

## 环境问题与审查

仓库默认 minio/minio:RELEASE.2022-02-07T08-17-33Z 拉取报 access denied；因此仅在本次命令通过显式系统属性使用已有 ARM 镜像。最初 Testcontainers 报找不到 Docker 环境；本机 /var/run/docker.sock 不存在，实际 socket 如命令所示。设置该 socket 和兼容 API1.44 后容器创建成功；MinIO 非 root 用户对 /data 无写权限，容器日志给出 file access denied。测试夹具改用容器自己的 /tmp/paimon-minio-data 后成功，未改 Docker 配置或宿主目录权限，也未关闭 Ryuk/容器检查。

local-red.log 的初始测试编译错误及 local-red-assertion.log 的格式检查失败已修正，不把这些前置失败或容器启动失败算作 TDD 行为 RED。仅 local-red-runtime.log 与 commit-mutation.log 是预期行为断言失败。

独立五轴审查提出两项 Required：历史查询与 latest 结果相同可能假通过；PK 未覆盖同一主键跨冷热存储冲突。已分别加入新写入后历史重读和热端 update/delete 冷端主键，最终矩阵实际执行并通过。mutation 临时把 ignoreEmptyCommit 设为 false，追加了多余空 APPEND，9例全部失败；恢复前后候选文件 SHA-256 均为 2e0784dae83af6a7c7435395c2205d76446148d772c5b6933970928a30ba3b23。

独立复审确认恢复后的候选无剩余高置信度 Critical/Required，并独立核对 SHA；其验证结论只引用当时已结束的本地9例、Scala11例和MinIO首例。最终18例/独立IT9例报告由主执行者另行核实，不把后到的运行结果归给评审者。修改的4份Markdown已解析、检查相对链接和行尾空白，均通过；git diff --check通过，原checkout两份MD的SHA保持原值。结束时没有遗留带org.testcontainers标签的测试容器；本次未commit/push。

## 来源和限制

- [Paimon1.3配置](https://paimon.apache.org/docs/1.3/maintenance/configurations/)：resolving IO默认关闭，外部根策略none保留普通写入路径。
- [Paimon1.3文件系统](https://paimon.apache.org/docs/1.3/maintenance/filesystems/#s3-compliant-object-stores)：MinIO endpoint与path-style配置。
- [Paimon1.3 Spark时间旅行](https://paimon.apache.org/docs/1.3/spark/sql-query/#batch-time-travel)：VERSION AS OF snapshot/tag。
- [Testcontainers配置](https://java.testcontainers.org/features/configuration/#customizing-docker-host-detection)：显式DOCKER_HOST与环境覆盖。
- 组合方式依据本checkout的 FileIO.copyFile、DataFileMeta.rename/newExternalPath、CompactIncrement、FileStoreCommit、FileStoreScan 和 DataFilePathFactory 源码；它不是官方已提供的 archive_partitions 功能。

未验证：完整入口参数/URI登记/历史门槛、sidecar/embedded index谓词、并发维护、提交UNKNOWN恢复、pin释放/GC/drop、streaming/cache刷新、S3→另一个bucket或反向迁移、跨进程崩溃、分布式Spark。test-only helper不允许直接接为生产入口。MinIO/Local临时数据随各自夹具结束清理；故障测试删除的冷对象在finally恢复，没有移动真实业务表。下一正式开发任务仍是AP-01完整安全入口。
