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

# AP-CALL: Spark archive_partitions 实现与写入验证

本文件保留初次交付候选的测试及产物SHA记录。后续本地提交、代码审查修正与当前候选结果见[AP-REVIEW](AP-REVIEW.md)，下述旧产物SHA不代表后来重新构建的JAR。

日期：2026-10-11。基线 release-1.3.2 / c05f7d1f1b1e5d37e64edab0f2978124d90b64f7。
开发分支 codex/archive-partitions-132，worktree /Users/conradjam/.codex/worktrees/archive-partitions-132/paimon。

结论：**已实现并注册真实 Spark CALL；最终 core131、本地CALL20、MinIO CALL20及既有Scala11例通过，失败/错误/跳过均为0。** 数据文件采用原字节复制、长度和SHA-256读回验证；每个完整分区仅发布一个COMPACT引用替换，不运行数据compactor。独立审查发现均已修复。本报告不等于完整AP-01–18生产放行。

## 实现与版本依据

- JDK11、Maven3.9.16，spark3 profile；UT实际Spark3.5.6/Scala2.12。公共生产源码target仍Java8。
- [PartitionArchiver](../../paimon-core/src/main/java/org/apache/paimon/operation/PartitionArchiver.java)：完整分区类型化去重、固定快照计划、源pin、复制、重检、单COMPACT、fresh确认、逐分区状态和停止规则；实例一次性调用。
- [ArchiveSafety](../../paimon-core/src/main/java/org/apache/paimon/operation/ArchiveSafety.java)：表/branch/历史白名单、持久根登记、跨所有表及其branch历史的所有权、真实本地别名、预算检查。
- [ArchiveFileCopier](../../paimon-core/src/main/java/org/apache/paimon/operation/ArchiveFileCopier.java)：64KiB缓冲、非覆盖输出、源长度/字节数、关闭后长度和目标SHA-256读回。
- [Spark入口](../../paimon-spark/paimon-spark-common/src/main/java/org/apache/paimon/spark/procedure/ArchivePartitionsProcedure.java)，注册于SparkProcedures；三个必填STRING参数table/partitions/target_path，返回result STRING。
- 原URI helper继续复用；没有修改POM、共享FileIO/Path、FileStoreCommitImpl、compactor或cleaner，没有新增依赖。
- [Paimon1.3 Spark procedure](https://paimon.apache.org/docs/1.3/spark/procedures/)：沿用sys命名空间和现有BaseProcedure/required参数模式，完整分区的逗号/分号约定另加严格检查。
- [Paimon1.3文件系统](https://paimon.apache.org/docs/1.3/maintenance/filesystems/#s3-compliant-object-stores)、[时间旅行](https://paimon.apache.org/docs/1.3/spark/sql-query/#batch-time-travel)、[快照保留](https://paimon.apache.org/docs/1.3/maintenance/manage-snapshots/)：据此配置IO、MinIO及历史读取。新归档组合不是官方现有API，内部提交/元数据签名另以release-1.3.2源码和实际测试核对。

## 最终测试结果

| 层级 / 类 | 实际用例数 | 结果 |
|---|---:|---|
| ArchivePathTest | 38 | 0失败/错误/跳过 |
| ArchiveFileCopierTest | 7 | 0失败/错误/跳过 |
| PartitionArchiverTest | 15 | 0失败/错误/跳过 |
| FileStoreCommitTest | 39 | 0失败/错误/跳过 |
| FileStorePathFactoryTest / DataFilePathFactoryTest | 3 / 2 | 0失败/错误/跳过 |
| SnapshotManagerTest / TagManagerTest | 16 / 2 | 0失败/错误/跳过 |
| LocalOrphanFilesCleanTest | 9 | 0失败/错误/跳过 |
| core合计 | 131 | BUILD SUCCESS |
| ArchivePartitionsProcedureTest | 20 | BUILD SUCCESS |
| ArchivePartitionsS3ITCase | 20 | BUILD SUCCESS，真实MinIO |
| ExpirePartitionsProcedureTest（Scala） | 11 | All tests passed |

最终 core131来自同一次 /tmp/paimon-archive-core-release-candidate.log，包含全部缓存和一次性实例修正，不拼接不同候选的131例结果。此前core128、后续60focused为中间验证，不额外累加为新用例。

### 真实写入与查询链路

复用原隔离数据场景，但新类通过SQL CALL替代test-only引用提交。每后端9个模式/格式场景：append fixed2 buckets、append unaware、PK fixed2 buckets × Parquet/ORC/Avro；另11个输入/多分区/索引场景，总计每后端20例。

1. Spark真实CREATE/INSERT；append含重复行，PK包含迁移前upsert/delete。核对明确预期的完整行多重集。
2. CALL带重复完整分区只执行一次，返回ARCHIVED；重入返回NOOP，不新增快照。
3. append snapshot2→3，PK snapshot3→4，只新增一个COMPACT，delta_record_count=0；total_record_count不变，cold全部bucket配对DELETE/ADD，hot metadata不变。
4. 持久化新metadata逐字段核对，恢复原fileName/externalPath后等于before；源文件保留，字节长度/SHA一致，旧snapshot和tag不变。
5. 新session读latest、迁移前snapshot/tag；继续INSERT id6后，cold分区同时有热/冷文件，append最新7行。
6. PK继续在热端更新冷端id1为111、删除冷端id5；latest为(1,111,cold)、(2,20,cold)、(3,30,hot)、(6,60,cold)，历史snapshot/tag仍保持各自原值。
7. 精确移除测试自有一个冷对象，latest必须报该对象FileNotFoundException；旧tag仍可读。finally恢复并校验目标字节和最新结果。
8. 多分区缺失项FAILED，其后两项分别ARCHIVED并各加一个快照；复合INT分区01/1类型化去重。
9. 非法分区、未登记根、其他表共享根在发布/创建pin之前拒绝。
10. bitmap embedded/独立.index两种真实文件索引，归档后id=2谓词结果正确，索引类型断言防止未启用索引的假通过。

这些是local[2]上的真实Spark SQL，不是分布式集群。MinIO类继承相同20场景；cold对象在隔离bucket，元数据和热数据在临时本地目录，没有连接既有业务MinIO。

### 核心故障与审查反例

复制器固定seed覆盖0、B-1、B、B+1、2B+17字节，目标碰撞不覆盖、源长度错误不写目标、同长度损坏必须SHA失败。

PartitionArchiverTest15例覆盖：close失败无发布；复制期append检测并停止；branch出现导致重检失败保留pin；提交响应丢失后确认ARCHIVED；确认读取失败UNKNOWN保留目标/pin并停止；发布竞态append确认INCOMPLETE；他表branch历史根共享拒绝；FILE-MONITOR大小写拒绝；symlink越界拒绝；不可读protected tag拒绝；转义data-only分区成功；带空格冷根的sidecar不能reader往返时在发布前拒绝；CachingCatalog实际重载与陈旧构造表不误报；UNKNOWN实例复用不删除旧pin。

## 命令、报告与构建产物

所有命令在本worktree执行，JDK路径为/Library/Java/JavaVirtualMachines/temurin-11.jdk/Contents/Home，Maven为/Users/conradjam/software/apache-maven-3.9.16/bin/mvn。

最终core：
~~~shell
env JAVA_HOME=/Library/Java/JavaVirtualMachines/temurin-11.jdk/Contents/Home \
  /Users/conradjam/software/apache-maven-3.9.16/bin/mvn -B -ntp \
  -pl paimon-core \
  -Dtest=PartitionArchiverTest,ArchiveFileCopierTest,ArchivePathTest,FileStoreCommitTest,FileStorePathFactoryTest,DataFilePathFactoryTest,SnapshotManagerTest,TagManagerTest,LocalOrphanFilesCleanTest test
~~~

打包前只clean paimon-bundle、paimon-spark-common、paimon-spark3-common、paimon-spark-3.5的生成target，再执行：
~~~shell
env JAVA_HOME=/Library/Java/JavaVirtualMachines/temurin-11.jdk/Contents/Home \
  /Users/conradjam/software/apache-maven-3.9.16/bin/mvn -B -ntp \
  -Pspark3 -pl paimon-spark/paimon-spark-3.5 -am \
  -DskipTests spotless:apply install
~~~
21模块成功；skipTests只用于构建依赖/连接器，不是测试证据。

最终本地CALL：
~~~shell
env JAVA_HOME=/Library/Java/JavaVirtualMachines/temurin-11.jdk/Contents/Home \
  /Users/conradjam/software/apache-maven-3.9.16/bin/mvn -B -ntp \
  -Pspark3 -pl paimon-spark/paimon-spark-ut \
  -Dtest=ArchivePartitionsProcedureTest \
  -DwildcardSuites=org.apache.paimon.spark.procedure.ExpirePartitionsProcedureTest test
~~~

最终真实MinIO CALL：
~~~shell
env JAVA_HOME=/Library/Java/JavaVirtualMachines/temurin-11.jdk/Contents/Home \
  DOCKER_HOST=unix:///Users/conradjam/.docker/run/docker.sock \
  /Users/conradjam/software/apache-maven-3.9.16/bin/mvn -B -ntp \
  -Pspark3 -pl paimon-spark/paimon-spark-ut \
  -Dtest=ArchivePartitionsS3ITCase -Dapi.version=1.44 \
  -Dpaimon.test.minio.image=alpine/minio:RELEASE.2025-10-15T17-29-55Z \
  test-compile surefire:test@integration-tests
~~~

socket/API/image沿用本机已验证覆盖；保留Ryuk和容器启动检查。没有修改Docker配置或停止用户容器；结束时docker ps按org.testcontainers标签只读检查为空。

| 日志（/tmp/） | 实际结果 |
|---|---|
| paimon-archive-copier-red.log | 最小未实现复制器：6例，1断言失败/5未实现错误 |
| paimon-archive-call-local.log | 旧shaded注册表遮蔽导致CALL解析失败，保留失败证据 |
| paimon-archive-call-clean.log | 清理旧打包后真实CALL20+Scala11通过 |
| paimon-archive-copy-mutation.log | 反转SHA比较：7例中1失败/5错误；恢复后正常 |
| paimon-archive-reuse-red.log | UNKNOWN实例复用：1例预期断言失败 |
| paimon-archive-reuse-green.log | 修复一次性guard后新增core60通过 |
| paimon-archive-artifact-build.log | 最终连接器21模块BUILD SUCCESS，66秒 |
| paimon-archive-call-final-local.log | 最终CALL20+Scala11通过，44.235秒 |
| paimon-archive-call-final-minio.log | 最终CALL20通过，28.739秒 |
| paimon-archive-core-release-candidate.log | 最终core131通过 |

Surefire XML保存在paimon-core/target/surefire-reports及paimon-spark/paimon-spark-ut/target/surefire-reports；target/临时日志可能被重跑或clean覆盖，本报告保留摘要。

构建产物：paimon-spark/paimon-spark-3.5/target/paimon-spark-3.5-1.3.2.jar（约40MiB）。
SHA-256：088286885f7a9569f06a36232317a7bf08ab09a59027147b10a2180f3c8ae9ac。
这是开发分支自定义产物，不是官方发布二进制。

核对JAR内PartitionArchiver.class与当前core编译class的SHA相同：
9898d4f2ee158bd10911e7423c9081a6901fe1fcbcc963cffaba46dcd13562d7。
SparkProcedures.class与common编译class也相同：
13f371852b0cf3d2e85224fd7936b4cbc543b66e22b4d71fe16dc957f77324ee。
防止只测新源码却交付旧shade类；CALL测试还断言运行时注册表包含archive_partitions。

## 独立五轴审查

读者/元数据正确性、可读性、模块边界、路径/凭据安全和资源预算均审查。发现2 Critical及6 Required，后续另发现一次性实例Required，均修正并加入关键反例：

- 其他表branch历史根可能被DROP递归删除 → 所有权范围与原catalog drop范围一致。
- sidecar escaped路径会被本版本reader二次解析 → 发布前严格reader路径往返，不修改共享寻址代码。
- 合法转义分区误套root URI策略 → 现有物理Path URI单独规范化。
- schema时间戳与ID/authority不一致 → invalidate后catalog加载，固定表UUID，显式schema ID/语义字段比较。
- revalidation异常释放pin/继续 → 保留pin并停止。
- 竞态append已发布却UNKNOWN → 单独确认expected引用，再把额外live文件归类INCOMPLETE。
- FILE_MONITOR字符串大小写绕过 → 解析enum比较。
- 完整文件集合先分配再限额 → manifest list/entry预算先行、顺序枚举、累计历史预算和不可变snapshot缓存。
- 一实例二次调用覆盖ordinal/pin → synchronized一次性guard，UNKNOWN复用RED先复现再GREEN。

最终独立复审确认无剩余Critical/Required，并检查15个PartitionArchiverTest报告通过；最终Spark/MinIO运行结果由主执行者另行核实，不归给先完成的审查。

## 使用与剩余放行范围

[使用指南](../../archive-partitions-user-guide.md)和[官方文档位置](../../docs/content/spark/procedures.md)已更新。必须持久登记独占目标，所有reader/维护端能访问冷热FileIO；暂停写入/清理，UNKNOWN/INCOMPLETE不盲删副本或pin。

未取得完整验收：自动化维护协调、跨介质GC/drop故障矩阵、所有索引类型、stream/checkpoint/cache语义、生产资源测量、分布式Spark和其他存储方向/介质。已有LocalOrphan回归通过不等于新跨介质生命周期全矩阵通过。

初次交付时原checkout两份MD的SHA-256保持原值，源码在独立worktree，无业务表/业务MinIO变更，当时未commit/push/发布。用户随后授权本地提交，现有提交和审查修正以AP-REVIEW记录为准；仍不push/发布。

最终收尾：6份修改Markdown已解析，22个相对链接和8段guide/证据Shell语法通过；git diff --check通过（公开procedure文档原有行尾空白未作无关修改）。21模块构建经过Checkstyle/Spotless和Java/Scala编译；JDK11根目录apache-rat:check通过，4678资源、Unapproved=0、unknown=0，日志/tmp/paimon-archive-final-license.log。所有测试会话已结束，Testcontainers标签容器清单为空。实现、测试和文档保留在当前worktree供审查。
