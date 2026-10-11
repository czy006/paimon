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

# Implementation Plan: archive_partitions Phase 1

> 状态：按用户最新要求交付真实Spark CALL；PartitionArchiver/ArchiveSafety/ArchiveFileCopier及入口已实现，核心和本地CALL验证通过，最新跨介质验证与审查见任务证据。AP-01–18完整放行合同未全部验收，原AP-PROBE18例保留为独立证据。
> 日期：2026-10-11。开发基线：正式发布tag `release-1.3.2` / `c05f7d1f1b1e5d37e64edab0f2978124d90b64f7`，POM `1.3.2`。
> 开发分支：`codex/archive-partitions-132`；独立worktree，原checkout保留不变。构建/测试固定使用JDK11（Temurin11.0.32.1），公共源码target仍为Java8；本阶段不运行Spark4的Java17profile。

## 1. 目标与文档职责

依据 [archive-partitions-phase1-spec.md](../archive-partitions-phase1-spec.md)，实现手工调用的 Spark `archive_partitions`：复制完整分区的不可变数据文件及受支持 sidecar，关闭流后校验长度与 SHA-256，以一次 COMPACT 快照原子替换该分区文件引用，不运行数据 compactor。当前快照转向冷端，历史引用仍读源副本；旧副本经既有保留与清理机制延迟回收。

spec 是语义与安全约束的权威来源；本文件记录实施顺序、决策、风险和验证入口；[todo.md](todo.md) 是唯一任务状态清单，不在本文件重复维护完成勾选。`AP-xx` 是实施任务，`T1–T19` 是 spec 的验收场景，两者不得混用。背景 MD 的 master-only 能力不作为实现依据。若源码/测试推翻假设，先报告并修订 spec，不静默降低门槛。

2026-10-11编写并更新[开发分支使用指南](../archive-partitions-user-guide.md)，补充真实CALL、构建与写入验证；入口已记录在USER_DOC，完整生产生命周期及部署验收保持未完成。

交付对象为核心逻辑、Spark procedure、core/Scala/MinIO 测试及用户文档；不包含 Flink、新表格式/CommitKind、自动调度、expire-action 接入、永久冷热策略、离线存储 restore 或任意并发写入保证。

本地提交后的实现审查新增ArchiveProtection，显式核对snapshot/tag/pin；限制为文件系统snapshot发布协议，PK merge白名单为deduplicate/partial-update/aggregation，first-row及catalog托管版本管理拒绝。具体复现、修正和最新验证见[evidence/AP-REVIEW.md](evidence/AP-REVIEW.md)。

用户追加partition_idle_time：默认1d的准入检查，按固定source snapshot的PartitionEntry.lastFileCreationTime汇总所有bucket；活跃分区NOOP/PARTITION_ACTIVE，不创建pin/副本/新snapshot。新增默认、指定时长、边界和后续写入测试；已有立即归档场景显式0s，活跃检查不替代维护冻结。

## 2. 架构决策和不能降低的约束

| 决策 | 依据与代价 |
|---|---|
| 复制而非源文件 rename | 历史读者依赖旧 URI；跨存储 rename 无通用原子性。代价是临时双份载荷 |
| 数据与 sidecar 均换 fresh basename | 避免既有 tag/orphan 按 basename 保护失败冷副本；保持普通 partition/bucket-N 深度 |
| 核心在 paimon-core，Spark 仅薄入口 | 复用 FileStoreScan/FileIO/提交机制；不把策略注入共享 FileIO/cleaner/compactor |
| COMPACT before/after 引用替换 | 原子性来自原有快照发布；不是文件压实。需排除 FILE_MONITOR 等消费文件变化的模式 |
| 长度 + 源摘要 + 目标完整读回 | 能检测同长度内容损坏；源读一次、冷端写一次读一次，不声称零 CPU/IO |
| 外部维护窗口 + 无 TTL source pin | 现有 OCC 不排斥独立 append，pin 也不能保护未提交冷对象免于 orphan 清理 |
| 以 fresh 权威 metadata 分类结果 | 区分未发布失败、已发布成功、UNKNOWN、已发布但维护违约 INCOMPLETE；提交尝试后不 abort/delete after |
| 最小白名单与无外部副作用 | 支持 append fixed/unaware 和 PK fixed；拒绝 DV/独立index/changelog/row-tracking/evolution/多branch及 Iceberg/custom callback/metastore映射等 spec 排除项 |

每分区一次提交，不按 bucket 拆成多个快照。保持 oldMeta 全字段，after 仅改变 fileName、extraFiles 物理名字和 externalPath。完全 NOOP 不写文件/快照/pin。未知结果或维护违约停止后续分区并保留保护。CALL 不自动 ALTER 登记根，不覆盖文件，不建立未被所有 writer/cleaner 遵守的“原生锁”。

初始计划按CP-C/AP-09/AP-10后注册Spark。用户最新要求提前实现并验证真实CALL，当前已加入保守门槛、索引和故障保护，并直接测试入口；未测GC/drop、流消费及生产资源适用性不宣称通过，不降低最终CP-F/CP-G放行合同。

## 3. 阶段与依赖

2026-10-11按用户要求提前执行AP-PROBE：在test-only代码中组合现有FileIO.copyFile、DataFileMeta与COMPACT提交，验证普通本地存储/MinIO混合存储、snapshot及Spark查询。它可独立于生产实现运行，不提供归档公共入口，也不改变下面的正式依赖。实际18例通过；复现见[evidence/AP-PROBE-mixed-storage.md](evidence/AP-PROBE-mixed-storage.md)。覆盖data-only、多bucket、三模式/三格式、历史查询、跨层PK变更和缺失冷对象，不替代AP-03–15的实现与失败安全验收。

```text
AP-00 → AP-01 ┬→ AP-02 ─┐
             └→ AP-03 ─┴→ AP-04 → AP-05             [CP-B: 本地引用替换]
                                     ↓
                                AP-06 → AP-07 → AP-08 [CP-C: 故障与门槛]
                                                      ↓
                                                AP-09 → AP-10 → AP-11 [CP-D: 索引/GC/CALL]
                                                                       ↓
                                                               AP-12 → AP-13 → AP-14 [CP-E: 跨存储]
                                                                                    ↓
                                                                            AP-15 → AP-16 → AP-17 [CP-F]
                                                                                                   ↓
                                                                                                AP-18 [CP-G]
```

CP-A 在 AP-00–02 后核查环境/计划模型；AP-03 可在 AP-02 的规划阶段并行开发，但 AP-04 必须等待两者完成。图表达默认推进顺序；精确前置依赖以 todo 中每项为准。除 AP-02/AP-03 的独立写范围外，不同时修改同一个 PartitionArchiver/测试文件；列出并行机会不等于已启动代理或外部任务。

| 阶段 | 任务 | 可验收产物 |
|---|---|---|
| A：环境与不可变计划 | AP-00–02 | 基线/工具链证据、拒绝门槛、固定快照文件映射；无数据副作用 |
| B：最小本地闭环 | AP-03–05 | 受控本地 data-only 分区，copy/hash → pin → 单 COMPACT → 读取等价 → NOOP；尚不公开 Spark |
| C：安全闭环 | AP-06–08 | 提交不确定恢复、维护违约停止、所有受保护历史/回调门槛 |
| D：完整本地与 Spark | AP-09–11 | embedded/sidecar 实际过滤、生命周期清理、完整 CALL 合约 |
| E：消费者与跨存储 | AP-12–14 | 缓存/流语义、本地→MinIO/external→external、远端故障恢复 |
| F：部署收尾 | AP-15–17 | 冷热 GC/drop、资源/安全预算、操作与恢复文档 |
| G：放行审查 | AP-18 | 五轴评审、T1–T19 证据全覆盖、无遗留 Critical/Required |

各任务原则上 S/M、最多约 5 个文件且验收不超过 3 项。若预计无法在一次聚焦会话完成，先细分，不以删掉失败测试缩小任务。checkpoint 审阅确认之前不将对应能力宣称可用；CP-B 的局部成功不是生产批准。

## 4. 拟修改文件与边界

todo 使用下列路径缩写；均相对仓库根，文件名为后续拟新增而非已存在。ArchivePlan/ArchiveResult 优先为 PartitionArchiver 内部不可变类型，不先制造公共框架。

| 缩写 | 路径 |
|---|---|
| CORE | `paimon-core/src/main/java/org/apache/paimon/operation/` |
| CORE_TEST | `paimon-core/src/test/java/org/apache/paimon/operation/` |
| SPARK_PROC | `paimon-spark/paimon-spark-common/src/main/java/org/apache/paimon/spark/procedure/` |
| SPARK_REG | `paimon-spark/paimon-spark-common/src/main/java/org/apache/paimon/spark/SparkProcedures.java` |
| SPARK_TEST | `paimon-spark/paimon-spark-ut/src/test/scala/org/apache/paimon/spark/procedure/` |
| S3_TEST | `paimon-spark/paimon-spark-ut/src/test/java/org/apache/paimon/spark/ArchivePartitionsS3ITCase.java` |
| S3_CORE_TEST | `paimon-spark/paimon-spark-ut/src/test/java/org/apache/paimon/operation/PartitionArchiverS3ITCase.java` |
| USER_DOC | `docs/content/spark/procedures.md` |

预计新增 CORE/PartitionArchiver.java、CORE/ArchiveFileCopier.java、CORE_TEST/PartitionArchiverTest.java、CORE_TEST/ArchiveFileCopierTest.java、SPARK_PROC/ArchivePartitionsProcedure.java、SPARK_TEST/ArchivePartitionsProcedureTest.scala、S3_CORE_TEST、S3_TEST；修改 SPARK_REG 和 USER_DOC。S3_CORE_TEST不启动Spark，放在已有S3/test-jar依赖的spark-ut测试模块，避免给paimon-core添加S3依赖；与core的operation包同名便于测试包内边界，不因此新增生产公共API。测试 helper 优先放测试内；超过文件预算须重新切片。复用 `ExpirePartitionsProcedure`/Scala suite 和 `SparkS3ITCase`/MinioTestContainer 模式，不修改它们来削弱既有断言。

AP-01首个增量将URI规则放在包内ArchivePath.java及ArchivePathTest.java。真实CALL随后由ArchiveSafety/PartitionArchiver实现完整入口检查；Spark场景实际采用UT/src/test/java/ArchivePartitionsProcedureTest.java与ArchivePartitionsS3ITCase.java，复用既有写入/历史断言。U02仍由ArchivePathTest承载，core验证包含该测试。实际源码/验证范围见AP-CALL证据，不将URI子集等同于完整生产放行。

不预设修改 FileStoreCommitImpl、FileIO、DataFileMeta 格式、cleaner、compactor、Flink 或 POM；若测试证明必须变动，先解释原因/新增影响并更新 spec 与计划。不把“只新增类”当作正确性免检条件。

## 5. 验证命令与证据规则

以下是未来在仓库根运行的入口，**本轮未运行**。AP-00 先记录实际 Maven/JDK/profile；core/Spark3 公共代码维持 Java 8 兼容，Spark4 使用本分支 Java17/Scala2.13 profile。不要根据 IDE SDK 推断 Maven runtime，不更改全局 Java/PATH/配置。

```shell
# V-ENV：环境、checkout 与部署前提
git status --short --branch
git rev-parse HEAD
mvn -v
java -version
docker info

# V-BUILD：安装当前 reactor 依赖并编译；不是测试通过证据
mvn -Pspark3 -pl paimon-core,paimon-spark/paimon-spark-common,paimon-spark/paimon-spark-ut -am -DskipTests install

# V-CORE：核心行为测试；类存在后才运行
mvn -pl paimon-core -Dtest=ArchivePathTest,PartitionArchiverTest,ArchiveFileCopierTest test

# V-SPARK：Scala CALL suite
mvn -Pspark3 -pl paimon-spark/paimon-spark-ut -Dtest=none -DfailIfNoTests=false -DwildcardSuites=org.apache.paimon.spark.procedure.ArchivePartitionsProcedureTest test

# V-S3：两层Java JUnit IT，Docker/MinIO 前提成立后运行
mvn -Pspark3 -pl paimon-spark/paimon-spark-ut -Dtest=PartitionArchiverS3ITCase,ArchivePartitionsS3ITCase -DfailIfNoTests=false integration-test

# V-STYLE：相关模块已有风格门槛
mvn -Pspark3 -pl paimon-core,paimon-spark/paimon-spark-common,paimon-spark/paimon-spark-ut spotless:check checkstyle:check
```

早期V-CORE的 `-Dtest` 只指定已实现的类：当前只有ArchivePathTest；PartitionArchiverTest/ArchiveFileCopierTest加入后再扩大全套。不能允许缺失用例被零测试成功掩盖。Core focused run 不使用 failIfNoTests=false。Scala/Java IT 入口中允许 runner 差异，不代表允许目标 suite 零用例：必须读取实际 ScalaTest/Surefire 报告，记录目标类、用例数、失败/跳过数、退出码和耗时。Java IT 不用 wildcardSuites；同时确认 integration-test execution 实际执行了目标 ITCase。

默认 spark3 测试入口并非覆盖所有 Spark 版本；AP-18 从当前 profiles 决定声明支持矩阵并补跑对应构建/suite。不得把 Java8/Spark3 单跑称为 Spark4 验证，也不预写未经 AP-00 核实的跨版本命令。

每次改源后重装受影响 reactor 依赖再跑下游 Spark/S3，避免读取旧本地 jar。任务完成必须有其 focused tests、相关模块编译和适用风格检查；最后还跑相关既有提交/扫描/清理回归。失败或环境阻塞如实记录，不删除断言、不跳过测试、不关静态检查、不发明 fast-build profile。

每个任务在 todo 的证据栏填写：HEAD/局部变更、命令与 JDK、报告路径/用例数、实际结果、残余风险、下一任务。持久证据可在后续实施时存入 `tasks/evidence/`（日志安全摘要，不放密钥/真实业务数据）；本轮不生成空的通过报告。

### 5.1 单元测试、本地文件系统与MinIO模拟测试执行计划

完整用例合同见spec §14.2–14.5：U01–U04纯逻辑，L01–L12真实本地文件系统，M01–M10容器化MinIO模拟S3。U/L由core的两个测试类承载；涉及文件/表交互的L用例是组件测试。MinIO不是内存mock，不与无Docker单元测试混为一类；M-core直接调用核心，M-Spark只补CALL/运行端语义。

| 执行顺序 | 环境/夹具 | 先验证的反例 | 对应任务 |
|---|---|---|---|
| 1. U纯逻辑 | JUnit5/AssertJ；不可变metadata/URI/状态样例 | 错误路径前缀、skipped进入增量、UNKNOWN误分类、预算边界 | AP-01/02/06/08 |
| 2. L复制器 | @TempDir内独立hot/cold目录；真实LocalFileIO；固定seed与buffer边界payload | 等长损坏、close失败、dst已存在、源缺失、资源泄漏 | AP-03 |
| 3. L完整表 | 三种真实表模式；完整分区、多bucket、旧snapshot/tag与真实index | before失效、独立append、索引缺失、历史选项已关仍有引用、orphan误删 | AP-04–10/16 |
| 4. M-core | 现有MinioTestContainer、动态endpoint/临时凭据，file表根/独占S3冷根及两个bucket | 错误部署、对象损坏/close错误、发布后响应未知、孤儿/邻表误删 | AP-13–16 |
| 5. M-Spark | Spark local[2] + 全新catalog配置/session、MinIO对象和checkpoint | 新session不能读、部分成功缓存不刷新、COMPACT制造业务重复、UNKNOWN仍跑后续 | AP-13–15；本地CALL/stream先由AP-11/12验证 |

每个非平凡行为按“具体失败测试RED → 最小实现GREEN → 保持测试通过的重构”推进；编译失败/环境失败与预期业务断言失败分别记录，不能把容器无法启动当RED已复现正确性问题。优先L06/L07/L08/L05和M05/M07等高风险失败场景，所有故障用具体路径/阶段/调用次数定点控制，按原有IO或经评审的最小包内边界注入，不向SQL增加test flag。

未来focused入口（测试类实现后、reactor依赖更新后运行）：

```shell
# V-ROOT：当前已实现的纯URI政策测试，无文件系统/Docker
mvn -pl paimon-core -Dtest=ArchivePathTest test

# V-LOCAL-COPY：无需Docker/网络，字节复制与故障单测
mvn -pl paimon-core -Dtest=ArchiveFileCopierTest test

# V-LOCAL-TABLE：无需Docker/网络，计划/真实表/历史/GC组件测试
mvn -pl paimon-core -Dtest=PartitionArchiverTest test

# V-S3-CORE：真实MinIO对象IO，目标类不启动Spark
mvn -Pspark3 -pl paimon-spark/paimon-spark-ut -Dtest=PartitionArchiverS3ITCase -DfailIfNoTests=false integration-test

# V-S3-SPARK：MinIO上CALL/new session/executor/stream语义
mvn -Pspark3 -pl paimon-spark/paimon-spark-ut -Dtest=ArchivePartitionsS3ITCase -DfailIfNoTests=false integration-test
```

V-CORE合并V-ROOT与两套本地测试；V-S3合并两套MinIO IT。分别审查Surefire报告中目标类实际执行的参数/方法与非零用例数，不用测试表格行数或Maven退出码充当覆盖证明。显式V-S3-CORE/S3-SPARK仍经过所在Maven模块生命周期，故可能额外执行其ScalaTest插件；“不启动Spark”指目标M-core类不创建SparkSession，不承诺整条Maven命令零Spark初始化，需记录实际runner范围。

本地临时目录与MinIO每用例UUID prefix隔离，不用用户既有MinIO。复用仓库固定镜像版本及容器health wait，不绑定固定9000端口或硬编码凭据；所有S3/cross-URI选项在catalog初始化前生效。MinIO不可用时U/L照常验证，M层记录未执行/环境阻塞，不能skip/mock降级并宣称已通过。测试资源清理由各层finally/afterEach/afterAll完成，仅删除本次拥有的根；UNKNOWN阶段保留副本的断言结束并确认现场后才能进行测试收尾。

## 6. 风险、前置条件与停止规则

| 风险 | 处理与首次验证任务 |
|---|---|
| 文件身份冲突与独立 append | AP-07：区分内核快照竞争重试与维护违约；后者停止 CALL，不自动重规划 |
| 发布已成功但响应丢失 | AP-06/AP-14：fresh 权威身份+after 引用确认；不能确认则 UNKNOWN/保留pin，不立即删 cold |
| 创建/删除 pin 的未知结果 | AP-04/AP-06：提前确定 own name，按名复查；清理异常不覆盖已确认发布状态 |
| 历史 DV/changelog/schema 或外部回调 | AP-08：扫描受保护引用，读不全 fail closed；不临时关闭功能绕过门槛 |
| sidecar 内容绑定物理 basename | AP-09 先用真实过滤验证；失败则阻断对应支持，先修订 spec |
| Cleaner/drop 误删或孤儿不回收 | AP-10/AP-15：独占登记根+fresh名字+标准目录；仅在隔离测试表中操作 |
| FileIO/catalog 凭据丢失及权限越界 | AP-13/AP-16：复用真实 CatalogContext，新 session/executor/清理端一并验证 |
| 计划/历史扫描耗尽内存 | AP-16：有界复制和计划预算，超限在复制前失败，不按 bucket 拆提交 |
| 后续写入/compaction 重新变热 | AP-16/AP-17：明确一次性迁移，不加入长期策略 |

部署责任由操作者承担并在调用前确认：冻结目标分区，等待并暂停破坏性维护/清理/branch/tag/schema变化，单表单CALL，预先登记本表独占冷根，所有读取/清理运行端有统一冷热访问能力，冷层在线可 seek。procedure 的复查不证明外部冻结真实成立。凭据不存入仓库，目标 URI 不含 userInfo/query 密钥。

需要外部选择的事项不阻塞文档创建，但阻塞相应实测/生产放行：生产冷根和所有权、部署 catalog/插件/凭据、maintenance责任人和窗口、活跃流模式、资源预算。先用隔离 FileSystemCatalog/local + MinIO 验证；不能据此声称 HDFS/OSS/REST catalog 已实测。测试资源使用唯一表根，结束时仅清理本次拥有的资源，不删除整个 bucket/warehouse。

## 7. 完成标准与下一步

满足 spec §14 的 T1–T19及U/L/M细化场景有效运行证据、八项不变量、完整支持组合、错误状态与生命周期测试；通过相关编译/风格/回归；Critical/Required 修复；用户文档可重复操作并注明范围，才可把 AP-18 标记完成。路径登记和读取前提不满足时应拒绝归档，不能切换为低保证实现。

此前文档阶段已完成。用户现已明确授权在1.3.2基线的独立worktree使用JDK11开始编码，按AP-00起推进；只对实际满足验收且有证据的项勾选。该授权不包含创建PR、推送、运行真实表迁移或定时自动化。
