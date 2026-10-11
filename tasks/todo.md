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

# Tasks: archive_partitions Phase 1

> 状态：用户最新要求的真实Spark archive_partitions已实现；保守门槛、复制校验、pin及提交确认已加入，实际核心/本地/MinIO验证与审查见AP-CALL交付。AP-01–18全量生产验收未全部完成，未取得证据的原检查项不盲目勾选。2026-10-11。
> 基线：release-1.3.2 / c05f7d1，开发分支codex/archive-partitions-132，JDK11；原checkout不变。本文件为唯一实施状态清单，未取得有效验证证据的任务不勾选。
> 文件缩写、验证入口 V-* 与证据要求见 plan §4/§5。AP 是任务编号，T 是 spec 验收编号。各任务完成后填写实际证据，不能只填“通过”。
> 测试合同：spec §14.2–14.5的U01–U04、L01–L12、M01–M10；本地core无需Docker，MinIO分core与Spark IT。实施/测试状态以各任务证据为准。

## AP-00：确认 checkout、工具链与隔离验证环境

**任务：** 建立可复现的构建与测试基线，不调整全局工具链或真实表配置。

**验收：**

- [x] 记录分支/HEAD、dirty文件、实际 Maven/JDK与profile；保留既有两份MD和用户变更。
- [x] 验证 V-BUILD 和既有 focused core/Spark样例；记录真实suite/用例数，区分基线失败与后续回归。
- [x] 确认@TempDir/真实LocalFileIO的无Docker测试入口，及MinioTestContainer/动态endpoint/隔离bucket-prefix前提；无服务时只标M层未执行，不伪报已测。

**验证：** V-ENV、V-BUILD、现有 procedure/提交测试；命令与实际 runner 记录到证据，不先创建未实现的目标类。
**依赖：** 无。**文件：** 后续 `tasks/evidence/AP-00.md`，必要时修正 plan 的环境记录；不改POM。**范围：** S，1–2文档。
**证据：** [AP-00](evidence/AP-00.md)：20模块构建、core42用例、Spark既有suite11用例通过；Docker可用，MinIO未执行。

## AP-01：定义归档安全入口与基础拒绝策略

> 当前插入的验证增量AP-PROBE见下；不因探针通过而降低或跳过本任务门槛。

**任务：** 在核心实现边界约束表模式、完整分区键、注册独占目标和实际多路径IO；暂不暴露Spark入口。

**验收：**

- [ ] 参数/当前表模式/branch/layout/副作用选项不符合spec时，在创建pin/目标文件前拒绝；普通BINARY不误拒。
- [ ] URI规范化按scheme/authority/路径段判断，拒绝祖先/共享/未登记/越界根和带凭据URI；不默默ALTER。
- [ ] 拒绝/结果类型明确区分表级异常与分区状态，不加入公共通用框架或占位成功返回。

**验证：** V-ROOT（URI子增量）、后续V-LOCAL-TABLE + V-BUILD/V-STYLE；U02/U04及T7/T11/T12/T19的当前配置反例。
**依赖：** AP-00。**文件：** CORE/ArchivePath.java、CORE_TEST/ArchivePathTest.java，以及后续CORE/PartitionArchiver.java、CORE_TEST/PartitionArchiverTest.java。**范围：** M，4文件。
**证据：** [AP-01 URI](evidence/AP-01-uri.md)记录初始URI增量。随后AP-CALL已实现表/branch/历史/登记/IO门槛、全catalog各表/branch历史根重叠检查及本地别名校验，拒绝反例已运行；完整矩阵与资源/部署验收未全部闭合，具体范围见AP-CALL证据。

## AP-PROBE：提前验证本地/MinIO混合存储、snapshot和查询

**任务：** 按用户2026-10-11追加要求，建立受控data-only test harness，直接验证方案所依赖的现有存储/元数据/读取能力；不提供生产归档API。

- [x] 本地独立热/冷目录无需Docker，三种模式×三种格式9例通过。
- [x] 真实MinIO容器与本地热文件混读，同样9例通过；实际integration-tests入口通过，非mock/skip。
- [x] 仅一个COMPACT、完整metadata等价、配对delta、旧snapshot/tag、新session、后续同分区混读、跨冷热PK更新/删除及缺失冷对象反例通过。
- [x] 独立审查提出的历史查询差异与跨层PK冲突用例已补齐；关闭ignoreEmptyCommit触发9个预期断言失败，恢复后18例通过。

**文件：** Spark UT的PartitionRelocationTestBase.java、LocalPartitionRelocationTest.java、MixedStoragePartitionRelocationS3ITCase.java；spec/plan和本证据。**证据：** [AP-PROBE](evidence/AP-PROBE-mixed-storage.md)。

**边界：** 仅验证本地测试表与私有MinIO容器，没有完整archiver/CALL、sidecar、恢复、并发、streaming、GC或分布式Spark验证；不能以此勾选AP-04/AP-13等正式任务。下一生产任务仍为AP-01完整入口门槛。

## AP-CALL：交付真实Spark入口及本地写入验证

**任务：** 按用户最新source-driven-development要求实现Spark CALL，沿用既有1.3.2 API与JDK11，对真实SQL写入→归档→查询→继续写入进行验收。

- [x] 新增core复制校验、安全/历史门槛和单分区COMPACT引用替换；登记Spark入口，返回逐分区结果，不改POM/共享FileIO/cleaner。
- [x] 本地CALL20个场景及既有Scala procedure11例通过；写入前后/主键更新删除/历史查询/索引过滤有真实SQL断言。
- [x] 核心128例含既有回归通过；后续catalog缓存及一次性实例修正后的新增core60例通过。SHA-256 mutation与UNKNOWN pin实例复用反例均先失败，再修复。
- [x] 最终连接器21模块构建、本地CALL20及MinIO CALL20、Scala11和最终core131全部通过；独立审查结案，报告与产物SHA保存到AP-CALL证据。格式/链接/许可证收尾记录见证据末尾。

**证据：** [AP-CALL实现与验证](evidence/AP-CALL.md)。本条记录用户最新交付范围，不将尚未验证的AP-12/AP-15/AP-16/最终生产放行勾选为完成。

## AP-REVIEW：本地提交、实现复核与活跃检查

- [x] 现有实现分为URI/复制基础、core编排、Spark入口三个本地提交保存，不push。
- [x] 对pin/历史保护、发布前冲突、skipped变化、catalog发布协议和PK merge白名单补充反例并修正；保护AND→OR mutation触发失败后恢复。
- [x] 按用户新增需求加入partition_idle_time可选参数，默认1d，复用compact的manifest活跃口径；固定Clock覆盖阈值及三表模式，默认活跃分区不创建对象/pin/快照。
- [x] 最终core154、本地CALL28、MinIO CALL28及Scala11通过，独立复审无剩余Critical/Required；使用/表类型文档更新，修正本地提交见AP-REVIEW/git log。

**证据：** [AP-REVIEW](evidence/AP-REVIEW.md)。生产GC/drop、stream/cache及资源测量继续保留未完成状态。

## AP-02：构建固定快照的不可变文件归档计划

**任务：** 使用同一snapshot的ALL/keepStats扫描生成old/after映射，不通过目录listing推断live。

**验收：**

- [ ] 覆盖 append fixed/unaware、PK fixed和多bucket；保留totalBuckets及原metadata，after仅允许三类物理字段变化。
- [ ] 每次尝试使用fresh data/extra名字，保持真实格式与压缩后缀、标准bucket深度；skipped不进增量。
- [ ] 不存在分区、重复类型化spec、空计划按spec分类；计划/历史枚举预算有明确拒绝路径，不复制或提交。

**验证：** V-LOCAL-TABLE + V-BUILD/V-STYLE；U01/U02/U04及T1/T2/T7/T18的计划断言，固定snapshot枚举。
**依赖：** AP-01。**文件：** CORE/PartitionArchiver.java、CORE_TEST/PartitionArchiverTest.java。**范围：** S，2文件。
**证据：** 未执行。

## CP-A：基础合同检查（AP-00–02）

- [ ] 基线、门槛与计划focused tests实际执行；编译/风格通过，无数据副作用。
- [ ] 复核目录/名字/metadata转换符合spec；用户或指定评审者确认后继续，不把模型测试当完整归档。

## AP-03：实现有界原字节复制与读回校验

**任务：** 实现包内ArchiveFileCopier，限定其职责为传输，不决定发布或GC。可与AP-02独立开发，但AP-04等待两者完成。

**验收：**

- [ ] 固定seed生成0字节及B-1/B/B+1/2B+17 payload，验证固定缓冲/close后读回；目标不覆盖且既有对象不复用；0字节仅是复制器payload，不是Paimon文件。
- [ ] 复制中断、close失败、同长度损坏、访问失败均阻止VERIFIED；记录仅本次拥有的残留，绝不删源。
- [ ] 已存在外部源按真实URI和table FileIO读取；不依赖同scheme rename或S3 ETag作为内容证明。

**验证：** V-LOCAL-COPY + V-BUILD/V-STYLE；L02/L06及T2/T6，按URI/第N次read-write-close-status定点故障、真实翻转byte；资源关闭断言，不依赖随机FailingFileIO。
**依赖：** AP-01；与AP-02独立开发时先确认FileCopy输入合同。**文件：** CORE/ArchiveFileCopier.java、CORE_TEST/ArchiveFileCopierTest.java。**范围：** S，2文件。
**证据：** 未执行。

## AP-04：验证本地完整分区单次引用发布

**任务：** 对受控、data-only分区打通pin→copy→验证→纯COMPACT→fresh确认的最小核心闭环；仍不公开Spark。

**验收：**

- [ ] own无TTL pin确认后才复制；copy每阶段暂停时旧snapshot/tag和热文件持续可读，after就绪前无发布。
- [ ] 全bucket仅一次COMPACT，无空APPEND；保持行多重集、PK更新/删除语义和metadata，源保持原位。
- [ ] commitUser含operation UUID、分区identifier单调且内部重试稳定；close资源，不经TableCommitImpl触发额外维护。

**验证：** V-LOCAL-TABLE + V-BUILD/V-STYLE；L01/L03及T1–T4；三模式/真实writer夹具，snapshot delta、行多重集与有界latch暂停读取，不只COUNT。异常安全尚待AP-06，禁止宣称可用。
**依赖：** AP-02、AP-03。**文件：** CORE/PartitionArchiver.java、CORE_TEST/PartitionArchiverTest.java。**范围：** S，2文件。
**证据：** 未执行。

## AP-05：完成NOOP与部分文件归档的重入语义

**任务：** 以已发布metadata判断重入，不把未提交残留视为可复用副本。

**验收：**

- [ ] 完全NOOP无新文件/快照/pin；部分归档仅替换changed pairs；skipped引用缺失报错，不猜热端回退。
- [ ] 不同根/相邻前缀/authority的containment正确；数据文件数、extra数、字节与bucket数区分。
- [ ] 新调用不用旧未提交cold文件，已发布重入NOOP；无metadata digest时不宣称既有文件内容已重新证明。

**验证：** V-LOCAL-TABLE + V-BUILD/V-STYLE；L04、U03及T7/T10/T16；核查对象/快照/pin无增量与skipped缺失失败。
**依赖：** AP-04。**文件：** CORE/PartitionArchiver.java、CORE_TEST/PartitionArchiverTest.java。**范围：** S，2文件。
**证据：** 未执行。

## CP-B：最小本地闭环检查（AP-03–05）

- [ ] V-LOCAL-COPY/V-LOCAL-TABLE不依赖Docker；L01–L04及适用L06实际执行，复制/单COMPACT/历史读取/NOOP通过；sidecar暂不纳入能力声明。
- [ ] 编译/风格及证据完整；评审确认进入安全闭环，不注册Spark或迁移真实表。

## AP-06：实现发布未知结果与pin恢复分类

**任务：** 完成ARCHIVED/FAILED/UNKNOWN及pin生命周期的故障语义。

**验收：**

- [ ] 响应丢失或close/确认异常用fresh权威commit身份+after引用复查；元数据不可确认维持UNKNOWN，不因latest变化判成功。
- [ ] 提交尝试后不abort/delete after；UNKNOWN保留pin、停止后续并输出NOT_ATTEMPTED；重启后按metadata确认而非复用随机对象。
- [ ] pin创建/删除已生效但响应失败可按own name复查；释放失败仅附cleanup_required，不覆盖已确认发布状态。

**验证：** V-LOCAL-TABLE + V-BUILD/V-STYLE；U03/L07及T9/T10/T16，先执行真实snapshot/pin动作再注入返回异常。回调副作用已被AP-01拒绝，不能启用生产回调放宽范围；实例重建仅证明重入，不夸称跨JVM崩溃。
**依赖：** AP-05。**文件：** CORE/PartitionArchiver.java、CORE_TEST/PartitionArchiverTest.java。**范围：** S，2文件。
**证据：** 未执行。

## AP-07：实现维护违约检测与停止规则

**任务：** 复查目标完整live集合、门槛和发布后分区状态，区分快照竞争与文件身份失效。

**验收：**

- [ ] before失效/复查变化按维护违约停止当前与后续、保留pin，不在当前CALL重规划；其他分区合法发布竞争允许内核原重试。
- [ ] 独立append不必OCC冲突：已发布但发现额外热文件返回INCOMPLETE；发布无法确认返回UNKNOWN，两者不混淆。
- [ ] 模拟同源归档、compaction/overwrite、维护变化及普通分区失败，确认停止/继续边界与未执行行输出。

**验证：** V-LOCAL-TABLE + V-BUILD/V-STYLE；L08及T8/T16；有界barrier/latch控制并发而非sleep，测试隔离表故意违约，不将其解释为支持任意并发。
**依赖：** AP-06。**文件：** CORE/PartitionArchiver.java、CORE_TEST/PartitionArchiverTest.java。**范围：** S，2文件。
**证据：** 未执行。

## AP-08：完成受保护历史与外部副作用拒绝门槛

**任务：** 把当前配置检查扩展到保留snapshot/tag/独立changelog及其引用schema，覆盖spec全部排除项。

**验收：**

- [ ] 当前关闭但历史仍引用DV/indexManifest/changelog/row-tracking/evolution/Iceberg时拒绝；历史读不全或预算超限fail closed。
- [ ] main/其他branch/fallback、未知extra布局、path-directory、bucket模式及custom/内建回调条件全部在pin/复制前阻断。
- [ ] 默认无副作用白名单部署可通过；普通BINARY不误拒；保留历史与已不可达过期历史边界按spec处理。

**验证：** V-LOCAL-TABLE + V-BUILD/V-STYLE；U04/L09及T11/T19，真实snapshot/tag/schema构造“选项已关但tag保留旧特性”，拒绝时确认无pin/目标副作用。
**依赖：** AP-07。**文件：** CORE/PartitionArchiver.java、CORE_TEST/PartitionArchiverTest.java；必要包内门槛helper至多1文件。**范围：** M，2–3文件。
**证据：** 未执行。

## CP-C：核心失败安全检查（AP-06–08）

- [ ] UNKNOWN/INCOMPLETE/维护违约/历史门槛反例均通过；未确认发布无删after，未冻结并发不虚报成功。
- [ ] 相关提交/scan回归、编译/风格通过；评审确认无安全占位逻辑，方可扩展实际索引与Spark入口。

## AP-09：支持真实embedded index与sidecar读取

**任务：** 根据extraFiles和toAlignedPath复制实际单个file-index，不按dataName.index推断源。

**验收：**

- [ ] embedded index原样保留；重建编号sidecar换fresh名字并写after.extraFiles；缺失/未知/多个index在提交前拒绝。
- [ ] 索引文件字节校验，真实谓词过滤与无索引基准一致，证明内容不依赖旧basename；失败则停止并修订支持范围。
- [ ] oldsnapshot/tag读原sidecar，newsnapshot读新sidecar；失败索引副本不覆盖或误删其他对象。

**验证：** V-LOCAL-TABLE/V-LOCAL-COPY + V-BUILD/V-STYLE；L05/L06及T2/T3/T5/T6，实际构建/重建索引与真实谓词结果，不只造文件名。
**依赖：** AP-08。**文件：** CORE/PartitionArchiver.java、CORE/ArchiveFileCopier.java、两个CORE_TEST类。**范围：** M，4文件。
**证据：** 未执行。

## AP-10：验证本地快照过期与孤儿回收

**任务：** 在隔离表中验证既有cleaner可配合fresh basename和标准登记布局，不新增自定义GC。

**验收：**

- [ ] user tag/source pin保护旧data/index，解除保护并满足保留条件后源可回收；live cold不得删除。
- [ ] 失败cold data/index超过olderThan后可回收，不因hot同名保护或候选碰撞遗留，不误删成功/其他分区文件。
- [ ] 本表独占根drop清理与邻表隔离正确；UNKNOWN/违约恢复完成前不恢复破坏性维护。

**验证：** V-LOCAL-TABLE + V-BUILD/V-STYLE；L10/L11及T13–T15；显式retention/mtime/cutoff而非等一天，仅操作本次拥有的精确路径。
**依赖：** AP-09。**文件：** CORE_TEST/PartitionArchiverTest.java；若暴露实现bug，仅CORE相关两类，额外修改先修订spec。**范围：** M，1–3文件。
**证据：** 未执行。

## AP-11：实现并注册Spark CALL合同

**任务：** 用BaseProcedure薄壳接核心，实现三个必填STRING参数及ROW<result STRING>。

**验收：**

- [ ] 表标识、完整分区列表及去重顺序正确；拒绝空/歧义/缺键/多键/重复键/无法转换/未登记根等输入，不隐式全表归档。
- [ ] 每个去重分区结果准确，表级错误整体报错；普通失败可继续，UNKNOWN/违约停止并输出NOT_ATTEMPTED。
- [ ] 默认支持组合经CALL达到与核心相同的读取/快照语义；注册无临时options覆盖持久配置或额外维护。

**验证：** V-BUILD + V-SPARK/V-STYLE；T1/T7/T11/T16，参照既有ExpirePartitionsProcedureTest的实际runner。
**依赖：** AP-10及CP-C审阅通过。**文件：** SPARK_PROC/ArchivePartitionsProcedure.java、SPARK_REG、SPARK_TEST/ArchivePartitionsProcedureTest.scala。**范围：** M，3文件。
**证据：** 未执行。

## CP-D：完整本地归档与CALL检查（AP-09–11）

- [ ] 索引过滤、历史读取、源/孤儿回收与CALL通过；所有表级门槛不可绕过。
- [ ] 核心/Spark报告、编译/风格完整；评审确认只代表本地能力，不提前声称跨存储已验证。

## AP-12：验证Spark缓存与增量消费者语义

**任务：** 覆盖部分成功缓存刷新、新session读取与常规NONE streaming，不扩展文件变化型消费者支持。

**验收：**

- [ ] 缓存查询及新session读最新cold引用；已发布后续失败/UNKNOWN/INCOMPLETE不阻止必要缓存刷新。
- [ ] 常规NONE stream checkpoint/resume不把归档COMPACT当业务新增；完整行结果与基准一致。
- [ ] FILE_MONITOR及依赖COMPACT完成层级含义的消费者边界明确测试/文档化，不由procedure猜测所有活跃外部消费者。

**验证：** V-BUILD + V-SPARK/V-STYLE；T4/T16/T17；stream启动/停止及checkpoint资源可靠清理。
**依赖：** AP-11。**文件：** SPARK_PROC/ArchivePartitionsProcedure.java、SPARK_TEST/ArchivePartitionsProcedureTest.scala。**范围：** S，2文件。
**证据：** 未执行。

## AP-13：完成local→MinIO跨scheme集成

> AP-PROBE已验证data-only复制/COMPACT/Spark混合读取前提；本任务要求真实core archiver和CALL入口，仍未完成。

**任务：** 新增直接调用core的PartitionArchiverS3ITCase与CALL层ArchivePartitionsS3ITCase；复用已有MinioTestContainer和Spark test-jar，不给core加S3依赖。

**验收：**

- [ ] 临时容器动态endpoint/凭据在catalog初始化前配置；file→S3和同容器两bucket的S3→S3真实读写，由新core IO/session/executor读取data/index。
- [ ] resolving关闭、凭据错误、插件不可用在发布前失败；实际读取/清理运行端访问条件均被验证，不重造table-only context。
- [ ] 三模式parquet基线及orc/avro/真实index针对性组合有明确结果；源/冷载荷与metadata等价，历史可读；无mock代替S3对象IO。

**验证：** V-BUILD + V-S3-CORE/V-S3-SPARK/V-STYLE；M01–M04/M06及T1–T5/T7/T12；分别记录两类实际参数/方法与非零用例，不以Scala wildcard替代。
**依赖：** AP-12、AP-00中Docker/MinIO前置条件成立。**文件：** S3_CORE_TEST、S3_TEST；必要测试helper1文件。**范围：** M，2–3文件。
**证据：** 未执行。

## AP-14：验证远端copy与发布故障恢复

**任务：** 在MinIO部署加入访问/close/内容损坏/发布确认不可用等故障，复核本地恢复设计。

**验收：**

- [ ] data/index等长损坏与copy/close故障不能发布；新调用不覆盖/复用失败目标，源保留。
- [ ] 发布已成功但调用异常可确认ARCHIVED；元数据暂不可读为UNKNOWN，保留after/pin且停止后续。
- [ ] 按M07追加受控forked JVM在“复制未提交/发布未返回”边界终止并由另一JVM恢复；pin定位/cleanup_required、计数与NOT_ATTEMPTED正确，不把未知判FAILED删cold。

**验证：** V-BUILD + V-S3-CORE/V-S3-SPARK/V-CORE/V-STYLE；M05/M07/M10及T6/T9/T10/T16，定点注入+真实对象损坏；失败不能skip，实例重建与受控子进程终止/恢复证据分别标注，仅控制本次启动的测试子进程。
**依赖：** AP-13。**文件：** S3_CORE_TEST、S3_TEST；必要CORE两类及CORE_TEST/PartitionArchiverTest.java。**范围：** M，最多5文件。
**证据：** 未执行。

## CP-E：读取端与远端故障检查（AP-12–14）

- [ ] Spark缓存/常规stream、真正跨scheme读取及远端失败恢复通过；只有MinIO/S3兼容结论，不扩大到所有介质/catalog。
- [ ] MinIO core与Spark目标suite分别非零用例、M01–M07/M10证据及编译/风格完整；M-core目标类不创建SparkSession，评审同意进入跨存储回收验证。

## AP-15：验证冷端回收与独占根drop隔离

**任务：** 将本地GC断言落到MinIO并验证具体catalog对登记根的删除行为。

**验收：**

- [ ] pin/tag保护hot data/index；解除保护满足retention后hot回收，成功cold引用持续可读。
- [ ] 失败cold data/index可orphan回收且不误删成功attempt/邻分区/邻表；清理端具备冷热FileIO。
- [ ] DROP只删除该测试表专属登记根，验证邻表对象仍在；不删除bucket根/warehouse或真实用户资源。

**验证：** V-BUILD + V-S3-CORE/V-S3-SPARK/V-STYLE；M08/M09及T3/T13–T15，按真实object mtime构造资格；先明确隔离目标再执行破坏性操作，不清理整个bucket。
**依赖：** AP-14、AP-10。**文件：** S3_CORE_TEST、S3_TEST。**范围：** S，2文件。
**证据：** 未执行。

## AP-16：验证资源预算与安全边界

**任务：** 测量有界传输/计划开销并落实失败前置、路径授权与敏感输出控制。

**验收：**

- [ ] 文件数/计划字节/历史预算超限在复制前失败；大文件buffer有界、内存与文件数关系有测量，默认预算有依据。
- [ ] 越界/别名/不同authority/共享根/权限拒绝/目标碰撞有反例；日志和输出不泄露密钥或带凭据URI，不越权ALTER。
- [ ] 实测后续写入/compaction可能重新变热，禁止宣称永久冷热策略；记录IO/摘要/副本成本，不以未实测比例宣传优化。

**验证：** V-CORE/V-SPARK/V-S3-CORE、V-BUILD/V-STYLE；U02/U04/L12及T7/T12/T18，目标碰撞/恶意路径/预算前后边界；保存测量条件与数值，不以跑完大文件代替峰值证据。
**依赖：** AP-15、AP-08。**文件：** CORE两类、两个CORE_TEST类、S3_CORE_TEST（最多5文件；Spark额外修改另拆任务）。**范围：** M，≤5文件。
**证据：** 未执行。

## AP-17：编写操作、恢复与限制文档

**任务：** 基于已验证行为完善Spark用户文档，记录如何安全调用和恢复，而不是只给SQL示例。

**验收：**

- [ ] 描述catalog加载前配置、持久根登记/独占所有权、维护冻结/等待清理、CALL结果与历史读取保留。
- [ ] UNKNOWN/INCOMPLETE/维护违约/pin释放失败均有人工确认步骤；恢复前不清理未知cold对象，不盲删所有archive pin。
- [ ] 支持矩阵、offline/FILE_MONITOR/外部镜像等排除项、后续再次变热与双份载荷成本可见；示例可在隔离环境复现。

**验证：** 人工逐项对照spec和已记录测试；复跑文档CALL示例使用V-SPARK/V-S3，文档格式/构建按仓库现有入口；不得写未测介质已支持。
**依赖：** AP-16。**文件：** USER_DOC；必要spec修正及plan/todo证据，≤4文件。**范围：** M。
**证据：** 初版文档阶段按用户要求提前编写使用指南并标注当时待实现的CALL。随后已更新为[开发分支使用指南](../archive-partitions-user-guide.md)，真实CALL已实现并运行本地写入/索引及故障测试，Spark procedure文档同步补充。GC/drop、资源测量及完整部署放行仍需AP-16等证据，本项保持整体未完成。

初版文档阶段仅改文档，4份Markdown/14链接/4段shell及RAT通过，当时没有执行CALL。当前实现阶段的构建、真实CALL、文档校验和原checkout保留证据统一见AP-CALL，不将旧探针报告重复计入新入口测试数。

## CP-F：部署前合同检查（AP-15–17）

- [ ] 跨存储GC/drop、安全/资源预算、文档操作与恢复实测完整；生产部署选择未确定则不得宣称生产可用。
- [ ] T1–T19及U01–U04/L01–L12/M01–M10均有实际测试方法/报告与任务证据，遗漏或MinIO环境阻塞列为未完成，评审确认后进入最终放行。

## AP-18：最终五轴评审与有效验证审计

**任务：** 用code-review-and-quality复核实现和完整证据；不将设计批准代替代码/运行批准。

**验收：**

- [ ] 五轴审查完成，Critical/Required修复；八不变量、T1–T19、U/L/M细化场景与实际支持矩阵都有有效证据；U/L无Docker与两层MinIO IT的执行结果分别可查。
- [ ] focused与相关既有commit/scan/cleaner/procedure回归、构建/风格通过，目标suite非零用例；选定Spark/profile矩阵有实际运行记录。
- [ ] 在隔离副本反转关键条件（如hash不等判定、UNKNOWN删after、URI边界、停止规则）使对应测试失败并恢复；最终diff无故意突变/skip/削弱检查。

**验证：** V-BUILD/V-CORE/V-SPARK/V-S3/V-STYLE + 已确认profile矩阵与相关回归；审计报告与残余限制，用户/指定评审者最终确认。
**依赖：** AP-17及全部前置任务/checkpoint。**文件：** `tasks/evidence/AP-18.md`、todo状态；发现修复按原归属任务回开并重新验证，不在“最终审查”塞入大型重构。**范围：** S（评审本身）。
**证据：** 未执行。

## CP-G：最终完成判定

- [ ] AP-00–18全部满足验收并有证据；未完成/环境阻塞未被勾选，所有高风险问题已修复。
- [ ] 用户或指定评审者接受实现与限制。PR/commit/push、真实表迁移、定时自动化须另有明确授权，本任务清单不自动执行。

## Spec验收覆盖索引

| spec场景 | 主要实施/验证任务 |
|---|---|
| T1 | AP-02、AP-04、AP-11、AP-13 |
| T2 | AP-02、AP-03、AP-04、AP-09、AP-13 |
| T3 | AP-04、AP-09、AP-13、AP-15 |
| T4 | AP-04、AP-12、AP-13 |
| T5 | AP-09、AP-13 |
| T6 | AP-03、AP-09、AP-14 |
| T7 | AP-01、AP-02、AP-05、AP-11、AP-16 |
| T8 | AP-07 |
| T9 | AP-06、AP-14 |
| T10 | AP-05、AP-06、AP-14 |
| T11 | AP-01、AP-08、AP-11 |
| T12 | AP-01、AP-13、AP-16 |
| T13 | AP-10、AP-15 |
| T14 | AP-10、AP-15 |
| T15 | AP-10、AP-15 |
| T16 | AP-05、AP-06、AP-07、AP-11、AP-12、AP-14 |
| T17 | AP-12、AP-14 |
| T18 | AP-02、AP-13、AP-16 |
| T19 | AP-01、AP-08 |

AP-18审计全部场景，不能因为某项在索引中出现就视为已经通过。完成状态只有本文件勾选且证据充分才成立。
