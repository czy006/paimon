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

# Spec: archive_partitions Phase 1 — Spark 分区冷数据归档

> 状态：**1.3.2 worktree已实现并注册Spark archive_partitions，完成新一轮保护/结果分类审查修正和默认1d活跃检查；当前core154、本地CALL28、真实MinIO CALL28及Scala11全部通过。** 证据见tasks/evidence/AP-REVIEW.md；原探针18例是独立历史证据，流消费、资源测量、GC/drop及生产部署验收尚未全部放行。
> 本次修订日期：2026-10-11。源码基线：`release-1.3`，HEAD `5c59e6cb01ed0b29563371f56e14fcade4597a2e`；根 POM 版本为 `1.3-SNAPSHOT`，该提交更新文档版本为 1.3.2，不能把 checkout 称为已发布的 1.3.2 二进制。
> 用户已授权在独立worktree按正式发布tag `release-1.3.2`（`c05f7d1f1b1e5d37e64edab0f2978124d90b64f7`，POM `1.3.2`）开始实施，构建/测试使用JDK11、公共源码target保持Java8。与原复核基线相比，src文件无差异；实施状态看tasks/todo.md，不能把规划测试当已通过。`external-data-paths-architecture.md`仍是master背景资料，其master-only功能不构成本阶段API依据。

## 1. 可行性结论与目标

**可实现，但原方案不能直接照写。** release-1.3 已有外部路径元数据、读取解析、文件元数据克隆、重写增量和快照提交机制。普通分区表可用“复制不可变文件及其附属索引 → 字节校验 → 原子替换文件引用 → 延迟回收旧文件”下沉到不同存储介质，不执行数据文件的合并、排序、解码或重编码。

这里的“移动”是最终物理驻留位置的迁移，不是提交前调用文件系统 rename。元数据使用现有 `CommitKind.COMPACT` 表达引用替换，**不运行数据 compactor**；提交过程仍可能合并 manifest，这属于元数据维护，不是数据 compaction。

每个完整分区独立提交。原子性覆盖该分区本次计划中的全部文件及全部 bucket，不覆盖一次 CALL 的多个分区，也不覆盖对象复制过程。归档完成后，当前快照读冷端，保留的历史快照/tag 继续读原位置。源副本只能在其引用不再受保护后由 Paimon 清理。

第一阶段保持 Spark CALL 入口，核心方案放在 paimon-core；不新增 Flink 入口、表格式字段、CommitKind 或第三方依赖。支持条件见 §4，实施顺序见 §13，验收门槛见 §14。

DV和动态分桶扩展的单独限制/可行性审查见 [DV与动态分桶限制记录](archive-partitions-dv-dynamic-limitations.md)。其中方案尚未实施，不能据此解除本阶段的DV、独立indexManifest或动态bucket拒绝条件。

### 1.1 两份 MD 的关系

| 背景资料中的机制 | release-1.3 核实结果 | 本阶段用途 |
|---|---|---|
| external-paths 分离逻辑归属与物理路径 | 已存在：DataFileMeta.externalPath、DataFilePathFactory.toPath | 归档后寻址基础 |
| round-robin / specific-fs / none | 已存在 | none 保持后续普通新写入走表内 |
| entropy-inject / weight-robin / provider 接口化 | 不属于本分支现有机制 | 不引用、不 backport |
| import_files / managed BLOB / master global-index 扩展 | 不可按背景 MD 推断本分支支持 | 不作为实现积木；普通 BINARY/VARBINARY 不等同于 managed BLOB |
| force-rewrite-all-files | 已存在 | 重编码迁移的替代路线，不是本方案步骤 |
| 外部路径自动解析 FileIO | 有条件成立，默认未开启 resolving FileIO | 必须满足 §5 的部署前提 |
| 孤儿清理/删表外部目录处理 | 已存在，但依赖登记路径、目录布局和文件名身份 | 必须按 §6、§11 设计，不能宣称无条件自动正确 |

### 1.2 本次审查修正记录

按照 code-review-and-quality 的正确性、可读性、架构、安全和性能五轴复核，并进行了独立只读审查。下面的 Critical 是原 spec 的实现阻断问题，Required 是必须写入实现约束的问题；表内修正已纳入本版本。未用“测试已通过”替代源码推理。

| 级别 | 原描述/问题 | 修正与原理 |
|---|---|---|
| Critical | 同 scheme 提交前 rename 快路径 | 全程复制；rename 会立即使旧快照路径失效，同 scheme 也不代表同 authority 或原子 rename |
| Critical | 长度相等 + 顺序 IO 保证字节相等 | 关闭写流后检查长度，并读回目标计算 SHA-256，与复制时源流摘要比较 |
| Critical | 固定目标路径；已有同长度对象直接复用/覆盖 | 每次尝试生成新的文件名；不信任同长度既有对象，不覆盖其他尝试的文件 |
| Critical | 只推导 dataName.index；存在才搬 | 使用 manifest 的 extraFiles 与 toAlignedPath；被引用索引缺失即失败 |
| Critical | 表 FileIO 总绑定表 scheme；单独目标 FileIO 足以保证读 | 要求整张表的实际 FileIO 能解析冷热 URI，读取/清理端共同可用 |
| Critical | 仅最新快照和当前选项就能排除 DV/changelog | 检查保留快照/tag/独立 changelog 和相关 schema；保守拒绝独立 indexManifest |
| Critical | 提交异常即可删除 cold after 文件 | 区分提交前失败与提交结果不确定；提交一旦尝试，不立即 abort/delete after |
| Critical | 确认 Paimon 发布即可证明回调/其他格式元数据一致 | 初版排除 Iceberg 镜像、自定义 commit/tag 回调及 metastore 映射副作用；Paimon 发布与回调不是一个原子事务 |
| Required | OCC 等于分区冻结，任何并发写都会冲突 | OCC 保护被替换文件身份；纯 append 可能成功，完整归档另需写入冻结 |
| Required | 同名复制失败残留自动被 orphan cleaner 回收 | cleaner 的候选和 used 集合按 basename；冷端换新名字以避免被热端同名保护 |
| Required | 全 skip 仍把旧文件放进 before/after | 跳过的文件不加入增量；完全 no-op 在 commit 前返回 |
| Required | 任意引擎/任意流模式完全无感 | 限定为支持 externalPath 的读取端；FILE_MONITOR 等读取文件变化的模式不支持 |
| Required | 与 expire/清理无交互；旧读者永远安全 | 明确维护窗口、快照保护、清理协调和读取保留时间 |
| Required | forceCopy 测试钩子、返回 bucket 数作为 files 数 | 统一走 copy，删除该设计钩子；分别统计数据文件、附属文件和字节 |
| Required | 全阶段 JDK 8、fast-build profile、Java IT 用 Scala wildcard | core/Spark3 目标 Java 8；Spark4 profile 为 Java 17；修正命令和 runner |

## 2. 原理与正确性不变量

1. **数据不变**：每个冷端数据文件/索引文件的长度及 SHA-256 与本次源流相等。摘要在传输/验证阶段计算，不解码行；不声称能修复归档前已损坏的数据。
2. **逻辑不变**：分区、bucket、totalBuckets、level、schemaId、rowCount、key/value stats、序列号范围、creationTime、deleteRowCount、embeddedIndex、fileSource 等保留。允许变更的 DataFileMeta 字段仅 fileName、extraFiles 的物理文件名、externalPath。
3. **提交前可读**：源文件和旧元数据始终保留；未提交 cold 对象不对读者可见。
4. **提交后可读**：同一次快照发布包含 DELETE(oldMeta) 与 ADD(newMeta)，全部 after 文件须已关闭、可读、校验完成。
5. **历史可读**：不更改历史 manifest/tag；历史可读性仍受其保留策略约束，批读没有无限期隐式 pin。
6. **失败不丢数据**：未确认提交结果时不删除可能已发布的 cold 对象；保留源文件，优先遗留可回收垃圾，而不是误删 live 文件。
7. **可清理**：目标专属于本表并登记到持久 schema；冷端文件名与任一源/其他尝试不重复；目录满足现有 cleaner 的遍历规则。
8. **无数据 compaction**：归档路径不调用表 writer、merge function、compactor 或强制重写选项。即使分区只有一个大文件，也执行原字节复制与引用替换。

元数据转换示例：

~~~text
旧快照 N:
  ADD  (dt=20240101, bucket=0, level=2,
        fileName=data-A.parquet, extraFiles=[data-A-2.index], externalPath=null)

归档快照 N+1 的 delta:
  DELETE 完整 oldMeta（包括原 externalPath、extraFiles、embeddedIndex）
  ADD    oldMeta.rename(data-B.parquet)
                .copy([data-B.parquet.index])
                .newExternalPath(s3://cold/orders/dt=20240101/bucket-0/data-B.parquet)

物理文件:
  hot/.../data-A.parquet             保留
  hot/.../data-A-2.index              保留
  cold/.../data-B.parquet            字节等价副本
  cold/.../data-B.parquet.index       索引字节等价副本
~~~

`DataFileMeta.rename` 只克隆元数据中的文件名，不执行文件系统 rename。索引位置来自 oldMeta.extraFiles，而不一定是 data-A.parquet.index。

## 3. 整体架构与读取路径

~~~mermaid
flowchart TB
    SQL["Spark CALL archive_partitions"] --> PROC["ArchivePartitionsProcedure<br/>参数校验、调用、结果、刷新缓存"]
    OPS["外部维护协调<br/>冻结目标分区；暂停破坏性操作和清理"] -. 前提 .-> PROC
    PROC --> ARCH["PartitionArchiver<br/>固定快照、计划、复制、校验、提交、恢复"]
    ARCH --> PLAN["ArchivePlan<br/>oldMeta、新文件名、extraFiles 映射"]
    ARCH --> COPY["ArchiveFileCopier<br/>流式复制 + SHA-256 + 目标读回"]
    COPY --> IO["已配置的 ResolvingFileIO<br/>按 scheme + authority 解析"]
    IO --> HOT["热存储<br/>源文件保持不动"]
    IO --> COLD["本表独占冷端根<br/>新名字的数据文件与索引"]
    PLAN --> COMMIT["FileStoreCommit<br/>CompactIncrement before / after<br/>OCC + 原子快照发布"]
    COMMIT --> META["表内 schema / manifest / snapshot<br/>新引用带 externalPath"]
    META --> READ["读取端 toPath / toAlignedPath"]
    READ --> IO
    GC["已有快照/tag过期与 orphan cleaner"] --> IO
    META -. 可达性 .-> GC
~~~

组件按职责划分，避免把归档策略塞进共享 FileIO。ArchivePlan、ArchiveResult 可作为核心类内部不可变对象；ArchiveFileCopier 负责传输与校验，不能决定发布或回收。小实现可包内组织，不为行数目标制造 pass-through wrapper。

~~~mermaid
flowchart LR
    Q["兼容的 Paimon 读取端"] --> S["选择快照或 tag"]
    S --> M["manifest DataFileMeta"]
    M --> P["toPath<br/>externalPath 优先，否则默认 bucket 路径"]
    M --> X["toAlignedPath<br/>extraFiles 与对应数据文件同目录"]
    P --> IO["ResolvingFileIO"]
    X --> IO
    IO --> H["热端旧文件"]
    IO --> C["冷端新文件"]
~~~

元数据继续放在原表目录。只有数据文件和被支持的 file-index sidecar 下沉；独立 index manifest/DV 等布局暂不支持。冷存储必须在线可读；需要 restore 后才能打开的离线归档存储级别不属于本阶段“透明读取”。

## 4. 支持范围与拒绝门槛

支持普通 FileStoreTable 的完整分区：PK 的 HASH_FIXED，以及 append 的 HASH_FIXED/BUCKET_UNAWARE。append bucket=-1 的物理 bucket 是 0，不凭 SQL bucket 参数拼路径。append fixed 支持依据 AppendOnlyFileStore.bucketMode；每一种模式均需独立验收，不能从 PK 测试推断。

| 条件 | Phase 1 要求/检测 |
|---|---|
| 表类型 | 普通分区 FileStoreTable；拒绝 format/system/特殊链式表和不明布局 |
| 分区参数 | keySet 与 schema.partitionKeys 精确一致；拒绝缺失、多余、重复键；类型转换后按 BinaryRow 去重 |
| bucket 模式 | 白名单为上段三种组合；拒绝 HASH_DYNAMIC、KEY_DYNAMIC、POSTPONE_MODE |
| PK merge engine | deduplicate、partial-update、aggregation；first-row的lookup/level-0语义尚未完成验证，当前拒绝 |
| snapshot发布协议 | 当前实现只接受filesystem-managed snapshot；catalog或表环境supportsVersionManagement=true时拒绝，包括RESTCatalog托管发布 |
| branch | 当前为 main，无其他分支，未配置 scan.fallback-branch；提交前重新检查 |
| DV/独立索引 | 当前及相关历史 schema 不启用 DV；保留快照/tag 的 indexManifest 均为 null；元数据不可读取即拒绝 |
| row tracking/evolution | 当前及被引用历史 schema 均关闭；所选文件 firstRowId/writeCols 均为 null |
| changelog | 当前及被引用历史 schema 的 changelog-producer=none；保留快照/tag 不带 changelogManifestList；ChangelogManager 中无保留独立 changelog |
| 附属文件 | extraFiles 为空，或只有一个经过格式识别的 .index；拒绝未知类型、多个索引及绝对/越界名字 |
| 目录 | data-file.path-directory 未设置；普通 partition/bucket-N 布局，避免现有 orphan 扫描深度错位 |
| 外部选址策略 | data-file.external-paths.strategy=none；target_path 已在持久 schema 登记 |
| 元数据镜像/回调 | 当前及相关历史 schema 的 metadata.iceberg.storage=disabled；当前 commitCallbacks()/tagCallbacks() 为空；metastore.partitioned-table=false、metastore.tag-to-partition 未设置、tag.create-success-file=false；不得有依赖外部镜像的活跃消费者 |
| 流消费 | 常规 changelog-producer=none 的增量读取可验证支持；活跃 FILE_MONITOR、文件增删订阅不支持；运维必须确认消费者模式 |
| 冷端 | 读取端支持其 FileIO/plugin、网络、凭据及 seek；目标存储在线，所有副本在发布前可读 |

历史检查范围是目前仍被 snapshot/tag/所选文件引用的 schema 和元数据，不是把所有过期历史当作当前 live 数据。检查失败不视为空。以上 DV/changelog/branch 限制是为第一阶段证明正确性而作的保守范围裁剪，不是断言这些机制永远不能归档。

managed BLOB 等 master 功能在本 checkout 不作为可检测的已有 API；不能编造 options/type 枚举，也不能禁止普通二进制列。对来源不明的文件结构按白名单拒绝。

回调门槛与snapshot发布协议是两项独立限制。当前确认/历史枚举基于文件系统snapshot，不能仅凭id低于catalog latest就证明REST权威发布，所以外部版本管理当前拒绝；这不是断言只有FileSystemCatalog才能扩展归档，其他使用相同文件系统协议的catalog需部署验收。AbstractFileStore会添加Iceberg、metastore映射或tag success-file回调，以上选项须排除。不要临时关闭生产镜像配置绕过历史依赖；扩展catalog/镜像支持需权威历史枚举、幂等同步与恢复协议。

对于不存在的分区返回 FAILED。以固定快照的 manifest/partition 元数据判断；“存在但无 live 文件”只在该快照确实保留分区记录时返回 NOOP，不能把已消失的分区承诺为成功。

## 5. 使用接口、部署配置与输出

操作说明见 [分区冷数据归档使用指南（开发分支）](archive-partitions-user-guide.md)：说明真实CALL、本地/MinIO测试、配置、维护窗口、查询核验、限制和恢复。入口已在本worktree实现，官方1.3.2发布二进制不包含此新增API；未完成的最终验收保持可见。

已实现的CALL有三个必填STRING参数table/partitions/target_path，另有可选STRING partition_idle_time，默认1d。table是当前catalog的表标识；partitions以分号列举完整spec；target_path是持久登记的单根。沿用compact的PartitionEntry.lastFileCreationTime和TimeUtils时长语法；从同一source snapshot汇总全分区所有bucket，最后文件创建时间<=检查时刻减阈值才可移动。0s显式关闭检查，负值/溢出/小于1ms的正值拒绝；未满足则NOOP/PARTITION_ACTIVE，无pin/复制/commit，不停止其他分区。该过滤不替代维护冻结；归档保留creationTime，后续新文件会重新使分区活跃。不提供通配符、部分键、全表默认选择或forceCopy，完整分区类型化去重。

~~~sql
-- 部署配置：创建/初始化 Spark catalog 前设置
SET spark.sql.catalog.paimon.resolving-file-io.enabled=true;

-- 冷端必须是本表独占目录；保留已有 external-paths 登记，不能覆盖其他历史根
ALTER TABLE paimon.db.orders SET TBLPROPERTIES (
  'data-file.external-paths'='s3://cold-bucket/paimon/db/orders',
  'data-file.external-paths.strategy'='none'
);

-- 使用本worktree构建的连接器，完成 §10 维护协调后调用
CALL paimon.sys.archive_partitions(
  table       => 'db.orders',
  partitions  => 'dt=20240101;dt=20240102',
  target_path => 's3://cold-bucket/paimon/db/orders',
  partition_idle_time => '7d'
);
~~~

catalog 已加载后再 SET 不保证重新配置既有 FileIO，应在新的配置正确的 session/catalog 中重新加载。外部根登记是已有表配置的持久变更，procedure 不静默执行 ALTER。不能只用 table.copy 临时 options，因为 orphan 和 DROP TABLE 要从持久 schema 找目录。

`FileIO.get` 在 resolving-file-io.enabled=true 时返回 ResolvingFileIO；其按路径的 scheme+authority 加载并缓存 FileIO。默认该开关为 false。Archiver 应复用已正确配置的 table.fileIO，同时对所有实际源 URI 和目标 URI做访问检查。禁止从 table.options 重造 CatalogContext 来“继承 catalog 凭据”：schema options 不保证包含 catalog options、Hadoop Configuration、preferIO/fallbackIO、插件环境或令牌。

跨存储的第一阶段要求实际表 IO 为 ResolvingFileIO（或另一个明确通过同等冷热访问测试的多文件系统实现），失败在复制前暴露。读者和 expire/orphan/drop 执行端也要配置该能力；仅归档 driver 能写 S3 不够。Spark executor 需能读取，其他引擎需各自部署和兼容性验证，不能宣称“任意引擎自动支持”。S3/MinIO 键名沿用本分支：s3.endpoint、s3.path.style.access、s3.access.key、s3.secret.key。凭据不写进 target URI、输出或异常正文。

保留 `ROW<result STRING>`、每个输入分区一行；文本采用固定字段，示例：

~~~text
dt=20240101: state=ARCHIVED operation=<uuid> source_snapshot=10 committed_snapshot=11 data_files=5 extra_files=2 data_bytes=12345 extra_bytes=678 skipped=0
dt=20240102: state=NOOP data_files=0 extra_files=0 skipped=5
dt=20240103: state=FAILED code=PARTITION_NOT_FOUND
dt=20240104: state=UNKNOWN operation=<uuid> source_snapshot=10 recovery_required=true
dt=20240105: state=INCOMPLETE operation=<uuid> committed_snapshot=12 observed_warm_files=1 maintenance_violation=true
~~~

data_files 是迁移数据文件数，不是 bucket/CommitMessage 数。skipped 是已引用目标根内文件的数据文件数。bytes 只统计确认成功归档的有效载荷，不伪装成整个过程网络传输量。普通分区失败可继续；UNKNOWN 或检测到维护协议违反必须停止后续分区，输出其余 NOT_ATTEMPTED，保留维护状态供恢复。INCOMPLETE 表示本次发布已确认、数据引用正确，但 fresh 检查发现额外热文件或其他分区冻结违约，不能当作完整归档成功。参数/部署/表级门槛失败直接令 CALL 报错，不伪装为部分成功。

## 6. 目标命名、所有权与幂等

target_path 必须是规范化绝对 URI：有支持的 scheme，远端有合法 authority，path 非根目录，无 query/fragment/userInfo，无 . 或 .. 越界语义。本地采用 file:///绝对路径，路径别名/符号链接的所有权另需验证。对配置根和目标根做规范化等值比较；对已归档文件判断 containment 时比较 scheme、authority 及完整路径段，禁止 String.startsWith（/orders2 不在 /orders 内）。

AP-01词法校验子增量使用包内ArchivePath及ArchivePathTest。目标URI在进入Paimon Path前检查，保守拒绝local authority别名、重复分隔符、编码分隔符/控制字符、解码后dot段及嵌套percent escape；普通必要字符编码（如%20）保持语义，scheme规范为小写，移除一个尾斜杠。该helper不能证明独占登记/IO插件可用或真实symlink关系。持久external-paths配置按现有Path(String)的未转义文本语义解析，不能把它和目标URI的Path(URI)解析混用；完整登记比较在后续表级门槛中验证。

每条目标根必须独占属于一张表。拒绝 bucket 根、warehouse 根、与表目录相等/互为祖先的同文件系统路径；不允许两表登记共享目录。DROP TABLE 可能递归删除登记根，单靠“目录当前为空”不能证明所有权，必须由部署命名规范和权限范围保证。未登记目标改为硬失败，附操作指引，不再只是 warning。

布局为：

~~~text
s3://cold-bucket/paimon/db/orders/
  dt=20240101/
    bucket-0/
      data-<new-uuid>-0.parquet
      data-<new-uuid>-0.parquet.index
~~~

实现使用现有相对 bucket 路径工厂。**每次尝试使用新的 UUID basename**，保存实际源格式和完整压缩后缀，不能按表当前 file.format 给旧文件换扩展名。可用 DataFilePathFactory.newPathFromExtension 辅助生成；其选路返回值只取文件名，目标目录由归档计划决定。索引独立换新名字并写进 newMeta.extraFiles。不增加 operation-id 中间目录，否则既有 cleaner 的分区深度不匹配。

newOutputStream(dst,false) 是必要条件；FileIO 不提供跨所有后端统一的原子 put-if-absent 承诺，UUID 隔离和独占根才是避免并发覆盖的基础。碰到已有 dst，不做覆盖/删后重建/仅长度复用；丢弃该命名计划，生成新名字或失败。

幂等按**已发布元数据**判断：若文件 externalPath 已处于合法目标根且所有引用文件存在、数据长度与 metadata 匹配，则跳过，不加入 before/after；引用文件缺失则 FAILED，不回退热端“碰运气”。完整 NOOP 无新对象、无 commit、无新增 pin tag。无 metadata digest 的既有文件不能靠再算一个无对照 SHA-256 声称完整性已重新证明；NOOP 只是保持既有引用。

本阶段不持久化复制进度日志，也不跨进程复用未提交副本。崩溃后重新扫描，未发布文件使用新名字重复制，旧尝试对象由协调后的 orphan 清理回收。这样避免共享半成品、同长度伪副本和复杂日志协议；代价是失败后可能重复传输。

## 7. 单分区执行时序

~~~mermaid
sequenceDiagram
    autonumber
    participant O as 运维协调
    participant P as Spark Procedure
    participant A as PartitionArchiver
    participant H as 热端 FileIO
    participant D as 冷端 FileIO
    participant C as FileStoreCommit
    O->>O: 冻结目标分区并暂停清理/破坏性操作
    P->>A: archive(fullPartitionSpec)
    A->>A: 固定快照 S；检查历史/布局/IO；枚举 live ADD
    alt 全部已在目标根
        A-->>P: NOOP，无提交
    else 需要迁移
        A->>A: 建立无 TTL 的 source pin tag；生成新名字与 extra 映射
        loop 每个待迁文件及其引用索引
            A->>H: 读取源不可变字节并计算 SHA-256
            A->>D: 写新对象，关闭输出流
            A->>D: 读回长度与 SHA-256
            D-->>A: 校验通过
        end
        A->>A: 重查条件/所选分区文件集合，构造 before/after
        A->>C: 一次纯 CompactIncrement commit
        C->>C: 文件身份冲突检测与原子快照发布
        C-->>A: 返回或抛异常
        A->>A: 确认发布身份及 fresh manifest 引用
        A-->>P: ARCHIVED / INCOMPLETE / FAILED / UNKNOWN
    end
    O->>O: 结果确认后释放本次 pin/恢复维护；旧引用按保留策略清理
~~~

“所有 after 已就绪 → 发布快照”是线性化点。可见性不依赖对象存储 listing；应直接按 manifest 指定 URI 检查。关闭流之前，exists/length 都不是完成证明。

### 7.1 核心算法（设计伪代码，不是可编译交付）

~~~java
ArchiveResult archive(Map<String, String> spec) {
    // 所有参数和环境失败须发生在数据复制前。
    BinaryRow partition = validateExactKeysAndConvert(spec);
    Snapshot source = loadFreshLatestSnapshot();
    validateScopeAndRetainedReferences(source);
    List<ManifestEntry> live = scanAllAt(source, partition); // keepStats，仅 live ADD
    ArchivePlan plan = planWithFreshNames(live, targetRoot);

    validateAlreadyArchivedReferences(plan.skipped());
    if (plan.changedFiles().isEmpty()) {
        return noopWithoutCommit(plan);
    }

    // 建 pin 时必须已协调 tag creation 与 expiration；tag 不是整个维护协议。
    String pin = sourcePinName(operationId, partitionIdentifier);
    ArchiveResult outcome;
    try {
        createAndConfirmSourcePinWithoutTtl(pin, source.id());
        for (FileMapping mapping : plan.mappings()) {
            copyAndVerify(mapping.data());
            for (FileCopy extra : mapping.extras()) {
                copyAndVerify(extra);
            }
        }
        revalidateMaintenanceScopeAndSelectedLiveSet(source, partition);
        List<CommitMessage> messages = changedPairsByBucket(plan);

        // UUID commitUser + 单调分区 identifier；同一次提交重试保持二者不变。
        ManifestCommittable committable = new ManifestCommittable(partitionIdentifier);
        messages.forEach(committable::addFileCommittable);
        try (FileStoreCommit commit = table.store().newCommit(commitUser, table)) {
            commit.ignoreEmptyCommit(true);
            markCommitAttempted();
            commit.commit(committable, false);
        }
        outcome = confirmFreshPublishedReferencesAndSnapshot(plan, commitUser, partitionIdentifier);
    } catch (Exception failure) {
        // 复制前/提交前失败与提交结果不确定采用不同处理，见 §9。
        outcome = classifyAndRecoverWithoutDeletingPossiblyPublishedFiles(plan, failure);
    }
    // UNKNOWN/维护违约保留 pin。清理异常不得覆盖已确认的提交结果。
    return tryReleaseOwnedPinOrAttachCleanupRequired(outcome, pin);
}
~~~

上述是合同伪代码，辅助名称不作为公共API。实际实现见PartitionArchiver/ArchiveSafety/ArchiveFileCopier：扫描前先按manifest计数/字节预算拒绝超限，再以withSnapshot、ALL/keepStats和完整分区过滤顺序枚举；存在性和文件计划针对同一快照，保留oldMeta全字段。真实catalog重新加载与显式schema ID/选项/表UUID比较，不依赖带动态时间戳的TableSchema.equals。实例只允许一次archive调用，防止UNKNOWN pin名被复用。

对每个有变化的 bucket：

~~~java
DataFileMeta after = before.rename(newDataName)
        .copy(newExtraNames)
        .newExternalPath(dst.toString());

CommitMessage message = new CommitMessageImpl(
        partition, bucket, originalTotalBuckets,
        new DataIncrement(emptyList(), emptyList(), emptyList()),
        new CompactIncrement(changedBefore, changedAfter, emptyList()));
~~~

校验同 bucket 的 totalBuckets 一致并保留原值。changedBefore/changedAfter 一一对应，skipped 不在其中，空 bucket 不产生消息。没有 data/changelog/index append 增量，FileStoreCommitImpl 的 compact 分支发布 COMPACT；不关闭 ignoreEmptyCommit，否则可能额外产生空 APPEND。

commitUser 为 archive_partitions-<operationUUID>，同 CALL 的分区 identifier 单调递增；禁止固定所有作业的 user 配 System.nanoTime 假设跨 JVM 唯一。原内核重试和成功识别按 user+identifier+kind；关闭 commit 对象，避免资源泄漏。选用原始 FileStoreCommit 的原因是保持纯文件引用提交，不顺带触发 TableCommitImpl 的 partitionExpire 等维护；后续回收另行协调。原始 commit 仍会加载回调，不能把绕过 TableCommitImpl 误认为绕过回调；§4 的副作用门槛必须在创建 pin 和提交前成立。

## 8. 传输与索引完整性

copyAndVerify 的协议：

1. 校验 src/dst 都在允许范围内；确保目标 bucket 目录存在。源 data 按 toPath(oldMeta)，源 extra 按 toAlignedPath(extraName,oldMeta)。
2. 从源 data 状态读取长度，必须等于 oldMeta.fileSize；extra 的预期长度从源 FileStatus 得到。引用源缺失/拒绝访问立即失败。
3. 使用固定大小缓冲区顺序复制，计算源 SHA-256 和实际字节数；只写本次唯一新对象，禁止覆盖。
4. 关闭输出流，确认 close 没有失败；目标 getFileStatus 检查长度，再 newInputStream 读回计算 SHA-256 和字节数；全部匹配才标记 VERIFIED。
5. 完成所有 data 和 extras 后才生成可提交 after 列表。嵌入式 index 留在元数据内，字节原样保留。

SHA-256 来自 JDK MessageDigest，无新增库。它提供强内容一致性校验，不是数学上的零碰撞保证。S3 ETag 可能不是内容 MD5，不能替代；加密/多段上传也不能直接作等价证据。

`extraFiles` 是权威索引引用。只允许经识别的单个 file index，复制其真实文件名（例如重建版本 data-A-2.index），将新名字登记在 after.extraFiles。文件内容不依赖把名字重新推导成旧 dataName.index；读取端使用 metadata 中的 extra 名和 aligned path。若实际索引格式出现内部文件名绑定，须通过测试证明可克隆，否则拒绝/扩展协议，不猜测。

未知未被 manifest 引用的 sidecar 不纳入读取承诺。若配置开启 file index，但既无 embeddedIndex 也无 extra 索引引用，不凭 exists 某个推导名字改变 metadata。谓词读取测试必须覆盖实际生效的 sidecar，不只检查对象存在。

复制前失败可尝试删除本次拥有的半成品（检查删除结果并记录残留）；复制完成后/提交尝试后统一保留目标对象供确认与清理。deleteQuietly 不等于回滚成功，不作正确性证明。不删除源，不调用 FileStoreCommit.abort 删除结果不确定的 after 文件。

## 9. 状态、崩溃与提交结果不确定

~~~mermaid
stateDiagram-v2
    [*] --> Planned
    Planned --> Copying
    Copying --> Verified: 全部副本关闭并校验
    Copying --> FailedBeforeCommit: IO或校验失败
    Verified --> CommitAttempted
    CommitAttempted --> Committed: 发布身份与引用确认
    CommitAttempted --> Conflict: 确认未发布且before已失效
    CommitAttempted --> Unknown: 超时或元数据无法确认
    Unknown --> Committed: 恢复确认已发布
    Unknown --> FailedBeforeCommit: 恢复确认未发布且不可再发布
    Committed --> Incomplete: 发现额外热文件或维护违约
    Incomplete --> RecoveryRequired: 保留pin并重新建立维护窗口
    Conflict --> RecoveryRequired: 文件身份冲突视为维护违约
    Committed --> SourceReclaimable: 旧snapshot和tag解除保护
    SourceReclaimable --> ColdOnly: 正常GC回收源
    FailedBeforeCommit --> OrphanCandidates
    RecoveryRequired --> OrphanCandidates: 结果确认后仅处理未发布副本
    OrphanCandidates --> [*]: 协调后的orphan清理
~~~

| 中断位置 | 现场与处理 |
|---|---|
| 复制中/目标 close 失败 | 源和 live metadata 未变；本次部分目标不提交；恢复使用新名字重复制 |
| VERIFIED 但还未调用 commit | 源仍 live；冷副本未发布，随后可成为孤儿候选 |
| 确认文件身份 OCC 冲突未发布 | before失效说明分区冻结前提破坏；停止当前及后续分区、保留pin并报告维护违约。不在当前CALL自动完整重试；重新建立维护窗口后才能重新规划，旧冷对象延迟回收 |
| 已发布但调用方没收到响应 | 不能把异常当未提交；查 user+identifier+COMPACT 和 fresh manifest；成功则返回 ARCHIVED |
| 提交结果无法确认 | UNKNOWN，保留全部冷文件/source pin，停止后续分区和清理；需要恢复处理 |
| 成功后、返回前崩溃 | 重入按 fresh 当前 metadata 判定 NOOP；保留的 operation pin 在确认后释放 |
| 新进程重跑未提交尝试 | 不复用旧 cold 对象；新名字重复制，旧对象延迟回收 |

确认必须从可靠的新鲜元数据读取，避开旧 snapshot/manifest cache，并使用对应 catalog 的权威快照来源；不能把直接枚举文件系统当作所有 REST/其他 catalog 的权威实现。失败后仅靠 latest snapshot id 变化不足以证明本操作成功；使用 commitUser、identifier、kind 和本次 after 物理引用匹配。latestSnapshotOfUser 可辅助，不替代对引用和身份的验证。无法获得确认时维持 UNKNOWN，不扫描不到就删除。

区分两种冲突：目标分区 before 身份失效是维护违约；其他分区合法提交导致快照发布竞争但本分区身份仍有效时，可由 FileStoreCommit 内核按原有逻辑重试同一 committable。Archiver 不把内核发布重试扩大为复制新文件、追赶目标分区变更的完整重规划重试。

本次 unique pin 名使用 operation UUID 与分区 ordinal，并可由运维识别。通过 createTag(name,sourceId,null) 创建无 TTL pin，避免沿用表默认 TTL；确认目标对象和引用后才调用 deleteTag。本 pin 保留源数据/tag 元数据，不永久保留 numeric snapshot 文件，因此恢复可从 tag 读取源快照。临时 tag 的创建/删除须受 §10 维护协调；创建本身不是与 expiration 的自动原子事务。

先生成并记录本次 pin 名，再尝试创建。创建可能已经写入 tag 却因 callback/响应失败而抛异常；需按该名字确认状态，不创建第二个未知 pin。只有确认 source pin 有效才开始复制。释放 pin 也可能删除已完成但 callback 失败：重新确认本次 tag 是否仍在。释放失败只附加 pin_name/cleanup_required=true，不把已确认 ARCHIVED 改写成“未提交 FAILED”，也不让 finally 异常覆盖 UNKNOWN；此时源空间释放可能延后，待维护窗口内处理。

审查修正：ArchiveProtection在采集的完整tag映射内确认本次pin的精确source snapshot和无TTL属性，记录当时保留的snapshot/tag；复制后、确认发布后及清理前核对，检测删除/改向/TTL或历史过期。保护尚未确认返回FAILED/SOURCE_PROTECTION_UNCONFIRMED并停止；发布已确认而保护变化/不可验证返回INCOMPLETE，保留committed_snapshot，不自动释放pin。只有真正调用过deleteTag，响应异常后确认不存在才能视为删除成功。原生FileStoreCommitImpl明确的发布前冲突与普通IO错误分开；无本次发布且具备原生冲突证据时FAILED/维护违约，否则UNKNOWN。

没有进度日志意味着进程丢失后不能只从一个随机 cold 对象证明其可复用。恢复在维护窗口内重新查当前引用；能证明操作已结束才释放相关 pin。自动扫描并删除所有 archive pin 超出本阶段范围。

## 10. 并发与维护协议

**release-1.3 没有本方案可直接调用的“封存分区”原子 API。** procedure 无法通过两次文件集合比较证明未来不会再 append。整体分区归档成功的运行前提，是外部协调把目标分区冻结在整个 CALL 期间；不可把建议写冻结降成可选条件。

初始可交付运行模式为受控维护窗口：

- 停止目标分区的 append/upsert/delete/overwrite、compaction、bucket rescale 以及会产生晚到数据的 writer；普通只读查询继续。
- 暂停该表的 expire_snapshots、expire_partitions、remove_orphan_files、自动维护、tag/branch 变更、rollback/drop、schema/layout 变更；必须确认已有异步清理任务已结束。
- 单个窗口只运行一个本表 archive CALL。其他分区写入只有在其 writer 不触发上述自动维护、且协调系统明确保证时才允许；最简单部署是暂停表写入。
- source pin 在无 expiration 竞争时创建，期间不能被自动/人工移除。清理协调不能靠默认 olderThan=一天代替，长任务也可能超过保护窗口。
- 完成发布及冷端访问验证后释放本次 pin，按保留策略恢复清理；UNKNOWN 继续维持保护直到确认。

以上是部署协议，**不是已存在的原生锁**。普通 commit/catalog 锁只保护快照发布，不把整个复制窗口锁住；本阶段不编造一个所有 writer/cleaner 都会遵守的租约。未来如果要求无人值守、任意并发写入和清理下强保证，需要额外封存/维护协调协议，本 spec 不声称仅加 procedure 能做到。

OCC 细节：FileEntry.Identifier 包含 partition、bucket、level、fileName、extraFiles、embeddedIndex、externalPath。其他操作删除/替换 before 时，本次 DELETE 无法与最新 ADD 抵消，提交失败；同源并发归档也应冲突。但独立新增文件可以与本次引用替换同时存在，OCC 不会一定报错——数据仍正确，分区却未完全变冷。

提交前 fresh 扫描必须比较目标分区完整 live 身份集合（包含原本 skipped），并重新验证门槛；发现变化停止当前及后续分区并报告维护违约，不在违约窗口自动反复追赶写入。这是防御性检查，不能替代外部冻结。归档后 fresh 扫描也应确认本次计划全部 after 已发布、skipped 保留，无额外热端 live 文件；发布可确认但发现额外热文件时返回 INCOMPLETE，保留协调/pin供处理。发布或引用状态无法确认则返回 UNKNOWN，不混淆两者。

读取端的隔离只保证在其使用的快照/tag受保留保护时可读。长批读需足够 snapshot retention 或显式受保护 tag；streaming consumer 按既有 Consumer ID 机制保护进度，不能把只读操作理解为自动锁住文件。

## 11. 回收、生命周期和后续写入

release-1.3 的普通 snapshot 数据删除按实际 Path 合并 DELETE/ADD：不同 externalPath 可区分热旧/冷新。tag skipper 和 LocalOrphanFilesClean 的 used/candidate 集合有 basename 语义。保留相同 basename 的 cold 失败副本会被 hot 引用保护，并可能让候选 Map 合并不同 URI；因此本设计同时更换数据与索引文件名，以复用已有清理行为，而不是修改 cleaner。

热副本在旧快照/tag解除保护后由已有 expiration 删除，索引按旧 extraFiles 一起删除；不能只说“旧快照到期立即释放空间”，还受 pin、用户 tag、consumer、保留数及清理执行状态影响。新冷副本被新 manifest保护。orphan cleaner 只扫描已登记的普通分区/bucket 根；无登记、非标准深度、未知共享根的行为不能承诺。

各发布状态的清理规则：

| 状态 | 源 | 目标 | 维护 |
|---|---|---|---|
| ARCHIVED | 保留到旧引用解除保护后再正常 GC | live 文件不得删除 | 确认后释放本 pin，恢复正常清理 |
| INCOMPLETE | 保留源/pin，重新建立分区冻结 | 已确认发布的 cold 引用仍有效，不回滚/删除 | 停止后续，处理维护违约，再规划剩余热文件 |
| FAILED 且确认未发布 | 不动原 live 文件；若维护违约则保留pin | 延迟 orphan 回收；也可能仍被未合并 DELETE 元数据保守保护 | 普通失败解决后可重新计划；维护违约停止后续，重建窗口后再处理 |
| UNKNOWN | 保留源/pin | 全部保留，不主动 abort/清理 | 继续协调并先确认结果 |

注册的 root 为本表专属，dropTable 收集历史 schema 中 external-paths；具体删除由 catalog 实现处理，不把“有收集代码”当所有 catalog/权限/IO都保证回收。DROP 成功删除了哪些目录仍需实际验证。

本阶段不是永久冷热策略引擎。strategy=none 使普通新文件仍写表内：目标分区若重新写入会混合冷热；以后 compaction 可能把冷文件重新写热。保持归档分区冻结，避免通用 compaction 调度再次重写；如以后确需冷端 compaction，应在专门任务里显式配置冷端输出并另行验证。归档不会主动把小文件压实，冷端读性能保留原文件数量/LSM 结构。

## 12. 读取语义、性能与安全边界

常规批读：用当前/指定 snapshot 的完整 manifest 构建 split，toPath/toAlignedPath 指向同一套有效载荷，原统计、level、序列范围保留，行结果应一致。包括 PK tombstone、更新覆盖、聚合/partial-update 等情况时仍应由原 merge function读取；必须以实际测试验证，不能只比较 COUNT。

普通 changelog-producer=none 的 DeltaFollowUpScanner 只消费 APPEND，跳过归档 COMPACT，避免将复制当新增业务行。FILE_MONITOR 的 AllDeltaFollowUpScanner 会读取文件变化，不满足该语义；按 §4 排除。COMPACT 标记仍可被 compacted-full/startup 或其他工具观察到，不保证它代表“所有 level 已压到最大层”；依赖这个标记的消费者需要单独评估，不用读取配置自动推断所有外部客户端。

复杂度：数据量 D、sidecar量 I、文件数 F。顺序复制 O(D+I)，目标全量读回再 O(D+I)，摘要计算也有 CPU 成本；源完整读一次，冷端写一次、读一次。避免的是格式解码/编码、排序和合并成本，不能写成零 CPU/零 IO或绝对“双倍节省”。峰值额外空间约等于本次复制载荷，加上失败尝试残留；原副本暂不释放。

第一版数据传输顺序、有界缓冲（建议 1 MiB），不建无界 future/task 队列。文件计划内存 O(F)，通过枚举时的明确文件数/计划字节预算限制单分区大小；预算超限在复制前失败，提示拆分/调整经过评估的预算，不按 bucket 分多次提交破坏分区原子性。历史引用检查同样有范围/预算，超限 fail closed；实现时测量并记录默认预算，不能承诺一个未测的文件数适用于所有表。snapshot manifest merge 是既有内核行为，不算规避失败。

仅需已有 FileIO、IOUtils/顺序流、JDK MessageDigest/UUID。校验 URI、extra 文件名、表类型和完整分区键；复制目标不能逃出独占根。调用方必须拥有表提交及冷端写/读权限；所有运行端具备访问凭据，不能通过 procedure 绕过 catalog/后端授权。异常做安全摘要，日志可定位 operation/分区/阶段，不输出密钥或完整凭据 URI。

## 13. 实施范围、顺序与构建

### 13.1 实现方案与逐项可行性审查

以下是后续实现的任务合同，不是声称对应归档类已经存在。§7 给出算法，§3 给出架构，表内把各步骤与本分支已有能力、必要新增逻辑和验收一一对应。

| 步骤 / 实现内容 | 可复用能力及新增逻辑 | 可行性判断 / 放行依据 |
|---|---|---|
| A. 入口与门槛 | Spark BaseProcedure/SparkProcedures；新增精确分区解析、表模式/历史引用/回调/URI/预算检查 | 可实施；拒绝范围先于复制，T7/T11/T19；维护冻结由运维提供，不能由参数检查代替 |
| B. 固定快照与计划 | FileStoreScan 固定 snapshot、ALL/keepStats；新增每 bucket 的 old/after 一对一映射和 fresh basename，保留 skipped | 可实施；无需目录 listing 推断 live；T1/T2/T7/T18 |
| C. 源保护与传输 | 已有无 TTL tag API、ResolvingFileIO、toPath/toAlignedPath；新增有界流式复制、源摘要、关闭流后目标全量读回 | 有条件可实施；必须协调清理且冷热端在线可读，T3/T5/T6/T12/T13 |
| D. 元数据克隆 | PojoDataFileMeta.rename/copy/newExternalPath；新增仅允许物理三字段变化的校验 | 可实施；不解码/重编码数据，普通 sidecar 内容无外部名字绑定需 T2/T5 证明 |
| E. 原子发布 | CompactIncrement + CommitMessageImpl + FileStoreCommit；新增按分区一次提交及稳定 user/identifier | 可实施；原子性来自快照发布而不是文件复制，OCC 不冻结分区；T1/T4/T8 |
| F. 确认和异常恢复 | 原提交身份与 snapshot/tag/manifest 可查询；新增 fresh 权威确认、UNKNOWN/INCOMPLETE分类、停止后续和 pin安全释放 | 有条件可实施；元数据不可确认就保持 UNKNOWN，不能承诺自动恢复所有崩溃；T9/T10/T16 |
| G. 读者与回收 | 已有 externalPath 读取、expiration/orphan/drop；新增部署检查和 Spark部分成功缓存刷新 | 有条件可复用；根登记、独占、新basename和标准布局不可省；T4/T13/T14/T15/T17 |

实施时固定这些决策：复制而非源 rename，原因是历史读者仍持有旧路径；换新 basename 而非同名复制，原因是现有 cleaner/tag 的 basename 保护；复用 COMPACT 而非新快照类型，原因是已有 before/after 原子发布，但必须排除文件变化型消费者；外部维护窗口而非伪造内核分区锁，原因是现有 OCC 不能排斥独立 append。代价是额外副本与冷端读回 IO，以及本阶段无法无人值守地对任意并发写入作强保证。

### 13.2 后续文件清单与实施顺序

下列文件构成整体实施范围；已实现的子增量及其证据以tasks/todo.md为准，未完成项不得视为已有API：

| 文件 | 职责/改动 |
|---|---|
| paimon-core/.../operation/PartitionArchiver.java | 固定快照、门槛、计划、提交、结果确认、pin生命周期 |
| paimon-core/.../operation/ArchivePath.java、ArchivePathTest.java | 包内URI词法政策与纯单元测试；不代替文件系统所有权/IO/登记验证 |
| paimon-core/.../operation/ArchiveFileCopier.java | 包内复制与强校验，便于注入故障 FileIO |
| ArchivePlan / ArchiveResult | 可为包内/内部不可变类，不新增通用公共接口 |
| paimon-spark-common/.../procedure/ArchivePartitionsProcedure.java | BaseProcedure 参数/结果薄壳，成功和部分成功后刷新缓存 |
| paimon-spark-common/.../SparkProcedures.java | import + procedure 注册；不声称只需物理改一行 |
| paimon-core/.../operation/PartitionArchiverTest.java | 本地/故障注入单测 |
| paimon-core/.../operation/ArchiveFileCopierTest.java | 无Docker的本地真实文件复制、缓冲边界与定点IO故障 |
| paimon-spark-ut/.../procedure/ArchivePartitionsProcedureTest.scala | CALL 和 Spark 缓存/读取集成 |
| paimon-spark-ut/src/test/java/org/apache/paimon/operation/PartitionArchiverS3ITCase.java | 直接调用core的MinIO模拟S3测试，不启动Spark；使用该测试模块已有S3依赖 |
| paimon-spark-ut/.../ArchivePartitionsS3ITCase.java | JUnit MinIO 跨 scheme、读回、恢复及清理 |
| 对应用户文档 | 已核实语义、配置、运维前提、限制 |

核心不需要预设修改已有格式、FileIO、compactor 或 cleaner。设计保持源代码改动集中，但不把“只新增类”作为覆盖正确性问题的理由；未来测试证明需要额外改动时，先修订设计和范围。

按可验收增量实施：①门槛/命名和 metadata 转换测试；②copy/hash/sidecar 与故障注入；③COMPACT 发布、pin与结果不确定恢复；④Spark procedure和缓存；⑤MinIO实际跨域读写/GC；⑥五轴最终审查及用户文档。每步都有 §14 对应断言，不能一次写完整实现后仅跑 happy path。

Java：core/Spark3 公共代码目标 Java 8，避免新语法/API；Spark4 profile 将 target.java.version 设为17，Scala2.13，需相应 runtime。使用当前 checkout/profile 合适的 Maven/JDK 并先核实 mvn -v；不重用其他仓库的工具链。根 pom 未定义 fast-build，不能把 -Pfast-build 当已存在的跳过检查开关。

未来验证命令（本次未执行；先安装 reactor 所需依赖）：

~~~shell
mvn -Pspark3 -pl paimon-core,paimon-spark/paimon-spark-common,paimon-spark/paimon-spark-ut -am -DskipTests install
mvn -pl paimon-core -Dtest=PartitionArchiverTest,ArchiveFileCopierTest test
mvn -Pspark3 -pl paimon-spark/paimon-spark-ut -Dtest=none -DfailIfNoTests=false -DwildcardSuites=org.apache.paimon.spark.procedure.ArchivePartitionsProcedureTest test
mvn -Pspark3 -pl paimon-spark/paimon-spark-ut -Dtest=PartitionArchiverS3ITCase,ArchivePartitionsS3ITCase -DfailIfNoTests=false integration-test
mvn -Pspark3 -pl paimon-core,paimon-spark/paimon-spark-common,paimon-spark/paimon-spark-ut spotless:check checkstyle:check
~~~

Java IT 使用 Surefire，不用 Scala wildcardSuites。根 pom 的 integration-tests execution 绑定 integration-test；执行时核查 Surefire 报告中的 ITCase 实际用例数，-DfailIfNoTests=false 不能当成功证据。ScalaTest 插件可能同时运行，实施时核实其实际范围。MinioTestContainer 与 paimon-s3/test-jar 已由 Spark 父 POM提供，不添加 paimon-core 的 S3 依赖；Docker需运行。HDFS→S3、OSS 实际集成属于部署支持扩展，MinIO只能证明 S3兼容路径，不能声称所有介质已实测。

## 14. 测试、验收与评审结论

下面是**完整归档功能的验收计划**。实施阶段已验证ArchivePathTest的URI子集、既有core/Spark基线，以及§14.6的独立混合存储探针；证据写入tasks。探针不调用尚未实现的archive_partitions，不能把这些局部结果视为下表完整归档/MinIO场景已通过；反例表本身不是运行证据。

| 编号 | 未来测试（core / Spark / MinIO） | 必须断言 |
|---|---|---|
| T1 | append fixed、append unaware、PK fixed；单个大文件及多bucket | 全量行结果多重集相等；不丢重复/删除/更新语义；bucket裁剪正确；只产生一次纯 COMPACT |
| T2 | 每对 old/after metadata、冷端有效载荷 | 仅三种允许字段变更；全部其余字段相等；data/index长度与SHA-256相等 |
| T3 | 所有复制阶段停顿，同时读旧snapshot/tag | 热文件与索引一直存在；旧快照可读；未发布冷对象不可见 |
| T4 | 发布前后 reader、new session/executor和缓存查询 | 旧snapshot热端、新snapshot冷端；缓存正常刷新；新FileIO能读；无中间引用状态 |
| T5 | embedded index及重建编号sidecar，有/无索引谓词 | actual extraFiles正确映射；带过滤SELECT与无索引基准一致；缺失索引在提交前失败 |
| T6 | copy中断、close失败、目标同长度内容损坏、访问失败 | 无提交，源可读；hash可发现等长损坏；残留不会被复用/覆盖 |
| T7 | 全部/部分已归档；相邻URI前缀、不同authority | 全NOOP无快照/新对象/pin；部分只替换变化文件；边界路径判断正确 |
| T8 | 两个同源计划、并发compaction/overwrite、独立append及其他分区快照发布竞争 | 失效before触发冲突并停止后续，不自动重规划；append不必冲突且不被误称为完整归档；合法快照发布竞争可内核重试 |
| T9 | 发布前失败、发布成功但响应丢失、确认元数据不可读、pin创建/删除callback失败 | FAILED/ARCHIVED/UNKNOWN准确；不删除可能已发布after；UNKNOWN保留pin并停止后续；pin异常不覆盖发布结果 |
| T10 | 模拟进程崩溃后新调用、跨JVM operation身份 | 新对象名不共享；已发布重入NOOP；旧尝试孤儿可回收；pin可定位/释放 |
| T11 | options关闭但历史仍有DV/index/changelog/evolution；各种拒绝模式 | 表级门槛在复制前阻断；history不可读fail closed；普通BINARY不误拒 |
| T12 | file→MinIO/S3，resolving关闭/凭据错误/插件缺失 | 正确配置端到端可读；不正确部署在发布前失败；源也是外部URI时按实际scheme读 |
| T13 | user tag/source pin存在与解除，snapshot expiration | 受保护hot/data/index不删；保护解除后hot回收；live冷数据不删 |
| T14 | 已登记根内失败data/index残留超过olderThan，orphan清理 | 源仍live时不同名的冷失败副本可回收；不误删另一分区/表/成功尝试 |
| T15 | 独占根DROP TABLE、本地目录和MinIO | 验证具体catalog删除目标根；没有跨表/共享路径误删 |
| T16 | 多分区成功/普通失败/UNKNOWN/INCOMPLETE及输出计数 | 普通失败隔离；UNKNOWN及维护违约停止；文件数不等于bucket数；partial成功仍刷新缓存 |
| T17 | 常规NONE streaming checkpoint/resume、FILE_MONITOR反例 | 归档COMPACT不制造业务增量；FILE_MONITOR不纳入支持承诺 |
| T18 | schema历史格式/压缩后缀、预算超限、后续写入/compaction | 新名字保持源格式；超限未复制；普通新写入仍热；记录再次变热的边界 |
| T19 | Iceberg镜像、自定义commit/tag回调、metastore映射、success-file及历史镜像选项 | 全部在创建pin/复制前被拒绝；不以Paimon发布确认宣称其他格式已同步；默认无副作用部署可通过 |

测试先写关键失败场景，注入故障的 FileIO/helper 应验证行为，避免“只调用了某方法”式测试。后续代码评审可在隔离副本中反转关键条件验证测试确实失败；本轮文档任务不做源码 mutation。读取等价用行结果与索引谓词，并行样例不能只比 COUNT 或 file exists。

设计反例复核：

| 被否定的假设 | 反例 | 设计应如何处理 |
|---|---|---|
| 同域rename安全 | 旧reader已持有hot路径，rename后打开失败 | 源保持不动 |
| 同长度副本可复用 | 截取/翻转内容但文件长度不变 | SHA-256不匹配；新命名，不复用 |
| latest changelog=null即无历史 | INPUT选项关掉后tag仍引用changelog | 检查受保护历史与配置 |
| noConflicts会冻结分区 | writer新增一个文件，原before仍存在 | 外部冻结 + 集合复查，不虚报整分区完成 |
| 提交异常等于失败 | 快照发布成功，响应/回调失败 | 查身份和引用，未知时保留 |
| 同名孤儿可清 | hot的basename仍在used集合 | cold新basename且登记标准根 |
| COMPACT对全部stream不可见 | file-monitor扫描全部文件变化 | 明确不支持该消费者模式 |
| pin能替代清理协调 | pin创建与expiration竞争，或冷对象尚未发布被olderThan扫描 | 维护窗口覆盖创建、复制和结果确认 |

### 14.1 五轴评审结论与残余门槛

| 维度 | 设计审查结论 | 尚需实现证据 |
|---|---|---|
| 正确性 | 在维护协议和白名单下，可利用不可变副本与原子文件引用替换保持当前/历史读取；原高风险路径已修正 | T1–T19，尤其sidecar实际过滤、发布不确定恢复与冷热GC；未运行测试，不作实现通过判定 |
| 可读性 | 原理、状态、方案、限制与源码索引分区说明；辅助方法明确标为拟新增 | 实际方法不得把FAILED与UNKNOWN合并，输出合同与测试一致 |
| 架构 | 核心归档独立于Spark薄入口；复用现有格式/提交/IO，不向共享cleaner或compactor散布策略 | 新增类依赖与公共API范围、注册和部分成功缓存刷新须代码审查 |
| 安全 | 目标独占/登记、URI边界和权限约束明确；不泄露凭据，不覆盖既有对象 | 恶意路径/别名/跨表根、权限故障和drop边界实测 |
| 性能 | 消除数据解码/编码/排序，不消除传输、摘要、manifest维护；顺序有界复制，计划受预算限制 | 大文件/多文件内存、总耗时和存储开销实测，默认预算依测量设置 |

设计层评审结论：**有条件可实施，可作为后续编码方案；不批准“任意表、任意并发、任意消费者的无 compaction 透明迁移”。** 条件为 §4/§5/§10；技术路线见 §13.1。若无法冻结目标分区、协调清理、提供冷热端统一读取或排除外部元数据副作用，应拒绝执行，而不是降级保证。不得把本 spec 标记为实现已验证或生产批准。原 Critical/Required 设计错误已修正。未来放行实现必须有 T1–T19 对应的有效运行证据、编译与风格通过、残余限制在文档可见。实现性验证若发现 sidecar/目录/consumer不符合假设，应修订范围或协议，不能弱化测试、仅长度校验或跳过门槛来获取通过。

此前的独立只读方案复审核查了提交/恢复/清理及实施验收合同，发现“文件身份冲突后完整重试”与维护违约停止规则矛盾，已按 §9 修正并加强 T8。该复审未发现其他需要修订的 Critical/Required 问题；这只表示该次设计范围内的审查结果，不替代新增测试方案、未来代码和运行测试的验证。

### 14.2 单元测试分层与公共夹具

本节是完整测试合同，部分场景现由ArchivePathTest/ArchiveFileCopierTest/PartitionArchiverTest和Java真实CALL测试覆盖，未取得证据的场景仍未完成。U/L/M是T1–T19的细化组，不新增业务范围；实际RED/GREEN、mutation和独立审查修正记录见tasks/evidence/AP-CALL.md。不能用原探针或少数happy-path结果替代全矩阵放行。

| 层级 | 计划测试载体 | 环境与定位 |
|---|---|---|
| U：纯逻辑单元测试 | core/PartitionArchiverTest 内的独立测试组 | JUnit 5/AssertJ，无文件IO/网络；metadata、URI、增量与状态合同 |
| L：本地文件系统测试 | core/ArchiveFileCopierTest、PartitionArchiverTest | JUnit 5、@TempDir、真实LocalFileIO；属于core测试套件，涉及真实文件/表交互者是组件测试而非纯单元测试；不需要Docker |
| M-core：MinIO模拟对象存储测试 | spark-ut/.../operation/PartitionArchiverS3ITCase | JUnit 5 + MinioTestContainer，直接调用core不启动Spark；真实S3协议/对象IO的集成测试，不是内存mock |
| M-Spark：MinIO CALL端到端 | spark-ut/.../spark/ArchivePartitionsS3ITCase | Spark local[2] + MinIO，验证CALL、executor/session、缓存和常规stream |

本地夹具用独立临时父目录内互不包含的tableRoot、coldRoot、neighborRoot；先持久登记coldRoot并配置strategy=none。三种支持表模式各建真实分区表：append含重复行，PK fixed含同key更新/删除，多bucket含至少两个有文件的bucket；写入/fixture建表可以使用已有writer，**归档Act阶段不可以**。源文件由真实表写入产生，不伪造一个带扩展名的字节文件来证明Paimon读取。

复制器的字节边界测试可单独使用任意payload，包括0字节及buffer B的B-1/B/B+1/2B+17长度；不能把0字节payload当作合法Paimon数据文件。采用固定seed生成内容，并确保equal-length corruption真的翻转至少一个byte。读结果比较行多重集和实际索引谓词，不依赖文件顺序、随机UUID值、仅COUNT或只verify内部方法调用。每个用例独立建表/根/pin并可靠关闭流、commit、reader、Spark及container资源。

### 14.3 本地文件系统测试用例

| 编号 | Arrange / Act | 必须断言 / 对应验收 |
|---|---|---|
| U01 | 用真实样例oldMeta构造after与before/after增量 | 仅三种物理字段变化，embeddedIndex/stats/level/序列等保留；skipped及空bucket无消息（T2/T7） |
| U02 | URI同根/相邻前缀/不同authority、越界extra名、重复类型化分区输入 | 规范化边界、精确键与去重顺序正确；非法输入无副作用（T7/T11） |
| U03 | 分别构造已确认成功/未发布失败/UNKNOWN/INCOMPLETE状态与多分区后续决策 | UNKNOWN与违约停止，普通失败可继续；cleanup_required不覆盖发布结果（T9/T16） |
| U04 | 当前选项/历史引用策略和预算边界输入 | 未知/无法读取不能当空；阈值以内/超限和外部副作用拒绝正确（T11/T18/T19）；真实历史读取还需L09 |
| L01 | 三种支持模式，单文件/多文件/多bucket，file热端→file冷端归档 | 一次纯COMPACT，无空APPEND；全部行及metadata等价，after指向cold，hot仍在（T1/T2/T4） |
| L02 | 复制器复制0字节与buffer边界payload、已存在dst | 全量长度/摘要正确，既有dst不改写/不复用，输出关闭后才验证（T2/T6） |
| L03 | 用latch暂停copy或发布，另一reader持有source snapshot/tag | 旧data/index持续可打开、新副本未发布不可见，发布后fresh reader读cold；每次等待有timeout（T3/T4） |
| L04 | 归档后重入、部分先归档、skipped引用缺失、未提交残留 | 全NOOP无新对象/snapshot/pin；部分只替换changed；缺失引用失败；新attempt不复用残留（T7/T10） |
| L05 | 真实embedded index与重建编号sidecar；缺失/多个/未知extra | 完整摘要、extraFiles新名字及aligned读取正确；谓词结果与基准相等；不支持布局提交前拒绝（T5） |
| L06 | 对指定data或index的第N次read/write/close/status定点报错；验证前翻转目标byte | 无发布，源可读，等长损坏可检测；流关闭；残留仅属于本attempt（T6） |
| L07 | snapshot已发布后响应丢失、fresh确认失败、pin写入/删除已生效后抛错；重建archiver模拟重入 | 状态分类准确，未知after/pin保留、后续停止，清理异常不覆盖成功；已发布重入NOOP（T9/T10） |
| L08 | barrier控制同源冲突/overwrite/compaction/独立append，以及其他分区发布竞争 | 失效before/维护违约停止不重规划；额外热文件INCOMPLETE；合法快照竞争只由内核重试（T8/T16） |
| L09 | 用真实snapshot/tag/schema/独立changelog保留旧特性，再关闭当前选项；破坏可读性 | DV/index/evolution/changelog/镜像等仍拒绝，fail closed；普通BINARY允许；拒绝无pin/cold增量（T11/T19） |
| L10 | source pin和user tag存在→解除→按测试retention执行expire | 受保护hot data/index不删；解除后可回收；live cold读不受损（T13） |
| L11 | 注册标准冷根内未发布残留、邻分区/邻表live文件；orphan与隔离drop | 仅适格孤儿回收；hot引用不按同名保护新冷残留；drop不越出本表独占根（T14/T15） |
| L12 | 历史文件format/压缩后缀变化、预算边界、大文件、归档后写入/compaction | 命名保持源格式；预算超限复制前失败；有界buffer；再次变热边界有实际证据（T18） |

### 14.4 MinIO模拟S3测试用例

采用已有MinioTestContainer的JUnit @RegisterExtension：镜像版本来自DockerImageVersions.MINIO，endpoint取动态映射端口，使用getS3ConfigOptions返回的临时凭据与path-style配置；不依赖用户已有MinIO/固定9000端口或真实bucket。每个测试用例使用独占UUID表根/prefix；S3→S3的authority用同一容器内两个测试bucket验证，第二bucket由测试client创建，不假设MinioTestContainer存在未提供的public getClient方法。catalog初始化前配置完整CatalogContext，禁用副作用并登记coldRoot；Spark测试使用未缓存该catalog的独立session/实例。

| 编号 | Arrange / Act | 必须断言 / 对应验收 |
|---|---|---|
| M01 | file热表→MinIO冷根，三种支持模式各一套parquet数据、多bucket | 原字节/metadata/行多重集一致，单COMPACT，新core reader读S3、历史读file（T1/T2/T4） |
| M02 | 实际可写读的parquet/orc/avro格式、embedded/sidecar和历史压缩后缀样例 | 保持真实format/后缀、data与index摘要；真实谓词正确；格式/索引不支持组合明确拒绝而非skip（T2/T5/T18） |
| M03 | 已外置源bucket-A→冷bucket-B，以及同bucket不同独占prefix/相邻前缀 | 按源实际URI读、目标authority/路径段判断正确，不能错把其他根当NOOP（T7/T12） |
| M04 | 成功发布后新core FileIO、新Spark session/executor及已缓存查询 | 不依赖归档driver热cache，新引用可读；部分成功仍刷新；生产多节点网络可达性不由local[2]证明（T4/T12/T16） |
| M05 | S3真实对象上定点copy/close/读回故障或验证前等长损坏（data/index分别测） | 拒绝发布、保留源，残留不复用/覆盖；校验不能使用ETag代替（T6） |
| M06 | 独立配置的错误凭据、不可达endpoint、resolving关闭、缺少所需loader夹具 | 发布前部署失败，错误不泄漏临时secret；loader负例无需修改全局classpath；真实后端与定点注入证据分别记录（T12） |
| M07 | 真实snapshot提交边界成功后故障、确认metadata不可读、销毁并重建归档实例 | ARCHIVED/UNKNOWN准确，after/pin保留、停止后续；重入NOOP；不把实例重建夸称真实JVM崩溃验证（T9/T10/T16） |
| M08 | copy暂停时旧snapshot/tag读热源；发布后expire且user tag保留/解除 | 历史可读；保护期内hot/data/index不删；解除保护后hot可回收，cold仍live（T3/T13） |
| M09 | 冷端失败data/index残留及邻表/分区对象，orphan与独占根drop | 只回收适格孤儿/本表根，成功cold与邻表对象保留，清理端具备正确多路径IO（T14/T15） |
| M10 | Spark多分区部分失败/UNKNOWN/INCOMPLETE、NONE stream checkpoint/resume | 计数/停止/缓存语义准确，归档不产生业务重复增量；FILE_MONITOR反例不纳入支持（T16/T17） |

M01–M03/M05–M09优先在M-core直接验证真实对象和核心语义；M04/M10及CALL层断言在M-Spark验证，不重复把每个core故障组合再经过Spark。先跑三模式parquet基线，再为其他format/sidecar增加有针对性的组合，记录参数展开后的实际用例，不声称上述表格行数等于测试数。T10的跨JVM身份/重启恢复必须另有受控forked进程场景：在复制未提交和已发布未返回两个持久边界中断本次测试拥有的子进程，另一JVM重新确认现场，记录终止/恢复证据；可先在本地L07验证，再在M07验证对象存储。实例重建只能证明重入，不能替代该证据。

### 14.5 故障注入、时间控制和运行门槛

优先真实FileIO + 测试内装饰器，按具体URI/阶段/调用次数触发一次错误或byte翻转，并可记录流关闭状态。TraceableFileIO可辅助本地资源检查；现有FailingFileIO主要基于概率且绑定本地fail://路径，不能原样当作确定性S3故障器或唯一失败证据。snapshot/pin“已生效后响应失败”必须先执行真实持久动作再抛错，不能mock一个不存在的成功快照。若已有IO边界无法稳定注入，应先评审最小包内测试边界，不增加SQL故障开关/forceCopy或修改共享提交算法来服务测试。

并发用CountDownLatch/barrier与有界await，禁止靠Thread.sleep碰时序；所有线程在finally释放/停止并join。orphan/expire用显式retention/截止时间及实际文件mtime构造资格；本地可设置测试文件mtime，MinIO按真实object状态构造cutoff，不伪造lastModified可修改API、不等一天、不改变生产默认。测试强制清理只在结果确认与维护协调完成后的隔离资源执行。

本地U/L套件必须独立于Docker/网络运行；MinIO core与Spark IT各有focused命令，命令见plan §5.1。无Docker或拉取镜像失败应记录M层“未执行/环境阻塞”，不得改为mock/skip并宣称MinIO通过。放行必须具备所有必需U/L/M场景实际方法名、suite/参数、报告路径、非零用例及失败/跳过数，不能用Maven exit=0或未运行测试的表格替代。上面的U/L/M表仍是完整实现的验收合同；已执行的局部探针结果单独记录如下。

### 14.6 已执行的本地/MinIO混合存储探针

用户于2026-10-11要求提前验证混合存储的snapshot与查询。因此在Spark UT模块增加独立test-only探针，先验证数据文件原字节复制、externalPath引用替换和现有读取链路的兼容性；不绕过生产实现的CP-A–G门槛。

| 实际环境 | 三种表模式 × 三种格式 | 结果 |
|---|---|---|
| 本地表根/热文件 → 独立本地冷目录，无Docker | append fixed(2)、append unaware(-1)、PK fixed(2) × Parquet/ORC/Avro | 9例通过，失败/错误/跳过均0 |
| 本地表根/热文件 + MinIO S3冷对象 | 同上，真实容器/对象IO，非mock | 9例通过，失败/错误/跳过均0；独立integration-tests入口亦通过 |

实际运行使用JDK11.0.32.1、Spark3.5.6 local[2]、Scala2.12；不能从根POM的test.spark.version=3.3.0推断本次UT运行的是Spark3.3。append的snapshot从2到3，PK从3到4；仅一次COMPACT，delta_record_count=0，源snapshot保持不变。冷分区所有bucket的DELETE/ADD数量配对，发布后metadata除物理名字/externalPath外完整相等，热分区引用保持不变，目标文件长度和SHA-256与源一致。迁移过程中没有调用data compactor。

查询检查完整行多重集、冷/热分区过滤、新session、旧snapshot/tag；追加新行后同一分区同时含本地与冷端文件。PK另验证热端更新冷端已有主键、热端删除冷端主键，最新结果正确，历史版本不变。每例精确删除一个测试冷对象后，最新查询必须以该文件的FileNotFoundException失败，旧tag仍可读；finally恢复该对象并再次核对摘要和完整查询。证明没有默默回退到热副本。

完整命令、实际容器版本、审查和mutation结果见[tasks/evidence/AP-PROBE-mixed-storage.md](tasks/evidence/AP-PROBE-mixed-storage.md)。该证据只覆盖受控静止表的data-only方案前提：没有实现/验证CALL、sidecar、并发维护、UNKNOWN恢复、streaming/cache刷新、GC/drop、跨主机共享本地路径；AP-13等正式验收不得据此勾选完成。探针复制器使用既有FileIO.copyFile后分别读源/目标算摘要，不代替AP-03的一次源读取生产复制器。

## 15. 源码证据索引（本次复核的 release-1.3）

路径均相对于仓库根。行号是本次 HEAD 的定位辅助，实施前需核对方法；没有把 master 的行号复用到本分支。

| 路径:行号 / 方法 | 本 spec 使用的事实 |
|---|---|
| pom.xml:35、105、393、419–425 | 1.3-SNAPSHOT；Java8默认；Spark3/Spark4 profile；Spark4目标17 |
| paimon-api/src/main/java/org/apache/paimon/CoreOptions.java:185–211、2030 | external paths配置、强制重写、branch默认main |
| paimon-api/src/main/java/org/apache/paimon/options/CatalogOptions.java:163 | resolving-file-io.enabled默认false |
| paimon-common/src/main/java/org/apache/paimon/fs/FileIO.java:227、425 | rename可抛IOException；get在启用resolving时返回多scheme IO |
| paimon-common/src/main/java/org/apache/paimon/fs/ResolvingFileIO.java:57–142 | 保留完整CatalogContext并按scheme/authority分派读取、状态、删除等 |
| paimon-core/src/main/java/org/apache/paimon/AbstractFileStore.java:138、264 | none关闭新写入external provider；newCommit创建原有内核提交 |
| paimon-api/src/main/java/org/apache/paimon/CoreOptions.java:1391、1408、1490、1503、1526、2872 | 自定义commit/tag callbacks，metastore映射和success-file选项 |
| paimon-core/src/main/java/org/apache/paimon/iceberg/IcebergOptions.java:39 | metadata.iceberg.storage默认DISABLED；镜像是提交后的额外动作 |
| paimon-core/src/main/java/org/apache/paimon/AbstractFileStore.java:354、450 | 原始commit/tag仍加载自定义及内建回调，不具备自动多格式原子同步 |
| paimon-core/src/main/java/org/apache/paimon/operation/FileStoreCommitImpl.java:1207 | 成功发布Paimon快照后执行callback，回调异常不等于未发布 |
| paimon-core/src/main/java/org/apache/paimon/io/DataFileMeta.java:82、316 | externalPath字段；collectFiles使用extraFiles及aligned路径 |
| paimon-core/src/main/java/org/apache/paimon/io/PojoDataFileMeta.java:281、382、407 | rename/copy(extra)/newExternalPath为metadata克隆 |
| paimon-core/src/main/java/org/apache/paimon/io/DataFilePathFactory.java:109、124–169 | 保留扩展名命名、toPath/toAlignedPath、索引名字推导不是所有extra来源 |
| paimon-core/src/main/java/org/apache/paimon/io/FileIndexEvaluator.java:111–145 | embeddedIndex优先；extraFiles内.index按aligned位置读取；多个index报错 |
| paimon-core/src/main/java/org/apache/paimon/manifest/FileEntry.java:73–134、173–213 | 身份包含externalPath/extraFiles等；DELETE/ADD合并 |
| paimon-core/src/main/java/org/apache/paimon/manifest/PojoManifestEntry.java:105 | manifest identifier构造 |
| paimon-core/src/main/java/org/apache/paimon/operation/FileStoreScan.java:72–89、125–171 | 固定快照、ALL扫描、分区枚举、分bucket分组 |
| paimon-core/src/main/java/org/apache/paimon/utils/FileStorePathFactory.java:227 | 实际相对bucket路径；path-directory会改变深度 |
| paimon-core/src/main/java/org/apache/paimon/table/sink/CommitMessageImpl.java:51 | CommitMessage公开构造 |
| paimon-core/src/main/java/org/apache/paimon/io/CompactIncrement.java:38–54 | before/after/changelog与独立index增量 |
| paimon-core/src/main/java/org/apache/paimon/operation/FileStoreCommit.java:43–50、89 | ignoreEmptyCommit、commit、abort语义与close生命周期 |
| paimon-core/src/main/java/org/apache/paimon/operation/FileStoreCommitImpl.java:237、291–415、752 | 默认忽略空提交；compact增量收集为DELETE/ADD并推导COMPACT |
| paimon-core/src/main/java/org/apache/paimon/operation/FileStoreCommitImpl.java:960–1044、1054–1105、1400–1515 | user/identifier/kind成功识别、OCC、计数/watermark/logoffset继承、LSM约束 |
| paimon-core/src/main/java/org/apache/paimon/operation/FileDeletionBase.java:195–243、340–416 | 普通过期以实际Path处理；tag保护集合使用basename |
| paimon-core/src/main/java/org/apache/paimon/operation/LocalOrphanFilesClean.java:166–234 | used和候选Map按basename；extraFiles也纳入保护 |
| paimon-core/src/main/java/org/apache/paimon/operation/OrphanFilesClean.java:320–385 | 登记根的普通分区层级与bucket遍历 |
| paimon-core/src/main/java/org/apache/paimon/catalog/AbstractCatalog.java:331–375 | DROP收集所有历史schema/branch中的外部根并交由catalog实现 |
| paimon-core/src/main/java/org/apache/paimon/table/ExpireSnapshotsImpl.java:120–195 | consumer/limit限制与tag保护；snapshot过期后的实际删除 |
| paimon-core/src/main/java/org/apache/paimon/table/AbstractFileStoreTable.java:574–644 | source tag创建/删除API |
| paimon-core/src/main/java/org/apache/paimon/utils/TagManager.java:128–174 | null TTL不会生成带TTL的tag元数据 |
| paimon-core/src/main/java/org/apache/paimon/utils/ChangelogManager.java:140 | 独立保留changelog枚举 |
| paimon-core/src/main/java/org/apache/paimon/deletionvectors/DeletionVectorsIndexFile.java:74 | DV关联data basename，不能随意换名而忽略独立映射 |
| paimon-core/src/main/java/org/apache/paimon/KeyValueFileStore.java:100 | PK -1动态模式取决于cross-partition，不能凭参数泛化 |
| paimon-core/src/main/java/org/apache/paimon/AppendOnlyFileStore.java:73 | append fixed/unaware实际模式 |
| paimon-core/src/main/java/org/apache/paimon/table/source/DataTableStreamScan.java:266 | FILE_MONITOR等follow-up扫描器选择 |
| paimon-core/src/main/java/org/apache/paimon/table/source/snapshot/DeltaFollowUpScanner.java:34 | 常规NONE流模式只跟随APPEND |
| paimon-core/src/main/java/org/apache/paimon/table/source/snapshot/AllDeltaFollowUpScanner.java:28 | 文件监控扫描所有delta |
| paimon-core/src/main/java/org/apache/paimon/table/source/snapshot/CompactedStartingScanner.java:66 | compacted-full可能选择归档产生的COMPACT快照 |
| paimon-core/src/test/java/org/apache/paimon/table/source/snapshot/InputChangelogFollowUpScannerTest.java:82 | INPUT场景的COMPACT处理测试线索 |
| paimon-core/src/test/java/org/apache/paimon/table/source/snapshot/CompactionChangelogFollowUpScannerTest.java:82 | FULL_COMPACTION的不同消费语义测试线索 |
| paimon-spark/paimon-spark-common/src/main/java/org/apache/paimon/spark/procedure/BaseProcedure.java:85–136 | modifyPaimonTable和Spark缓存刷新模板 |
| paimon-spark/paimon-spark-common/src/main/java/org/apache/paimon/spark/SparkCatalog.java:134 | catalog创建时注入options和Spark Hadoop配置 |
| paimon-spark/paimon-spark-common/src/main/java/org/apache/paimon/spark/SparkProcedures.java:99 | procedure注册模板 |
| paimon-filesystems/paimon-s3/src/test/java/org/apache/paimon/s3/MinioTestContainer.java:111–128 | S3配置与bucket URI测试基建 |
| paimon-spark/paimon-spark-ut/src/test/java/org/apache/paimon/spark/SparkS3ITCase.java:49–85 | JUnit MinIO/Spark基建；混合scheme仍需新增归档测试 |
| paimon-spark/pom.xml:278–290；pom.xml:633–692 | S3 test-jar已存在；Surefire test与integration-test执行 |
| paimon-core/src/test/java/org/apache/paimon/operation/FileStoreCommitTest.java:103；paimon-core/src/test/java/org/apache/paimon/operation/LocalOrphanFilesCleanTest.java:92 | 本地真实文件/表fixture已有JUnit5临时目录惯例 |
| paimon-core/src/test/java/org/apache/paimon/utils/FailingFileIO.java:114 | read/write故障基于概率，不能直接作为确定性S3故障器 |

官方辅助说明：[1.3 配置](https://paimon.apache.org/docs/1.3/maintenance/configurations/)说明 external-paths 的三种策略与 resolving 开关；[1.3 快照管理](https://paimon.apache.org/docs/1.3/maintenance/manage-snapshots/)说明旧文件延迟删除和过短保留期对读者的影响。精确实现行为以上述固定 checkout 源码为准。

## 16. 后续扩展（不在 Phase 1）

DV/动态bucket/多branch/row-tracking/data-evolution/changelog 的支持，需要明确其索引和消费者不变量后再扩展，不能仅删除拒绝检查。允许同basename迁移则还需重新设计按物理URI处理的清理/保护机制。

自动按年龄归档、partition.expire.action接入、长期冷热写入/compaction策略，以及高并发的分区封存协议，均作为后续独立设计；不承诺 Phase 1 核心零改动即可实现。外部调度只有落实本 spec 维护协议后才能调用，普通cron定时本身不能提供并发安全。

现有强制重写compaction可作为部署已经验证过的另一条迁移路线，但会改变数据文件并消耗编解码/合并资源，不列为本方案依赖或必经步骤。
