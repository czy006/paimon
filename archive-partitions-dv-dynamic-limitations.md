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

# archive_partitions：DV 与动态分桶限制及扩展可行性

> 日期：2026-10-11。开发基线：release-1.3.2；分支：codex/archive-partitions-132；源码核对点：68a99dc58。
> 状态：只读可行性审查与限制记录，不是已实施方案或测试通过证据。本次仅修改 Markdown，不改变当前 CALL 白名单，不新增参数，不解除任何安全检查。

## 1. 当前限制与结论

当前 CALL 支持普通分区 FileStoreTable：无主键 append 的 HASH_FIXED/BUCKET_UNAWARE，以及 PK 的 HASH_FIXED（deduplicate、partial-update、aggregation）。必须使用文件系统管理的 snapshot，满足历史、路径登记、多存储 FileIO 和维护冻结等门槛。完整使用限制见 [用户指南第 6 节](archive-partitions-user-guide.md#6-使用限制与不支持项)。

| 类型 | 当前 CALL | 扩展判断 | 主要前提 |
|---|---|---|---|
| 固定分桶 PK + DV | 拒绝 | 源码级可行，尚未验证 | 同步变更数据文件与 DV 映射，原子发布索引和数据引用 |
| HASH_DYNAMIC，不启用 DV | 拒绝 | 优先扩展，尚未验证 | 保持原 partition/bucket 与 HASH 索引，禁止归档期间重新分桶 |
| HASH_DYNAMIC + DV | 拒绝 | 源码级可行，复杂度更高 | 同时满足 HASH 索引和 DV 的一致性协议 |
| KEY_DYNAMIC，跨分区 upsert | 拒绝 | 原理可行，需单独验收 | 全表维护冻结、混合存储索引初始化及跨分区更新正确性 |
| POSTPONE_MODE，bucket=-2 | 拒绝 | 不作为无数据 compaction 的首批支持 | 未完成分桶的记录需要真实 compaction 才能可读 |
| 无主键 append + DV | 拒绝 | 不从 PK-DV 的结论直接放行 | bucketed/unaware 索引合并、Spark 行位置操作需专项设计与验证 |

PK 的 bucket=-1 根据主键是否包含全部分区字段，分别使用 HASH_DYNAMIC 或 KEY_DYNAMIC；append 的 bucket=-1 是 BUCKET_UNAWARE，不能混同。bucket=-2 是 POSTPONE_MODE，也不是 HASH_DYNAMIC 的别名。

ArchiveSafety 当前不仅拒绝动态 bucket 和 DV 选项，还拒绝保留 snapshot/tag 中的独立 indexManifest。关闭当前 DV 配置不能消除受保护历史的依赖；不得通过删除业务历史或绕过检查来强行调用。

可行性结论是：可以保留“原字节复制 + 元数据引用替换”，不要求归档时合并、排序或重新编码数据文件；但必须扩展索引处理、提交确认、历史保护和生命周期验证。不能只删除拒绝条件。

## 2. DV 的实现候选方案

### 2.1 为什么当前数据文件复制不足以支持 DV

DV 描述一个数据文件内已删除记录的行位置。IndexFileMeta.dvRanges 和 DeletionVectorMeta.dataFileName 使用数据文件名关联位图。当前归档为数据文件生成新文件名，只修改 DataFileMeta 而保留旧 DV 映射，会使新文件失去应有的删除过滤，存在已删除记录复活风险。

DeletionFileWriter.write 将位图写入索引文件，将文件名、offset、length 和 cardinality 放入元数据。保持数据字节及行顺序不变时，原位图仍适用于新副本；因此无需为归档重新计算位图或重写数据内容。此判断来自源码结构，尚未形成实测保证。

### 2.2 建议的安全路径

1. 冻结写入、compaction 和清理，固定源 snapshot；保护源数据、索引及相关 snapshot/tag/schema。
2. 复制数据文件和附属文件，关闭流后验证长度与 SHA-256；建立旧数据文件名到新数据文件名的映射。
3. 为受影响 bucket 构造完整的新 DV 索引元数据，只替换已迁移文件的名字，保留 offset、length、cardinality 与 LinkedHashMap 顺序。
4. 优先将 DV 物理文件字节复制到独立新名字并验证，避免新旧 snapshot 共享物理索引的清理耦合。首批可仍放在表内 index 目录；若放冷端，必须额外验证 IndexFileMeta.externalPath 的读取、登记、GC/drop 和孤儿清理。
5. 用 CompactIncrement 的索引增量，在同一个 COMPACT snapshot 中发布数据引用替换与 DV 索引引用替换。不得先发布数据、再单独补写索引。
6. 发布后同时确认数据文件集合、DV 映射及索引身份；旧 snapshot/tag 仍指向旧文件和旧映射。提交结果不明时保留新对象和源 pin，不立即 abort 或删除。

PK 的 DV 索引按 partition/bucket/indexType 合并和替换，不是单个数据文件的局部追加。必须保留该 bucket 中未移动、已在目标根而跳过的文件以及其 DV；只提交迁移文件的位图会丢失其他删除信息。

直接复用旧 DV 物理文件、仅改 manifest 映射也是候选优化，但共享文件引用、回滚和清理身份尚未验证，不作为首批默认路径。重新序列化小型 DV 索引可以作为替代方案；它不等于数据文件 compaction。

COMPACT 是引用替换的提交类型，不代表执行数据 compactor。归档不能改变文件 level 或现有可见性：DV 模式的 level-0 数据有其原生 compaction 可见性规则，不承诺归档能替代正常写入路径的 compaction。

## 3. 动态分桶的实现边界

### 3.1 HASH_DYNAMIC

HASH 索引存储主键 hash 与 bucket 的对应关系，不依赖数据文件名。归档不改变记录、主键、分区或 bucket，因此可先将 HASH 索引留在原位置，只移动数据文件及其附属文件。

- 保持各 ManifestEntry 的 bucket、totalBuckets 及原 DataFileMeta 逻辑字段，不调用重新分桶、rescale 或动态分配器。
- 将“拒绝所有 indexManifest”改为经过验证的类型白名单；只接受明确支持的 HASH/DV 类型，不忽略未知索引。
- 没有索引增量时，现有 IndexManifestFile.writeIndexFiles 会继承旧 indexManifest；扩展后仍须核对索引内容、归档提交以及后续写入恢复，而不是假设继承就已正确。
- 后续更新已有主键必须回到原 bucket；新增主键、扩桶和写入端重启必须在混合存储下验证。
- 维持 Paimon 1.3 动态分桶的单写作业约束；归档不是增加一个可并发写入/compaction 的例外。

### 3.2 KEY_DYNAMIC

该模式维护 key 到 partition/bucket 的索引；IndexBootstrap 通过表 reader 从已有记录初始化索引，而不是按数据文件名建立映射。保持 partition/bucket 不变具有扩展基础，无需因物理路径变化就重新设计主键映射。

但不能仅冻结被归档分区：其他分区收到同一主键时，可能删除旧分区记录或继续更新旧分区。首批必须全表停止写入并暂停维护，验证写入端重新初始化、冷热读取、跨分区移动和 index TTL。冷数据会增加索引初始化的远程 IO 成本；保留 creationTime，不能通过归档刷新时间来延长索引 TTL，也不能把该 TTL 当作归档安全保证。

### 3.3 POSTPONE_MODE 与 append-DV

bucket=-2 的记录先写入 bucket-postpone，未实际分桶时不可读。字节复制不会完成分桶或使这些记录可见。后续如只支持已经完成分桶且 postpone 缓冲为空的分区，应另加准入证明与测试；不能声称任意 postpone 分区可不经 compaction 直接归档。

无主键 DV 还存在 BUCKET_UNAWARE 的按索引文件名全局合并路径，与 PK 按 bucket 替换不同。PK-DV 测试通过不能代替 append-DV 的 SQL UPDATE/DELETE、行位置、rolling DV 文件和清理验证。

## 4. 开放支持前必须解决的正确性问题

| 风险 | 必须补充的约束或验证 |
|---|---|
| DV-only 更新不生成新数据文件 | 默认 partition_idle_time=1d 当前检查 PartitionEntry.lastFileCreationTime，不代表最后逻辑更新；扩展需纳入相关 DV/索引活动，或定义更保守且可证明的提交时间门槛 |
| 仅数据文件 OCC 无法覆盖索引变化 | FileStoreCommitImpl 对 PK COMPACT 跳过一类 DV 冲突检查；必须增加索引级基线和确认，保持维护冻结 |
| 检查后到发布前仍有竞争窗口 | 外部冻结必须在整个操作期间有效；若要求在线并发归档，需提交锁内的校验/更强 OCC 协议，不能靠提交前后各检查一次宣称原子防并发 |
| 提交已发布，但索引内容不完整 | 确认必须覆盖完整数据集合和相关 bucket 索引，不能只观察 COMPACT snapshot 或 copied 文件存在 |
| 旧 snapshot/tag 使用旧 DV/HASH | 保持旧 manifest 和物理索引，pin/保护检查与确认必须扩展到索引引用；不得原地修改索引 |
| GC/drop/orphan cleaner 误删索引 | 验证索引身份、目录、externalPath 及所有受保护引用；UNKNOWN 保留对象，延迟清理，不能只复用数据文件生命周期结论 |
| 物理行数被当成查询结果 | DV/PK 的文件 rowCount、snapshot totalRecordCount 与逻辑 COUNT(*) 不等价；比较完整查询行多重集及删除语义 |

活跃时间检查只是准入过滤，不能替代写入冻结；显式 0s 也不解除维护窗口要求。未实现并验证索引活跃度之前，不将现有 1d 的 data-only 检查宣传成 DV 安全保证。

当前其他限制继续有效：Spark-only、普通分区表、main 且无其他 branch、无独立 changelog、row tracking/evolution、镜像/回调副作用及外部版本管理；冷端在线且所有 reader/维护端可访问。此文不扩展 catalog、表格式或消费者支持范围。

## 5. 待实现与验收顺序

建议顺序：HASH_DYNAMIC 无 DV → 固定分桶 PK-DV → HASH_DYNAMIC + DV → KEY_DYNAMIC；append-DV 和 POSTPONE_MODE 另行评估。首批 DV 优先限定 deduplicate，其他 merge engine 独立验收后再开放。不是将这些任务标记为已完成。

每阶段都需 JDK 11 下的 core 单元测试与真实 Spark CALL 测试，并分别验证本地热端 + 独立本地冷目录、本地热端 + 隔离 MinIO 冷端。不连接业务 bucket，不以容器启动、编译成功或已有 data-only 测试代替新增行为证据。

| 测试组 | 最低验证内容 |
|---|---|
| DV 复制及映射 | 32/64 位 bitmap、多个 bucket、多个数据文件、带/不带 DV、部分文件已在冷端；位图范围和逻辑删除均保持 |
| 查询与历史 | 含已删除/被更新旧版本的记录归档后不复活；latest、旧 snapshot/tag、过滤和完整行多重集正确；保留原 level-0 可见性 |
| 后续写入 | 归档后 UPDATE/DELETE/MERGE；新热文件与旧冷文件混合，索引更新及旧 tag 正确 |
| 动态 bucket | 更新旧主键、插入新主键并触发扩桶、不同 bucket 分布；HASH 索引恢复与写入端重启 |
| KEY_DYNAMIC | key 跨冷热分区移动；启动时 bootstrap 冷数据；deduplicate 与其他拟支持引擎的差异、TTL 边界 |
| 活跃门槛 | 固定 Clock 验证 1d 默认、阈值、0s、回灌；数据不变但 DV/索引更新时不得误判冷分区 |
| 竞争与失败 | 复制期间、提交前、发布后发生索引-only 变化；中断/重试/UNKNOWN/INCOMPLETE、pin 丢失；无悬空引用或误删 |
| 生命周期 | snapshot/tag 保留与过期、索引共享/独立副本、cold 对象与索引的 GC/drop/orphan 清理；缺失对象应明确报错，不偷偷回退热副本 |

已有 221 例通过记录仅证明当前 data-only 白名单实现，不证明本文件中的扩展支持。新增验收完成后应单独记录命令、版本、用例数、失败/跳过数和产物身份。

## 6. 源码与官方资料依据

下面路径相对本仓库；源码事实以本文件标注的核对点为准。官方文档解释机制，不代表官方发行版已包含此开发分支的 archive_partitions。

- [ArchiveSafety](paimon-core/src/main/java/org/apache/paimon/operation/ArchiveSafety.java)：validate、validateOptions、inspectSnapshot 的当前拒绝条件。
- [PartitionArchiver](paimon-core/src/main/java/org/apache/paimon/operation/PartitionArchiver.java)：当前文件创建时间过滤及发布确认，不含 DV/HASH 扩展协议。
- [DeletionFileWriter](paimon-core/src/main/java/org/apache/paimon/deletionvectors/DeletionFileWriter.java)、[IndexFileMeta](paimon-core/src/main/java/org/apache/paimon/index/IndexFileMeta.java)、[DeletionVectorMeta](paimon-core/src/main/java/org/apache/paimon/index/DeletionVectorMeta.java)：位图与文件名元数据的对应关系。
- [CompactIncrement](paimon-core/src/main/java/org/apache/paimon/io/CompactIncrement.java)、[FileStoreCommitImpl](paimon-core/src/main/java/org/apache/paimon/operation/FileStoreCommitImpl.java)：索引增量、同次 snapshot 发布以及 checkForDeletionVector 的冲突边界。
- [IndexManifestFile](paimon-core/src/main/java/org/apache/paimon/manifest/IndexManifestFile.java)、[IndexManifestFileHandler](paimon-core/src/main/java/org/apache/paimon/manifest/IndexManifestFileHandler.java)：索引继承、按 bucket 或按文件名合并。
- [HashIndexFile](paimon-core/src/main/java/org/apache/paimon/index/HashIndexFile.java)、[KeyValueFileStore](paimon-core/src/main/java/org/apache/paimon/KeyValueFileStore.java)：HASH 内容、bucket 模式及索引 maintainer。
- [IndexBootstrap](paimon-core/src/main/java/org/apache/paimon/crosspartition/IndexBootstrap.java)、[GlobalIndexAssigner](paimon-core/src/main/java/org/apache/paimon/crosspartition/GlobalIndexAssigner.java)：跨分区索引初始化和 key 映射。
- [官方 Paimon 1.3 分桶机制](https://paimon.apache.org/docs/1.3/primary-key-table/data-distribution/#dynamic-bucket)：动态分桶、单写作业、postpone 及跨分区 upsert。
- [官方 Paimon 1.3 表索引格式](https://paimon.apache.org/docs/1.3/concepts/spec/tableindex/#deletion-vectors)：HASH 与 DV 的存储结构。
- [官方 Paimon 1.3 Merge On Write](https://paimon.apache.org/docs/1.3/primary-key-table/table-mode/#merge-on-write)：DV 删除过滤与 level-0 的可见性。

相关当前实现文档：[Phase 1 spec](archive-partitions-phase1-spec.md)、[用户指南](archive-partitions-user-guide.md)、[已实现功能审查与测试证据](tasks/evidence/AP-REVIEW.md)。
