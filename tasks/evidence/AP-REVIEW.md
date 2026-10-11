<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements. See the NOTICE file
distributed with this work for additional information
regarding copyright ownership. The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License. You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# AP-REVIEW: 本地提交、实现复核与分区活跃检查

日期：2026-10-11。分支codex/archive-partitions-132，基线release-1.3.2/c05f7d1。
Worktree：/Users/conradjam/.codex/worktrees/archive-partitions-132/paimon。
按用户要求保存本地提交、修正实现问题，并新增类似compact的partition_idle_time，默认1d。

## 支持的表与协议

| 类型 | 分桶 | PK merge engine |
|---|---|---|
| 无主键append | HASH_FIXED，bucket正整数 | 不适用 |
| 无主键append | BUCKET_UNAWARE，bucket=-1 | 不适用 |
| 主键PK | HASH_FIXED，bucket正整数 | deduplicate、partial-update、aggregation |

只接受普通分区FileStoreTable、main且无其他branch，完整分区键；不接受first-row、HASH_DYNAMIC、KEY_DYNAMIC、POSTPONE_MODE/非分区/format/system/特殊链式表。拒绝DV、独立index/changelog、row tracking/evolution、镜像/回调副作用、FILE_MONITOR和非标准布局；相关历史也需满足门槛。

snapshot必须由文件系统协议管理；catalog或表环境supportsVersionManagement=true时拒绝（例如RESTCatalog）。原实现枚举文件系统历史，不能据此确认外部catalog发布，这次修正将边界变成硬拒绝。FileSystemCatalog+本地/MinIO有实际证据，其他同协议catalog仍需部署验收。目录须独占/持久登记，策略none，所有reader/维护端具备多存储IO能力。

## 发现、复现与修正

| 级别 | 实现问题 | 修正与测试 |
|---|---|---|
| Required | 源pin删除/改向/加TTL或保护历史被移除，仍可能ARCHIVED | ArchiveProtection捕获完整snapshot/tag和本操作精确无TTL pin；复制后、发布后及清理前重检。默认保留保护并停止 |
| Required | 捕获基线期间pin消失，基线不含pin却被接受 | 在已捕获tag映射内验证pin完整snapshot/无TTL属性，并在unchanged中持续验证 |
| Required | release验证抛错后pin不存在，被当作自己已删除成功 | 验证与删除响应分离；deletionAttempted只在实际deleteTag前置true，未尝试删除不可确认清理成功 |
| Required | 内核明确发布前文件冲突报UNKNOWN | 原生createConflictException来源+固定冲突前缀且无确认发布时，FAILED/MAINTENANCE_VIOLATION；普通IO错误继续UNKNOWN |
| Required | 部分已在冷根的skipped文件被改写，掩盖已成功复制发布 | 先以commit身份和本次copied after引用确认发布，再将skipped/live集合变化分类INCOMPLETE，保留committed_snapshot |
| Required | 目录可见snapshot被当作外部catalog权威历史 | 对外部版本管理硬拒绝，不假设REST等与filesystem确认相同 |
| Required | first-row未经lookup/level-0专项验证却进入PK白名单 | 明确限制为三种merge engine，first-row拒绝；补充非分区/两种动态bucket拒绝测试 |

所有修正保留源文件，不在提交结果不确定时删除cold对象，不删用户tag。创建/捕获源保护未确认时FAILED/SOURCE_PROTECTION_UNCONFIRMED并停止；发布已确认而保护变化/不可验证时INCOMPLETE，维持已知快照身份。

独立新上下文审查先报告三项Required，复审发现捕获/release两个更细的竞态；均补齐确定性反例并修正。最终对源保护、结果分类和活跃检查的限定复审没有剩余Critical/Required。最终integration报告由主执行者另核实。

## 新增活跃检查

公开参数三个必填STRING，加可选STRING partition_idle_time，默认1d。省略/NULL/空白使用1d；例如7d、12h、30m，负值/溢出/小于1ms的正值拒绝；0s显式关闭过滤。

复用release-1.3.2 CompactProcedure的TimeUtils和PartitionEntry.lastFileCreationTime。用同一source snapshot及完整分区过滤，汇总所有bucket的最后文件创建时间；lastActivity<=clock.now-minus-duration才允许移动，恰好阈值也满足。活动分区返回NOOP/PARTITION_ACTIVE，无对象/pin/snapshot。

采用manifest时间，不看分区日期或对象mtime；旧日期分区刚回灌仍活跃。数据和元数据引用复制保留creationTime，归档不会刷新活动时间；后续新写入会刷新。此准入检查不代替维护窗口，仍需冻结写入/清理。

新增core固定Clock案例不睡眠一天：默认拒绝新数据、阈值前1ms跳过、恰好阈值移动、三表模式/多bucket、时间不重置、后续写入重新活跃、非法时长。旧立即归档用例显式0s。

真实SQL在本地和MinIO验证：默认1d跳过刚写入的20200101分区；0s可以执行归档；1d/12h/15m正常过滤；非法/负时长未复制。另新增partial-update/aggregation在冷热同主键上的后续更新和旧tag读取，Parquet场景实际通过。

## RED、mutation与最终验证

全部JDK11，Maven3.9.16，Spark3.5.6/Scala2.12、spark3 profile。与原AP-CALL报告区分，不重复累加旧用例。

| 层级 | 最终用例数 | 结果 |
|---|---:|---|
| PartitionArchiverTest | 38 | 0失败/错误/跳过 |
| ArchivePathTest / ArchiveFileCopierTest | 38 / 7 | 0失败/错误/跳过 |
| 既有提交/路径/snapshot/tag/orphan core回归 | 71 | 0失败/错误/跳过 |
| core合计 | 154 | BUILD SUCCESS |
| 本地ArchivePartitionsProcedureTest | 28 | BUILD SUCCESS |
| 真实MinIO ArchivePartitionsS3ITCase | 28 | BUILD SUCCESS |
| 既有Scala ExpirePartitionsProcedureTest | 11 | All tests passed |
| 合计 | 221 | 全通过 |

日志均在/tmp/，保留失败证据，不把编译或容器启动失败算行为RED：

- paimon-archive-review-authority-red.log：外部版本管理反例1例断言失败。
- paimon-archive-review-boundaries-red.log：pin/历史、skipped与冲突边界23例中8失败、0错误。
- paimon-archive-review-table-modes-red.log：4表模式中first-row1例断言失败。
- paimon-archive-review-protection-races-red2.log：精确捕获/release竞态2例均断言失败；初始测试的read序号曾落在confirmation，修正为release后才记录此RED。
- paimon-archive-review-protection-mutation.log：AND→OR放宽tag保护，用户tag变化但owned pin完整的1例断言失败；已恢复AND。
- paimon-archive-idle-default-red.log：原代码默认立即移动，默认1d反例1例断言失败。
- paimon-archive-review-idle-final-core.log：最终core154成功，约71秒。
- paimon-archive-review-idle-build.log：21模块连接器构建成功，约72秒。
- paimon-archive-review-idle-local.log：真实CALL28+Scala11成功，43.875秒。
- paimon-archive-review-idle-minio.log：真实MinIO CALL28成功。

构建前clean指定4个shaded模块的target，避免旧类遮蔽；依赖构建skipTests不算测试证据。最终运行命令与[用户指南](../../archive-partitions-user-guide.md)一致，local用-Dtest=ArchivePartitionsProcedureTest，MinIO用-Dtest=ArchivePartitionsS3ITCase和surefire:test@integration-tests。本机MinIO仍使用显式Docker socket/API1.44/ARM镜像覆盖，保持Ryuk/启动检查，不碰业务bucket。

连接器：paimon-spark/paimon-spark-3.5/target/paimon-spark-3.5-1.3.2.jar。
SHA-256：181db4fa013ef15dab7ddd11c5c19259d5b01e4a7d81898187c6673324bfdc80。
PartitionArchiver.class：30e6ba6ab5166c68207730cfbc1657a0af68015add029535be0847ae9aca7e86，当前编译class与JAR内class一致。
旧AP-CALL产物SHA仅是初次交付历史，不能用来校验此新JAR。

## 本地提交与收尾

初始已测试实现保存为：
- fe8fab92f：URI/复制基础与测试。
- e40b280df：core引用替换与故障保护。
- 971a93ed6：Spark CALL入口和本地/MinIO测试。

修正和文档另提交到同一本地分支，最终commit见git log；不push、不发布。原checkout保持不变，原两份MD SHA仍同AP-00记录。GC/drop、stream/cache、生产资源预算和分布式部署继续保留未完成状态；局部验证不等于生产全量放行。

源码修正提交：abee90821（fix: preserve archive protection and require partition inactivity）。文档后续独立提交，避免把未通过的候选记录成最终代码。

收尾验证：8份Markdown解析、27个相对链接、11段shell语法通过；git diff --check通过。RAT根检查4680资源，Unapproved=0、unknown=0，日志/tmp/paimon-archive-review-final-license.log。结束时org.testcontainers标签容器清单为空，原checkout两份MD SHA复核未变。所有Maven/Docker验证会话已完成。
