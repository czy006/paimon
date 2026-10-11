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

# Paimon External Data Paths(外部数据路径)功能架构解析

> 基于 master 分支源码分析(2026-10)。核心配置:`data-file.external-paths`、`data-file.external-paths.strategy`。

## 1. 功能定位

External Data Paths 允许把表的**数据文件写到表目录之外的多个外部路径**上(可以是不同文件系统、不同 bucket、不同存储介质),实现:

- **多存储混布**:热数据/冷数据、或数据与元数据分离存放(如表在 HDFS、数据文件在 S3/OSS);
- **跨文件系统分流**:一次写入按策略(round-robin / 加权 / 熵注入 / 指定文件系统)把文件散到多条路径;
- **零拷贝挂载**:通过 `import_files` 直接把已存在的文件挂载进表,不搬数据;
- **存量迁移**:配合 `compaction.force-rewrite-all-files` 把已有数据整体重写到外部路径。

关键设计:**逻辑 bucket 分配与物理文件位置解耦**。文件的逻辑归属(分区/bucket/level)不变,物理位置由 manifest 中的 `_EXTERNAL_PATH` 字段记录,读取时透明解析。

```
┌─────────────────────────────────────────────────────────────────────┐
│                        表目录 (table path)                          │
│  ┌──────────┐ ┌──────────┐ ┌─────────┐ ┌────────┐ ┌──────────────┐  │
│  │ snapshot │ │ manifest │ │ schema  │ │ index  │ │ partition/   │  │
│  │          │ │ (_EXTERNAL_PATH 记录外部文件完整路径) │ │ bucket-*(默认路径) │  │
│  └──────────┘ └──────────┘ └─────────┘ └────────┘ └──────────────┘  │
└──────────────────────────────┬──────────────────────────────────────┘
                               │ 数据文件可落在:
              ┌────────────────┼────────────────┐
              ▼                ▼                ▼
      ┌──────────────┐ ┌──────────────┐ ┌──────────────┐
      │ hdfs://nn/…" │ │ s3://bucket/…│ │ oss://bucket/…│   ← external paths
      │ dt=…/bucket-0│ │ dt=…/bucket-0│ │ 1001/1001/…  │   (含 relative bucket path,
      └──────────────┘ └──────────────┘ └──────────────┘    entropy 策略再加 hash 子目录)
```

## 2. 配置项总览

全部定义在 `paimon-api/src/main/java/org/apache/paimon/CoreOptions.java`:

| 配置项 | 默认值 | 说明 |
|---|---|---|
| `data-file.external-paths` | 无 | 外部路径列表,逗号分隔。**每条路径必须带 scheme**(如 `s3://bkt/paimon-external`),否则构造时报错 |
| `data-file.external-paths.strategy` | `none` | 路径选择策略,枚举见下 |
| `data-file.external-paths.specific-fs` | 无 | `specific-fs` 策略下生效,按 scheme 前缀过滤外部路径,现支持 `s3`、`oss` |
| `data-file.external-paths.weights` | 无 | `weight-robin` 策略下生效,如 `'10,5,15'`,与路径按序对应,必须为正整数 |
| `compaction.force-rewrite-all-files` | `false` | 强制 compaction 重写所有文件(含大文件),用于向外部路径整体迁移数据 |

策略枚举 `CoreOptions.ExternalPathStrategy`(配置值 → 行为):

| 枚举 | 配置值 | 行为 |
|---|---|---|
| `NONE` | `none` | 不使用外部路径,数据仍写表内默认路径 |
| `SPECIFIC_FS` | `specific-fs` | 只保留 scheme 匹配 `specific-fs` 的路径(通常剩一条),退化成单路径轮询 |
| `ROUND_ROBIN` | `round-robin` | 逐个路径轮询写新文件 |
| `ENTROPY_INJECT` | `entropy-inject` | 轮询路径 + 按文件名 hash 注入多层子目录(打散小文件) |
| `WEIGHTED` | `weight-robin` | 按权重随机选路径(注意配置值是 `weight-robin` 而非 `weighted`) |

## 3. 整体架构图

```
┌────────────────────────────────── 配置层 ──────────────────────────────────┐
│ CoreOptions: DATA_FILE_EXTERNAL_PATHS / _STRATEGY / _SPECIFIC_FS /        │
│              _WEIGHTS / COMPACTION_FORCE_REWRITE_ALL_FILES                │
└───────────────────────────────────┬───────────────────────────────────────┘
                                    │ options
┌───────────────────────────────────▼───────────────────────────────────────┐
│ Store 层  AbstractFileStore.createExternalPaths()                          │
│  · 解析逗号分隔路径 → List<Path>,校验 scheme 非空                           │
│  · SPECIFIC_FS:按 specific-fs 过滤,要求结果非空                            │
│  · strategy == NONE 或路径为空 → 空列表(功能关闭)                          │
└───────────────────────────────────┬───────────────────────────────────────┘
                                    │ 构造 FileStorePathFactory(携带 paths+strategy+weights)
┌───────────────────────────────────▼───────────────────────────────────────┐
│ 工厂层  FileStorePathFactory (每个 Store 一份)                             │
│  createDataFilePathFactory(partition, bucket)                             │
│    ├─ relativeBucketPath(partition,bucket)   ← 分区目录/bucket-N(相对路径)  │
│    └─ ExternalPathProvider.create(strategy, paths, relBucketPath, weights) │
└───────────────────────────────────┬───────────────────────────────────────┘
                                    │ 每个 partition+bucket 一个
┌───────────────────────────────────▼───────────────────────────────────────┐
│ 路径选择层  ExternalPathProvider (paimon-common/fs)                        │
│  ├─ RoundRobinExternalPathProvider   (轮询)                                │
│  ├─ WeightedExternalPathProvider     (累积权重 TreeMap 随机)                │
│  └─ EntropyInjectExternalPathProvider(轮询 + murmur3 hash 子目录)          │
│  getNextExternalDataPath(fileName) = 外部根 + 相对bucket路径 [+ hash目录]    │
│                                            + fileName                      │
└───────────────────────────────────┬───────────────────────────────────────┘
                                    │ 注入 DataFilePathFactory
┌───────────────────────────────────▼───────────────────────────────────────┐
│ 写入层  DataFilePathFactory.newPathFromName(fileName)                      │
│   externalPathProvider != null ? provider.getNextExternalDataPath(fileName)│
│                                 : new Path(parent, fileName)   ← 默认表内   │
│                                                                            │
│ RowDataFileWriter.result():                                                │
│   externalPath = isExternalPath ? path.toString() : null  ← 写进 DataFileMeta│
└───────────────────────────────────┬───────────────────────────────────────┘
                                    │ DataFileMeta._EXTERNAL_PATH (field 17)
┌───────────────────────────────────▼───────────────────────────────────────┐
│ 元数据层  manifest FileEntry.externalPath                                  │
│   · 随 manifest 持久化;snapshots/tags/index manifest 同样支持               │
│   · 文件名(fileName)仍全局唯一,用于孤儿清理时的 used 集合比对               │
└───────────────────────────────────┬───────────────────────────────────────┘
                                    │ 读取
┌───────────────────────────────────▼───────────────────────────────────────┐
│ 读取层  DataFilePathFactory.toPath(DataFileMeta)                           │
│   file.externalPath().map(Path::new).orElse(new Path(parent, fileName))    │
│   调用方:KeyValueFileReaderFactory / RawFileSplitRead /                    │
│          DataEvolutionSplitRead / FileIndexEvaluator / 文件删除等           │
│   FileIO 按 external path 的 URI scheme 选择实现(需有对应 FS 与 credentials)│
└────────────────────────────────────────────────────────────────────────────┘
```

## 4. 核心机制详解

### 4.1 路径选择:`ExternalPathProvider`

接口位于 `paimon-common/src/main/java/org/apache/paimon/fs/ExternalPathProvider.java`,静态工厂 `create()` 按 strategy 分发:

```
                strategy
   ┌──────────────┬─────────────┐
   │ ENTROPY_INJECT             │ SPECIFIC_FS(与 ROUND_ROBIN 共用,
   ▼              │             │  路径已在 Store 层按 scheme 过滤)
EntropyInject     ▼             ▼
   └─────► RoundRobin ◄── WEIGHTED 且 (路径数<2 或无权重) 时也退化到它
                │
                ▼ WEIGHTED(路径≥2 且配了权重)
          WeightedExternalPathProvider
```

三个实现的选路算法:

| 实现 | 算法 | 生成路径形态 |
|---|---|---|
| `RoundRobinExternalPathProvider` | `position` 从随机值起步,每次 `++` 后取模轮询 | `extRoot/分区…/bucket-N/fileName` |
| `WeightedExternalPathProvider` | 权重累积成 `TreeMap<Double,Path>`,每次按 `[0,totalWeight)` 均匀随机数 `higherEntry` 选中 | 同上 |
| `EntropyInjectExternalPathProvider` | 对 fileName 做 `murmur3_32` hash,取低 20 bit,切成 3 层 × 4 字符子目录(余数再一层);**外部根仍然轮询** | `extRoot/分区…/bucket-N/1001/1001/1001/余数/fileName` |

熵注入的子目录设计是为了对象存储(如 S3)前缀热点打散,同时方便按目录做孤儿清理。`relativeBucketPath` 保证外部文件依旧保持 `分区/bucket-N` 的目录语义(逻辑归属不变,只是根换了)。

### 4.2 写入链路

以主键表为例的调用链:

```
FileStoreTable.write()
  → MergeTreeWriter → KeyValueFileWriterFactory.createRollingMergeTreeFileWriter(...)
      · pathFactory = store.pathFactory().createDataFilePathFactory(partition, bucket)
      · isExternalPath = pathFactory.isExternalPath()   // 即 provider != null
  → KeyValueDataFileWriterImpl(isExternalPath) → RowDataFileWriter
      · newPath() → newPathFromName() → provider.getNextExternalDataPath(fileName)
      · close 后 result():
            externalPath = isExternalPath ? path.toString() : null
            DataFileMeta.forAppend(..., externalPath, ...)
  → commit: externalPath 随 FileEntry 写入 manifest
```

同样走这条链路的还有:changelog 文件、blob/video 文件(`newBlobPath/newVideoPath`)、DV 文件(`DeletionFileWriter` 也会记录 externalPath)、file index(`KeyValueClusteringFileWriter` 把 `.index` 写到数据文件同目录)。

**要点**:`_EXTERNAL_PATH` 存的是**完整绝对 URI**(写入端 `path.toString()`),为空(null)表示文件在表内默认路径——所以一张表内可以同时存在"表内文件 + 多个外部路径文件"混合状态。

### 4.3 元数据与读取

- `DataFileMeta` 第 17 列 `_EXTERNAL_PATH`(nullable string),manifest entry 同名字段,见 `docs/docs/concepts/spec/manifest.md`;
- `PojoDataFileMeta.externalPathDir()` 返回 external path 的父目录,用于**对齐语义**:`toAlignedPath(fileName, aligned)` 让衍生文件(compacted changelog、rewrite 后的 file index、Spark copy 出的新文件)与源文件落在同一外部目录;
- 读取端在 `DataFilePathFactory.toPath(DataFileMeta/FileEntry)` 统一解析:有 externalPath 用之,否则用 `表根/分区/bucket/fileName`;
- 由于 externalPath 是带 scheme 的绝对 URI,`FileIO.get()` 会按 scheme 加载对应 FileIO/FileSystem 实现——**读取端 classpath 与 fs.* 配置必须覆盖外部路径的存储系统**。

### 4.4 Compaction 与存量数据迁移

Compaction 输出文件同样走 `DataFilePathFactory.newPath()`——**如果 compact 任务携带了 external-paths 选项**(如 `CALL sys.compact(... 'data-file.external-paths'='s3://…')`),输出就写到外部路径。但默认 compaction 会"抄近路":

- Append 表(`BucketedAppendCompactManager.FullCompactTask`):`fileSize >= compactionFileSize` 的大文件会被移出待压实队列;
- 主键表(`MergeTreeCompactTask.upgrade()`):达到 max level 的大文件只做 level 提升不重写数据。

这些文件若不重写,数据就永远留在原路径,无法完成整体迁移。`compaction.force-rewrite-all-files = true` 时:

1. `triggerFullCompaction` 不再要求"至少 3 个小文件";
2. 大文件不再被移出队列 / 不再走 upgrade 捷径,全部走真正重写;
3. (主键表)`CompactStrategy.pickFullCompaction` 会选中 max level 全部文件,以 `fileRewrite=true` 构造 CompactUnit,逐文件重写。

结果:一个 partition/bucket 的全部数据被重写到 external path,manifest 中旧 entry(指向原路径)被替换为带 `_EXTERNAL_PATH` 的新 entry,原文件随后由 snapshot 过期/删除逻辑清理。

## 5. 周边生态

### 5.1 Flink 流作业动态切换路径(ConfigRefresher)

`paimon-flink-common/.../flink/sink/ConfigRefresher.java` + `FlinkConnectorOptions.SINK_WRITER_REFRESH_DETECTORS`(`sink.writer-refresh-detectors`,目前唯一支持的组就是 `external-paths`):

```
ALTER TABLE t SET('data-file.external-paths'='s3://b2/…')   -- 产生新 schema
        │
Flink TableWriteOperator 每条记录 tryRefresh()
        │ schema id 变新?
        ▼ 比较配置组(external-paths / strategy / specific-fs)
值变化 → table.copy(newOptions) → refresher.refresh(table) → 替换底层 writer
        (作业不重启,后续新文件写向新路径;注意组内不含 weights)
```

compaction 作业有对应 `CompactRefresher`。

### 5.2 孤儿文件清理(OrphanFilesClean)

`paimon-core/.../operation/OrphanFilesClean.java` 与 Flink 分布式版 `FlinkOrphanFilesClean`:

- 扫描候选目录时,把 `data-file.external-paths` 的每条路径也按 `分区/bucket-*` 规则递归列出(`listFileDirs`);
- 防误删三道闸:① `olderThan`(默认 1 天前)只清旧文件,避免删掉写入中未提交的文件;② 汇总**所有分支全部 snapshot/tag** 引用的文件名构建 used 集合,候选文件按**文件名**比对,在集合内不删;③ 读 manifest 失败(FileNotFound)跳过该 snapshot 并重试 3 次。

### 5.3 零拷贝挂载:import_files

`paimon-core/.../migrate/ExternalFileImporter.java` + Flink/Spark `ImportFilesProcedure`(procedure 名 `import_files`):

- 面向**无主键、bucket=-1 的 append 表**,不支持 row tracking / data-evolution;
- 列出 location 下匹配表格式的文件 → 扫描已有 split 的 `externalPath()` 去重 → 读取文件统计构造 DataFileMeta → `fileMeta.newExternalPath(externalPath)` 把外部路径写入元数据 → 单次 commit;
- 文件本体不移动,Paimon 直接引用外部文件。

### 5.4 Spark copy_files 与删表清理

- Spark `CopyFilesUtil.toNewDataFileMeta/toNewIndexFileMeta`:整表拷贝到新表时,若源文件带 externalPath,新文件元数据的 externalPath = 旧 external 目录 + 新文件名(保留外部布局语义);Flink `CopyFilesAction` 则要求所有文件在源表根路径下(面向无 external path 的整表搬迁,已 @Deprecated);
- 删表:`AbstractCatalog.dropTable` 遍历**主表+所有 branch 的所有历史 schema**,收集曾出现过的 `data-file.external-paths`(及 `global-index.external-path`),`HiveCatalog.dropTableImpl` 在删表目录后把收集到的 external 目录也一并 `deleteDirectoryQuietly`——历史用过的外部路径不会泄漏。

### 5.5 相关但独立的机制

- `global-index.external-path`(`FileStorePathFactory.globalIndexExternalRootDir`):全局索引文件独立的外部存放根,`IndexFileMeta.externalPath()` 同样走 manifest 记录,机制与数据文件 external path 同构但配置独立;
- Hive 的 EXTERNAL/MANAGED 表类型(`HiveCatalog.usingExternalTable`)是 Hive 概念,与本功能无关。

## 6. 限制与约束

| 场景 | 约束 | 出处 |
|---|---|---|
| 主键表 managed BLOB | 不支持 `data-file.external-paths` | `SchemaValidation`(校验 dataFileExternalPaths()==null) |
| Chain read | 不支持 external path(直接抛异常) | `FileStorePathFactory.createChainReadDataFilePathFactory` |
| Format Table(csv/json 等 format 表) | 不支持,显式传 NONE | `FormatTableFileWriter` |
| 外部路径格式 | 必须带 scheme;SPECIFIC_FS 要求路径非空且需配 specific-fs | `AbstractFileStore.createExternalPaths` |
| 权重 | 正整数,数量与路径一致;不满足时 WEIGHTED 退化为轮询 | `CoreOptions.externalPathWeights` / `ExternalPathProvider.create` |
| Flink 动态刷新 | 需显式配 `sink.writer-refresh-detectors=external-paths`;组内不含 weights | `ConfigRefresher.configGroups` |
| 读取端 | external path 的 scheme 需有对应 FileIO/FileSystem 实现与凭据配置 | `FileIO.get(Path, CatalogContext)` |

## 7. 关键类速查表

| 层 | 类 | 位置(paimon-*) | 职责 |
|---|---|---|---|
| 配置 | `CoreOptions` | api | 4+1 个配置项、策略枚举、权重解析 |
| 路径选择 | `ExternalPathProvider` | common/fs | 接口 + 静态工厂分发 |
| 路径选择 | `RoundRobin/Weighted/EntropyInject ExternalPathProvider` | common/fs | 三种选路算法 |
| 工厂 | `FileStorePathFactory` | core/utils | 持有 externalPaths,按 partition+bucket 创建 provider |
| 工厂 | `DataFilePathFactory` | core/io | newPathFromName 选路写入;toPath 读取解析;toAlignedPath 目录对齐 |
| Store | `AbstractFileStore` | core | createExternalPaths 解析校验配置 |
| 写入 | `KeyValueFileWriterFactory` / `RowDataFileWriter` | core/io | 写文件、把 externalPath 写进 DataFileMeta |
| 元数据 | `DataFileMeta`(_EXTERNAL_PATH)/ `FileEntry` | core | 持久化外部路径 |
| Compaction | `BucketedAppendCompactManager` / `CompactStrategy` / `MergeTreeCompactTask` | core | force-rewrite-all-files 强制全量重写 |
| 动态刷新 | `ConfigRefresher` / `CompactRefresher` | flink-common | 流作业不重启切换外部路径 |
| 清理 | `OrphanFilesClean` / `FlinkOrphanFilesClean` | core / flink-common | 外部目录纳入孤儿扫描,按文件名防误删 |
| 挂载 | `ExternalFileImporter` + `import_files` procedure | core/migrate, flink, spark | 零拷贝挂载外部文件 |
| 拷贝 | `CopyFilesUtil`(Spark) | spark-common | 拷贝时保留 external 目录布局 |
| 删表 | `AbstractCatalog` + `HiveCatalog` | core, hive-catalog | 收集历史 schema 的全部外部路径并删除 |
