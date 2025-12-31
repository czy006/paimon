# Paimon Flink CDC 同时写出到主键表和追加表实现方案

> **文档版本**: v1.0
> **分析日期**: 2025-12-24
> **场景**: CDC订阅同时写出到主键表（支持更新删除）和追加表（仅追加）

---

## 1. 需求背景

在某些业务场景中，需要将CDC数据同时写入到两张表：
- **主键表**：支持UPDATE、DELETE操作，用于实时查询和更新
- **追加表**：仅支持INSERT操作，用于历史数据留存、审计、数据分析

### 1.1 典型应用场景

| 场景 | 主键表用途 | 追加表用途 |
|------|-----------|-----------|
| 订单系统 | 当前订单状态 | 订单变更历史 |
| 用户中心 | 用户最新信息 | 用户行为日志 |
| 库存管理 | 实时库存 | 库存变动记录 |
| 金融交易 | 当前账户余额 | 交易流水记录 |

### 1.2 数据流向示意图

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                        CDC数据同时写入两张表                                         │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                     │
│  MySQL CDC (Debezium)                                                             │
│      │                                                                             │
│      ▼                                                                             │
│  ┌──────────────────────────────────────────────────────────────────────────────┐  │
│  │  一条CDC记录:                                                                 │  │
│  │  {                                                                            │  │
│  │    "before": {"id": 1, "name": "Alice", "status": "active"},                   │  │
│  │    "after": {"id": 1, "name": "Bob", "status": "inactive"},                    │  │
│  │    "op": "u"                                                                 │  │
│  │  }                                                                            │  │
│  └──────────────────────────────────────────────────────────────────────────────┘  │
│                              │                                                     │
│           ┌───────────────┴───────────────┐                                       │
│           ▼                               ▼                                       │
│  ┌──────────────────────┐      ┌──────────────────────┐                            │
│  │  主键表 Sink          │      │  追加表 Sink          │                            │
│  │  (支持UPDATE/DELETE) │      │  (仅INSERT)           │                            │
│  │                      │      │                      │                            │
│  │  操作类型映射:        │      │  操作类型映射:        │                            │
│  │  c/r -> INSERT       │      │  c/r -> INSERT        │                            │
│  │  u   -> INSERT+DELETE │      │  u   -> INSERT(新值)   │                            │
│  │  d   -> DELETE        │      │  d   -> INSERT(旧值)   │                            │
│  └──────────────────────┘      └──────────────────────┘                            │
│           │                               │                                       │
│           └───────────────┬───────────────┘                                       │
│                          ▼                                                        │
│                   Paimon Storage                                                 │
│                   - orders_pk (主键表)                                          │
│                   - orders_append (追加表)                                      │
│                                                                                     │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. 现有架构分析

### 2.1 单表写入流程（现有实现）

基于之前的分析，现有CDC Action只支持写入一张表：

```
SyncTableActionBase.buildSink()
    -> CdcSinkBuilder.build()
        -> 根据BucketMode选择Sink:
            - HASH_FIXED -> CdcFixedBucketSink
            - BUCKET_UNAWARE -> CdcAppendTableSink
            - HASH_DYNAMIC -> CdcDynamicBucketSink
```

**代码位置**: `CdcSinkBuilder.java:129-141`
```java
BucketMode bucketMode = dataTable.bucketMode();
switch (bucketMode) {
    case HASH_FIXED:
        return buildForFixedBucket(converted);
    case HASH_DYNAMIC:
        return new CdcDynamicBucketSink((FileStoreTable) table).build(converted, parallelism);
    case POSTPONE_MODE:
        return buildForPostponeBucket(converted);
    case BUCKET_UNAWARE:
        return buildForUnawareBucket(converted);  // 追加表
    default:
        throw new UnsupportedOperationException("Unsupported bucket mode: " + bucketMode);
}
```

### 2.2 现有架构的限制

| 限制点 | 说明 |
|--------|------|
| 单Sink | 每个Action只能构建一个Sink |
| 固定路由 | 数据只能流向一个目标表 |
| 统一配置 | 无法为主键表和追加表配置不同的参数 |

---

## 3. 实现方案设计

### 3.1 方案概述

在现有的CDC Action流程中，添加一个**分支算子**，将数据流复制为两路，分别写入主键表和追加表。

### 3.2 整体架构图

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                    同时写入主键表和追加表的架构设计                                 │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                     │
│  1. CDC Source                                                                     │
│     Kafka/Pulsar/MySQL CDC                                                         │
│                          │                                                         │
│                          ▼                                                         │
│  2. Record Parsing                                                                 │
│     CdcSourceRecord -> RichCdcMultiplexRecord / CdcRecord                         │
│                          │                                                         │
│                          ▼                                                         │
│  3. 【新增】分支算子 (Branching Operator)                                      │
│     ┌────────────────────────────────────────────────────────────────────────────┐  │
│     │  DataStreamSplitter / ProcessFunction                                    │  │
│     │    - 将每条记录复制为两份                                                  │  │
│     │    - 输出到两个Side Output                                               │  │
│     │                                                                          │  │
│     │  input                                                                  │  │
│     │    ├── (主表输出) mainOutput                                             │  │
│     │    └── (追加表输出) appendOutput                                         │  │
│     └────────────────────────────────────────────────────────────────────────────┘  │
│         │                              │                                          │
│         ▼                              ▼                                          │
│  4a. 主键表处理                 4b. 追加表处理                                              │
│     ┌────────────────────────┐    ┌────────────────────────┐                         │
│     │ Schema Evolution       │    │ Schema Evolution       │                         │
│     │ (并行度=1)              │    │ (并行度=1)              │                         │
│     └────────────────────────┘    └────────────────────────┘                         │
│         │                              │                                          │
│         ▼                              ▼                                          │
│  5a. 主键表Sink                 5b. 追加表Sink                                              │
│     ┌────────────────────────┐    ┌────────────────────────┐                         │
│     │ CdcFixedBucketSink     │    │ CdcAppendTableSink     │                         │
│     │ 或                     │    │                        │                         │
│     │ CdcDynamicBucketSink   │    │ - BUCKET_UNAWARE      │                         │
│     └────────────────────────┘    └────────────────────────┘                         │
│         │                              │                                          │
│         └──────────────┬───────────────┘                                          │
│                        ▼                                                        │
│                  Paimon Storage                                                 │
│                  - table_pk (主键表)                                           │
│                  - table_append (追加表)                                        │
│                                                                                     │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

### 3.3 实现位置

**方案A: 扩展SyncTableActionBase（推荐）**

在`SyncTableActionBase.buildSink()`中添加分支逻辑：

```java
// 文件: SyncTableActionBase.java
// 位置: buildSink()方法

@Override
protected void buildSink(
        DataStream<RichCdcMultiplexRecord> input,
        EventParser.Factory<RichCdcMultiplexRecord> parserFactory) {

    // 原有主键表Sink
    CdcSinkBuilder<RichCdcMultiplexRecord> primarySinkBuilder =
        new CdcSinkBuilder<RichCdcMultiplexRecord>()
            .withInput(input)
            .withParserFactory(parserFactory)
            .withTable(fileStoreTable)
            .withIdentifier(new Identifier(database, table))
            .withTypeMapping(typeMapping)
            .withCatalogLoader(catalogLoader());

    // 【新增】追加表Sink
    FileStoreTable appendTable = getOrCreateAppendTable();
    CdcSinkBuilder<CdcRecord> appendSinkBuilder =
        new CdcSinkBuilder<CdcRecord>()
            .withInput(convertToCdcRecord(input))  // 转换数据类型
            .withParserFactory(createAppendParserFactory())
            .withTable(appendTable)
            .withIdentifier(new Identifier(database, table + "_append"))
            .withTypeMapping(typeMapping)
            .withCatalogLoader(catalogLoader());

    // 构建两个Sink
    primarySinkBuilder.build();
    appendSinkBuilder.build();
}
```

**方案B: 自定义Action（更灵活）**

创建一个新的Action类`DualTableSyncAction`：

```java
public class DualTableSyncAction extends SyncTableActionBase {
    private String appendTableSuffix = "_append";

    @Override
    protected void buildSink(
            DataStream<RichCdcMultiplexRecord> input,
            EventParser.Factory<RichCdcMultiplexRecord> parserFactory) {

        // 1. 创建分支算子，复制数据流
        SingleOutputStreamOperator<RichCdcMultiplexRecord> branched =
            input.process(new BranchingProcessFunction())
                .name("Branch to Primary and Append");

        // 2. 主键表Sink
        CdcSinkBuilder<RichCdcMultiplexRecord> primarySinkBuilder =
            new CdcSinkBuilder<RichCdcMultiplexRecord>()
                .withInput(input)  // 原始流
                .withParserFactory(parserFactory)
                .withTable(fileStoreTable)
                .withIdentifier(new Identifier(database, table))
                .withTypeMapping(typeMapping)
                .withCatalogLoader(catalogLoader());

        // 3. 追加表Sink
        FileStoreTable appendTable = getOrCreateAppendTable();
        CdcSinkBuilder<RichCdcMultiplexRecord> appendSinkBuilder =
            new CdcSinkBuilder<RichCdcMultiplexRecord>()
                .withInput(convertToAppendFormat(branched))
                .withParserFactory(parserFactory)
                .withTable(appendTable)
                .withIdentifier(new Identifier(database, table + appendTableSuffix))
                .withTypeMapping(typeMapping)
                .withCatalogLoader(catalogLoader());

        // 4. 构建两个Sink
        primarySinkBuilder.build();
        appendSinkBuilder.build();
    }
}
```

---

## 4. 核心算子实现

### 4.1 分支算子 (BranchingProcessFunction)

**功能**: 将每条CDC记录复制为两份，分别输出到主键表和追加表

```java
package org.apache.paimon.flink.action.cdc;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 将CDC记录分支到主键表和追加表的ProcessFunction
 */
public class BranchingProcessFunction extends ProcessFunction<
        RichCdcMultiplexRecord, RichCdcMultiplexRecord> {

    // 追加表输出标签
    private static final OutputTag<RichCdcMultiplexRecord> APPEND_OUTPUT_TAG =
        new OutputTag<RichCdcMultiplexRecord>("append-output") {};

    @Override
    public void processElement(
            RichCdcMultiplexRecord record,
            Context ctx,
            Collector<RichCdcMultiplexRecord> out) throws Exception {

        // 1. 主输出：主键表数据（保持原样）
        out.collect(record);

        // 2. Side Output：追加表数据（可能需要转换）
        RichCdcMultiplexRecord appendRecord = convertToAppendRecord(record);
        ctx.output(APPEND_OUTPUT_TAG, appendRecord);
    }

    /**
     * 将CDC记录转换为追加表格式
     * - UPDATE操作转换为INSERT（使用after值）
     * - DELETE操作转换为INSERT（使用before值，保留历史）
     * - INSERT/READ保持不变
     */
    private RichCdcMultiplexRecord convertToAppendRecord(RichCdcMultiplexRecord record) {
        CdcRecord cdcRecord = record.record();
        RowKind kind = cdcRecord.kind();

        // 根据操作类型决定追加表的数据内容
        CdcRecord appendRecord;
        switch (kind) {
            case INSERT:
            case UPDATE_AFTER:
                // INSERT和UPDATE的after状态 -> 直接插入
                appendRecord = new CdcRecord(RowKind.INSERT, cdcRecord.data());
                break;
            case UPDATE_BEFORE:
            case DELETE:
                // UPDATE的before和DELETE -> 插入旧值（保留变更前的状态）
                appendRecord = new CdcRecord(RowKind.INSERT, cdcRecord.data());
                break;
            default:
                appendRecord = cdcRecord;
        }

        return new RichCdcMultiplexRecord(
            record.databaseName(),
            record.tableName(),
            appendRecord
        );
    }

    public static OutputTag<RichCdcMultiplexRecord> getAppendOutputTag() {
        return APPEND_OUTPUT_TAG;
    }
}
```

### 4.2 操作类型转换逻辑

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                        操作类型转换映射                                             │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                     │
│  CDC操作类型          主键表操作           追加表操作                               │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐                        │
│  │ INSERT (c)   │ -> │ INSERT        │ -> │ INSERT        │                        │
│  │              │    │ (新数据)       │    │ (新数据)       │                        │
│  └──────────────┘    └──────────────┘    └──────────────┘                        │
│                                                                                     │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐                        │
│  │ UPDATE (u)   │ -> │ DELETE        │ -> │ INSERT        │                        │
│  │              │    │ (旧数据)       │    │ (新数据)       │                        │
│  │              │    │ INSERT        │    │                │                        │
│  │              │    │ (新数据)       │    │                │                        │
│  └──────────────┘    └──────────────┘    └──────────────┘                        │
│                                                                                     │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐                        │
│  │ DELETE (d)   │ -> │ DELETE        │ -> │ INSERT        │                        │
│  │              │    │ (旧数据)       │    │ (旧数据)       │                        │
│  └──────────────┘    └──────────────┘    └──────────────┘                        │
│                                                                                     │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐                        │
│  │ READ (r)     │ -> │ INSERT        │ -> │ INSERT        │                        │
│  │              │    │ (快照数据)     │    │ (快照数据)     │                        │
│  └──────────────┘    └──────────────┘    └──────────────┘                        │
│                                                                                     │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

### 4.3 添加位置说明

**方法1: 修改SyncTableActionBase.buildSink()**

在现有的`buildSink`方法中添加追加表Sink构建逻辑：

```java
// 文件: paimon-flink-cdc/src/main/java/org/apache/paimon/flink/action/cdc/SyncTableActionBase.java

@Override
protected void buildSink(
        DataStream<RichCdcMultiplexRecord> input,
        EventParser.Factory<RichCdcMultiplexRecord> parserFactory) {

    // ========== 原有代码：主键表Sink ==========
    CdcSinkBuilder<RichCdcMultiplexRecord> sinkBuilder =
        new CdcSinkBuilder<RichCdcMultiplexRecord>()
            .withInput(input)
            .withParserFactory(parserFactory)
            .withTable(fileStoreTable)
            .withIdentifier(new Identifier(database, table))
            .withTypeMapping(typeMapping)
            .withCatalogLoader(catalogLoader());

    String sinkParallelism = tableConfig.get(FlinkConnectorOptions.SINK_PARALLELISM.key());
    if (sinkParallelism != null) {
        sinkBuilder.withParallelism(Integer.parseInt(sinkParallelism));
    }
    sinkBuilder.build();

    // ========== 新增代码：追加表Sink ==========
    if (shouldCreateAppendTable()) {  // 通过配置判断是否创建追加表
        buildAppendTableSink(input, parserFactory);
    }
}

/**
 * 构建追加表Sink
 */
private void buildAppendTableSink(
        DataStream<RichCdcMultiplexRecord> input,
        EventParser.Factory<RichCdcMultiplexRecord> parserFactory) {

    String appendTableName = table + "_append";  // 追加表名称后缀

    // 1. 获取或创建追加表
    FileStoreTable appendTable = getOrCreateAppendTable(appendTableName);

    // 2. 添加分支算子
    SingleOutputStreamOperator<RichCdcMultiplexRecord> branched =
        input.process(new BranchingProcessFunction())
            .name("Branch for Append Table");

    // 3. 获取追加表数据流
    DataStream<RichCdcMultiplexRecord> appendStream =
        branched.getSideOutput(BranchingProcessFunction.getAppendOutputTag());

    // 4. 构建追加表Sink
    CdcSinkBuilder<RichCdcMultiplexRecord> appendSinkBuilder =
        new CdcSinkBuilder<RichCdcMultiplexRecord>()
            .withInput(appendStream)
            .withParserFactory(parserFactory)
            .withTable(appendTable)
            .withIdentifier(new Identifier(database, appendTableName))
            .withTypeMapping(typeMapping)
            .withCatalogLoader(catalogLoader());

    String sinkParallelism = tableConfig.get(FlinkConnectorOptions.SINK_PARALLELISM.key());
    if (sinkParallelism != null) {
        appendSinkBuilder.withParallelism(Integer.parseInt(sinkParallelism));
    }
    appendSinkBuilder.build();
}

/**
 * 判断是否需要创建追加表
 */
private boolean shouldCreateAppendTable() {
    String enableAppendTable = tableConfig.get("sink.append-table.enabled");
    return "true".equalsIgnoreCase(enableAppendTable);
}

/**
 * 获取或创建追加表
 */
private FileStoreTable getOrCreateAppendTable(String appendTableName) throws Exception {
    Identifier appendIdentifier = new Identifier(database, appendTableName);

    try {
        return (FileStoreTable) catalog.getTable(appendIdentifier);
    } catch (Catalog.TableNotExistException e) {
        // 创建追加表（无主键，BUCKET_UNAWARE模式）
        Schema appendSchema = buildAppendTableSchema();
        catalog.createTable(appendIdentifier, appendSchema, false);
        return (FileStoreTable) catalog.getTable(appendIdentifier);
    }
}

/**
 * 构建追加表Schema
 * - 移除主键
 * - 设置为BUCKET_UNAWARE模式
 */
private Schema buildAppendTableSchema() {
    Schema originalSchema = fileStoreTable.schema();

    return Schema.newBuilder()
        .options(originalSchema.options())
        .options(Collections.singletonMap("bucket", "0"))  // BUCKET_UNAWARE
        .comment("Append-only table for " + table)
        .columns(originalSchema.fields())
        .build();
}
```

**方法2: 创建新的Action类**

创建独立的Action类，避免修改现有代码：

```java
// 文件: paimon-flink-cdc/src/main/java/org/apache/paimon/flink/action/cdc/DualTableSyncAction.java

package org.apache.paimon.flink.action.cdc;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.action.cdc.CdcMetadataConverter;
import org.apache.paimon.table.FileStoreTable;

import java.util.Map;

/**
 * CDC Action that writes to both primary key table and append table simultaneously.
 */
public class DualTableSyncAction extends SyncTableActionBase {

    private String appendTableSuffix = "_append";
    private boolean enableAppendTable = false;

    public DualTableSyncAction(
            String database,
            String table,
            Map<String, String> catalogConfig,
            Map<String, String> cdcSourceConfig,
            SyncJobHandler.SourceType sourceType) {
        super(database, table, catalogConfig, cdcSourceConfig, sourceType);
    }

    /**
     * 启用追加表功能
     */
    public DualTableSyncAction withAppendTable(String suffix) {
        this.enableAppendTable = true;
        this.appendTableSuffix = suffix;
        return this;
    }

    @Override
    protected void buildSink(
            DataStream<RichCdcMultiplexRecord> input,
            EventParser.Factory<RichCdcMultiplexRecord> parserFactory) {

        // 1. 构建主键表Sink（原有逻辑）
        buildPrimaryTableSink(input, parserFactory);

        // 2. 构建追加表Sink（新增逻辑）
        if (enableAppendTable) {
            buildAppendTableSink(input, parserFactory);
        }
    }

    private void buildPrimaryTableSink(
            DataStream<RichCdcMultiplexRecord> input,
            EventParser.Factory<RichCdcMultiplexRecord> parserFactory) {
        // 原有的Sink构建逻辑
        CdcSinkBuilder<RichCdcMultiplexRecord> sinkBuilder =
            new CdcSinkBuilder<RichCdcMultiplexRecord>()
                .withInput(input)
                .withParserFactory(parserFactory)
                .withTable(fileStoreTable)
                .withIdentifier(new Identifier(database, table))
                .withTypeMapping(typeMapping)
                .withCatalogLoader(catalogLoader());

        String sinkParallelism = tableConfig.get(FlinkConnectorOptions.SINK_PARALLELISM.key());
        if (sinkParallelism != null) {
            sinkBuilder.withParallelism(Integer.parseInt(sinkParallelism));
        }
        sinkBuilder.build();
    }

    private void buildAppendTableSink(
            DataStream<RichCdcMultiplexRecord> input,
            EventParser.Factory<RichCdcMultiplexRecord> parserFactory) throws Exception {

        String appendTableName = table + appendTableSuffix;
        FileStoreTable appendTable = getOrCreateAppendTable(appendTableName);

        // 添加分支算子
        SingleOutputStreamOperator<RichCdcMultiplexRecord> branched =
            input.process(new BranchingProcessFunction())
                .name("Branch for Append Table");

        DataStream<RichCdcMultiplexRecord> appendStream =
            branched.getSideOutput(BranchingProcessFunction.getAppendOutputTag());

        // 构建追加表Sink
        CdcSinkBuilder<RichCdcMultiplexRecord> appendSinkBuilder =
            new CdcSinkBuilder<RichCdcMultiplexRecord>()
                .withInput(appendStream)
                .withParserFactory(parserFactory)
                .withTable(appendTable)
                .withIdentifier(new Identifier(database, appendTableName))
                .withTypeMapping(typeMapping)
                .withCatalogLoader(catalogLoader());

        String sinkParallelism = tableConfig.get(FlinkConnectorOptions.SINK_PARALLELISM.key());
        if (sinkParallelism != null) {
            appendSinkBuilder.withParallelism(Integer.parseInt(sinkParallelism));
        }
        appendSinkBuilder.build();
    }

    private FileStoreTable getOrCreateAppendTable(String appendTableName) throws Exception {
        Identifier appendIdentifier = new Identifier(database, appendTableName);

        try {
            return (FileStoreTable) catalog.getTable(appendIdentifier);
        } catch (Catalog.TableNotExistException e) {
            Schema appendSchema = buildAppendTableSchema();
            catalog.createTable(appendIdentifier, appendSchema, false);
            return (FileStoreTable) catalog.getTable(appendIdentifier);
        }
    }

    private Schema buildAppendTableSchema() {
        Schema originalSchema = fileStoreTable.schema();

        Map<String, String> appendOptions = new HashMap<>(originalSchema.options());
        appendOptions.put("bucket", "0");  // BUCKET_UNAWARE

        return Schema.newBuilder()
            .options(appendOptions)
            .comment("Append-only table for " + table)
            .columns(originalSchema.fields())
            .build();
    }
}
```

---

## 5. Flink DAG拓扑

### 5.1 双表写入的完整DAG

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                        Flink Job DAG - 双表写入                                    │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                     │
│  Kafka Source                                                                     │
│  (并行度: N)                                                                       │
│      │                                                                             │
│      ▼                                                                             │
│  ┌──────────────────────────────────────────────────────────────────────────────┐  │
│  │  DebeziumJsonRecordParser                                                   │  │
│  │  并行度: N                                                                    │  │
│  │  CdcSourceRecord -> RichCdcMultiplexRecord                                   │  │
│  └──────────────────────────────────────────────────────────────────────────────┘  │
│      │                                                                             │
│      ▼                                                                             │
│  ┌──────────────────────────────────────────────────────────────────────────────┐  │
│  │  RichCdcMultiplexRecordEventParser                                           │  │
│  │  并行度: N                                                                    │  │
│  │  解析Schema变更和数据                                                         │  │
│  └──────────────────────────────────────────────────────────────────────────────┘  │
│      │                                                                             │
│      ▼                                                                             │
│  ┌──────────────────────────────────────────────────────────────────────────────┐  │
│  │  CdcDynamicTableParsingProcessFunction                                       │  │
│  │  并行度: N                                                                    │  │
│  │  主输出: Void                                                                  │  │
│  │  Side Output 1: CdcMultiplexRecord (数据)                                    │  │
│  │  Side Output 2: Tuple2<Identifier, CdcSchema> (Schema变更)                    │  │
│  └──────────────────────────────────────────────────────────────────────────────┘  │
│      │                                                                             │
│      ▼                                                                             │
│  ┌──────────────────────────────────────────────────────────────────────────────┐  │
│  │  【新增】BranchingProcessFunction                                           │  │
│  │  并行度: N                                                                    │  │
│  │  主输出: RichCdcMultiplexRecord (主键表数据)                                  │  │
│  │  Side Output: RichCdcMultiplexRecord (追加表数据)                             │  │
│  └──────────────────────────────────────────────────────────────────────────────┘  │
│      │                              │                                              │
│      │ 主输出                        │ Side Output                                │
│      ▼                              ▼                                              │
│  ┌──────────────────────┐    ┌──────────────────────┐                            │
│  │ 主键表流程           │    │ 追加表流程           │                            │
│  │                      │    │                      │                            │
│  │ Case Sensitive      │    │ Case Sensitive      │                            │
│  │ Convert             │    │ Convert             │                            │
│  └──────────────────────┘    └──────────────────────┘                            │
│      │                              │                                              │
│      ▼                              ▼                                              │
│  ┌──────────────────────┐    ┌──────────────────────┐                            │
│  │ Partition           │    │ Rebalance           │                            │
│  │ (按主键Hash)         │    │ (负载均衡)           │                            │
│  └──────────────────────┘    └──────────────────────┘                            │
│      │                              │                                              │
│      ▼                              ▼                                              │
│  ┌──────────────────────┐    ┌──────────────────────┐                            │
│  │ CdcRecordStore      │    │ CdcAppendTable       │                            │
│  │ MultiWriteOperator  │    │ WriteOperator         │                            │
│  └──────────────────────┘    └──────────────────────┘                            │
│      │                              │                                              │
│      ▼                              ▼                                              │
│  ┌──────────────────────┐    ┌──────────────────────┐                            │
│  │ StoreMultiCommitter │    │ Committable          │                            │
│  │ (并行度=1)           │    │ (并行度=1)           │                            │
│  └──────────────────────┘    └──────────────────────┘                            │
│      │                              │                                              │
│      ▼                              ▼                                              │
│   Paimon Storage (主键表)         Paimon Storage (追加表)                          │
│                                                                                     │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 6. 配置示例

### 6.1 使用方式

**方式1: 使用DualTableSyncAction**

```java
KafkaSyncTableAction action = new KafkaSyncTableAction(
    "my_database",
    "orders",
    catalogConfig,
    kafkaConfig
)
.withKafka("my-topic")
.withValueFormat("debezium-json")
.withPrimaryKeys("id")
.withPartitionKeys("dt")
// 【新增】启用追加表
.withAppendTable("_log");  // 追加表后缀

action.run();
```

**方式2: 通过配置启用**

```java
Map<String, String> tableConfig = new HashMap<>();
tableConfig.put("sink.append-table.enabled", "true");
tableConfig.put("sink.append-table.suffix", "_log");
tableConfig.put("sink.parallelism", "4");

KafkaSyncTableAction action = new KafkaSyncTableAction(...)
.withTableConfig(tableConfig);
```

### 6.2 表结构对比

| 特性 | 主键表 (orders) | 追加表 (orders_log) |
|------|----------------|---------------------|
| 主键 | `id` | 无 |
| 分区键 | `dt` | 无 |
| Bucket模式 | `HASH_FIXED` | `BUCKET_UNAWARE` |
| 支持操作 | INSERT/UPDATE/DELETE | 仅INSERT |
| 用途 | 实时查询 | 历史记录/审计 |

---

## 7. 数据流转示例

### 7.1 场景：订单状态变更

```
MySQL操作:
UPDATE orders SET status = 'shipped' WHERE id = 1;

Debezium消息:
{
  "before": {"id": 1, "status": "pending"},
  "after": {"id": 1, "status": "shipped"},
  "op": "u",
  "ts_ms": 1735039200000
}

数据转换:

主键表 (orders):
┌──────────────────────────────────────┐
│  操作: DELETE (before)               │
│  数据: {"id": 1, "status": "pending"} │
└──────────────────────────────────────┘
┌──────────────────────────────────────┐
│  操作: INSERT (after)                │
│  数据: {"id": 1, "status": "shipped"} │
└──────────────────────────────────────┘
结果: id=1的记录从pending更新为shipped

追加表 (orders_log):
┌──────────────────────────────────────┐
│  操作: INSERT (after)                │
│  数据: {"id": 1, "status": "shipped"} │
└──────────────────────────────────────┘
结果: 新增一条shipped状态的记录
```

### 7.2 场景：订单删除

```
MySQL操作:
DELETE FROM orders WHERE id = 1;

Debezium消息:
{
  "before": {"id": 1, "status": "cancelled"},
  "op": "d",
  "ts_ms": 1735039200000
}

数据转换:

主键表 (orders):
┌──────────────────────────────────────┐
│  操作: DELETE                         │
│  数据: {"id": 1, "status": "cancelled"}│
└──────────────────────────────────────┘
结果: id=1的记录被删除

追加表 (orders_log):
┌──────────────────────────────────────┐
│  操作: INSERT                         │
│  数据: {"id": 1, "status": "cancelled"}│
└──────────────────────────────────────┘
结果: 保留被删除前的记录
```

---

## 8. 注意事项

### 8.1 一致性保证

| 问题 | 说明 | 解决方案 |
|------|------|----------|
| Exactly-Once | 两个独立的Sink，需要保证都成功或都失败 | 使用Flink Checkpoint的两阶段提交 |
| 顺序一致性 | 主键表和追加表的数据顺序可能不同 | 在同一Checkpoint内提交 |
| Schema同步 | 两张表的Schema需要同步变更 | 共享Schema Evolution流程 |

### 8.2 性能考虑

| 因素 | 影响 | 优化建议 |
|------|------|----------|
| 写入放大 | 每条记录写入两次 | 确保确实需要两张表 |
| 并行度 | 两个Sink并行写入 | 合理设置并行度 |
| Checkpoint | 需要等待两个Sink都完成 | 可能增加Checkpoint时间 |
| 网络IO | 双倍的网络传输 | 使用本地Catalog时影响较小 |

### 8.3 Schema演变

- **主键表**: 支持所有类型的Schema变更
- **追加表**: 需要特别注意，由于不支持DELETE操作，某些变更可能需要特殊处理
- **建议**: 主键表和追加表使用相同的Schema结构，只通过主键和Bucket模式区分

---

## 9. 完整实现示例

### 9.1 BranchingProcessFunction完整代码

```java
package org.apache.paimon.flink.action.cdc;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecord;
import org.apache.paimon.flink.sink.cdc.CdcRecord;
import org.apache.paimon.types.RowKind;

/**
 * ProcessFunction that branches CDC records to both primary table and append table.
 */
public class BranchingProcessFunction extends ProcessFunction<
        RichCdcMultiplexRecord, RichCdcMultiplexRecord> {

    public static final OutputTag<RichCdcMultiplexRecord> APPEND_OUTPUT_TAG =
        new OutputTag<RichCdcMultiplexRecord>("append-output") {};

    @Override
    public void processElement(
            RichCdcMultiplexRecord record,
            Context ctx,
            Collector<RichCdcMultiplexRecord> out) throws Exception {

        // 主输出：主键表数据（保持原样）
        out.collect(record);

        // Side Output：追加表数据（转换后）
        RichCdcMultiplexRecord appendRecord = convertToAppendRecord(record);
        ctx.output(APPEND_OUTPUT_TAG, appendRecord);
    }

    private RichCdcMultiplexRecord convertToAppendRecord(RichCdcMultiplexRecord record) {
        CdcRecord cdcRecord = record.record();
        RowKind kind = cdcRecord.kind();

        CdcRecord appendRecord;
        switch (kind) {
            case INSERT:
            case UPDATE_AFTER:
                // INSERT和UPDATE的after状态 -> 直接插入
                appendRecord = new CdcRecord(RowKind.INSERT, cdcRecord.data());
                break;
            case UPDATE_BEFORE:
            case DELETE:
                // UPDATE的before和DELETE -> 插入旧值（保留历史）
                appendRecord = new CdcRecord(RowKind.INSERT, cdcRecord.data());
                break;
            default:
                appendRecord = cdcRecord;
        }

        return new RichCdcMultiplexRecord(
            record.databaseName(),
            record.tableName(),
            appendRecord,
            record.cdcSchema()
        );
    }
}
```

### 9.2 使用示例

```java
package org.apache.paimon.flink.action.cdc;

import org.apache.paimon.flink.action.Action;
import org.apache.paimon.flink.action.cdc.kafka.KafkaSyncTableAction;

import java.util.HashMap;
import java.util.Map;

public class DualTableSyncExample {

    public static void main(String[] args) throws Exception {
        Map<String, String> catalogConfig = new HashMap<>();
        catalogConfig.put("warehouse", "file:///tmp/paimon");

        Map<String, String> kafkaConfig = new HashMap<>();
        kafkaConfig.put("properties.bootstrap.servers", "localhost:9092");
        kafkaConfig.put("properties.group.id", "paimon-cdc-group");
        kafkaConfig.put("topic", "orders-cdc");
        kafkaConfig.put("value.format", "debezium-json");

        Map<String, String> tableConfig = new HashMap<>();
        tableConfig.put("sink.parallelism", "4");
        // 启用追加表
        tableConfig.put("sink.append-table.enabled", "true");
        tableConfig.put("sink.append-table.suffix", "_log");

        Action action = new DualTableSyncAction(
            "my_database",
            "orders",
            catalogConfig,
            kafkaConfig,
            SyncJobHandler.SourceType.KAFKA
        )
        .withPrimaryKeys("id")
        .withPartitionKeys("dt")
        .withTableConfig(tableConfig);

        action.run();
    }
}
```

---

## 10. 总结

### 10.1 实现要点

| 要点 | 说明 |
|------|------|
| **添加位置** | 在`SyncTableActionBase.buildSink()`中或创建新的`DualTableSyncAction` |
| **核心算子** | `BranchingProcessFunction`使用Side Output复制数据流 |
| **主键表Sink** | 使用现有的`CdcFixedBucketSink`或`CdcDynamicBucketSink` |
| **追加表Sink** | 使用现有的`CdcAppendTableSink` |
| **数据转换** | UPDATE转DELETE+INSERT，DELETE转INSERT |

### 10.2 数据流总结

```
CDC Record -> Parsing -> Branching (新增) -> [主键表流程, 追加表流程]
    -> Primary Table Sink (支持CRUD)
    -> Append Table Sink (仅INSERT)
    -> Paimon Storage
```

### 10.3 文件修改清单

| 文件 | 修改类型 | 说明 |
|------|---------|------|
| `SyncTableActionBase.java` | 修改（方案A） | 在buildSink中添加追加表逻辑 |
| `BranchingProcessFunction.java` | 新增 | 分支算子实现 |
| `DualTableSyncAction.java` | 新增（方案B） | 独立的双表同步Action |
| `CdcActionCommonUtils.java` | 可选 | 添加追加表相关工具方法 |
