# Paimon Flink CDC Sink写入机制分析

> **文档版本**: v1.0
> **分析日期**: 2025-12-24
> **模块路径**: `/paimon-flink/paimon-flink-cdc/src/main/java/org/apache/paimon/flink/sink/cdc`

---

## 1. 概述

Paimon CDC Sink模块负责将解析后的CDC数据写入Paimon存储层。该模块支持多表写入、Schema变更等待、Exactly-Once语义等特性，是整个CDC数据同步链路的关键环节。

---

## 2. Sink写入架构

### 2.1 整体架构图

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                        Paimon CDC Sink写入架构                                       │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                     │
│  CdcMultiplexRecord (输入)                                                          │
│  ┌──────────────────────────────────────────────────────────────────────────────┐  │
│  │  - databaseName: String                                                      │  │
│  │  - tableName: String                                                         │  │
│  │  - record: CdcRecord                                                          │  │
│  │    - kind: RowKind (INSERT/UPDATE/DELETE)                                    │  │
│  │    - data: Map<String, String>                                               │  │
│  └──────────────────────────────────────────────────────────────────────────────┘  │
│                                      │                                             │
│                                      ▼                                             │
│  ┌──────────────────────────────────────────────────────────────────────────────┐  │
│  │  1. 大小写转换 (Case Sensitive Handling)                                      │  │
│  │     CaseSensitiveUtils.cdcMultiplexRecordConvert()                           │  │
│  │     - 根据Catalog配置进行大小写转换                                            │  │
│  └──────────────────────────────────────────────────────────────────────────────┘  │
│                                      │                                             │
│                                      ▼                                             │
│  ┌──────────────────────────────────────────────────────────────────────────────┐  │
│  │  2. 分区路由 (Partitioning)                                                   │  │
│  │     CdcMultiplexRecordChannelComputer                                        │  │
│  │     - 按照database.table进行分区                                              │  │
│  │     - 确保同一表的数据进入同一个Writer实例                                     │  │
│  └──────────────────────────────────────────────────────────────────────────────┘  │
│                                      │                                             │
│                                      ▼                                             │
│  ┌──────────────────────────────────────────────────────────────────────────────┐  │
│  │  3. 多表写入 (CdcRecordStoreMultiWriteOperator)                               │  │
│  │     ┌────────────────────────────────────────────────────────────────────┐   │  │
│  │     │ a. 获取Table                                                         │   │  │
│  │     │    getTable(Identifier)                                              │   │  │
│  │     │    - 从catalog获取FileStoreTable                                      │   │  │
│  │     │    - 如果不存在则等待（上游会创建）                                   │   │  │
│  │     └────────────────────────────────────────────────────────────────────┘   │  │
│  │     ┌────────────────────────────────────────────────────────────────────┐   │  │
│  │     │ b. 获取或创建Writer                                                   │   │  │
│  │     │    writes.computeIfAbsent(tableId, id -> createWriter(table))        │   │  │
│  │     │    - 每个表维护独立的Writer                                            │   │  │
│  │     └────────────────────────────────────────────────────────────────────┘   │  │
│  │     ┌────────────────────────────────────────────────────────────────────┐   │  │
│  │     │ c. Schema检查和转换                                                   │   │  │
│  │     │    toGenericRow(record, table.schema().fields())                     │   │  │
│  │     │    - 检查所有字段是否存在                                             │   │  │
│  │     │    - 类型转换: String -> Paimon类型                                   │   │  │
│  │     │    - 如果失败，等待Schema更新后重试                                   │   │  │
│  │     └────────────────────────────────────────────────────────────────────┘   │  │
│  │     ┌────────────────────────────────────────────────────────────────────┐   │  │
│  │     │ d. 写入数据                                                           │   │  │
│  │     │    write.write(genericRow)                                           │   │  │
│  │     │    - 调用StoreSinkWrite写入                                          │   │  │
│  │     └────────────────────────────────────────────────────────────────────┘   │  │
│  │     ┌────────────────────────────────────────────────────────────────────┐   │  │
│  │     │ e. 生成Committable                                                  │   │  │
│  │     │    prepareCommit()                                                  │   │  │
│  │     │    - 收集需要提交的文件                                              │   │  │
│  │     └────────────────────────────────────────────────────────────────────┘   │  │
│  └──────────────────────────────────────────────────────────────────────────────┘  │
│                                      │                                             │
│                                      ▼                                             │
│  ┌──────────────────────────────────────────────────────────────────────────────┐  │
│  │  4. 提交分区 (Repartition for Commit)                                       │  │
│  │     MultiTableCommittableChannelComputer                                     │  │
│  │     - 按Table分区，确保同一表的变更在同一Committer                           │  │
│  └──────────────────────────────────────────────────────────────────────────────┘  │
│                                      │                                             │
│                                      ▼                                             │
│  ┌──────────────────────────────────────────────────────────────────────────────┐  │
│  │  5. 多表提交 (StoreMultiCommitter)                                           │  │
│  │     ┌────────────────────────────────────────────────────────────────────┐   │  │
│  │     │ a. 按Table分组Committable                                            │   │  │
│  │     └────────────────────────────────────────────────────────────────────┘   │  │
│  │     ┌────────────────────────────────────────────────────────────────────┐   │  │
│  │     │ b. 获取或创建TableCommitter                                          │   │  │
│  │     │    committers.computeIfAbsent(tableId, id -> createCommitter(table))  │   │  │
│  │     └────────────────────────────────────────────────────────────────────┘   │  │
│  │     ┌────────────────────────────────────────────────────────────────────┐   │  │
│  │     │ c. 提交变更                                                           │   │  │
│  │     │    committer.commit(committables)                                    │   │  │
│  │     │    - 原子性提交多表变更                                               │   │  │
│  │     └────────────────────────────────────────────────────────────────────┘   │  │
│  │     ┌────────────────────────────────────────────────────────────────────┐   │  │
│  │     │ d. 更新元数据                                                         │   │  │
│  │     │    - 更新Snapshot信息                                                │   │  │
│  │     │    - 触发Compaction（如果需要）                                       │   │  │
│  │     └────────────────────────────────────────────────────────────────────┘   │  │
│  └──────────────────────────────────────────────────────────────────────────────┘  │
│                                      │                                             │
│                                      ▼                                             │
│   Paimon Storage (HDFS/OSS/S3)                                                     │
│                                                                                     │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

### 2.2 核心类说明

| 类名 | 职责 |
|------|------|
| `CdcRecord` | CDC数据记录，包含操作类型和数据 |
| `CdcMultiplexRecord` | 多路复用CDC记录，包含database和table信息 |
| `CdcRecordStoreMultiWriteOperator` | 多表写入Operator，维护多表Writer映射 |
| `StoreSinkWrite` | 单表写入接口 |
| `StoreMultiCommitter` | 多表提交器 |
| `MultiTableCommittable` | 多表可提交对象 |
| `CdcRecordUtils` | CDC记录转换工具类 |

---

## 3. 数据记录结构

### 3.1 CdcRecord

```java
public class CdcRecord {
    private final RowKind kind;           // 操作类型: INSERT/UPDATE/DELETE
    private final Map<String, String> data;  // 字段名 -> 字符串值

    public CdcRecord(RowKind kind, Map<String, String> data) {
        this.kind = kind;
        this.data = data;
    }

    public RowKind kind() { return kind; }
    public Map<String, String> data() { return data; }
}
```

### 3.2 CdcMultiplexRecord

```java
public class CdcMultiplexRecord {
    private final String databaseName;   // 数据库名
    private final String tableName;      // 表名
    private final CdcRecord record;      // CDC记录

    public CdcMultiplexRecord(String databaseName, String tableName, CdcRecord record) {
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.record = record;
    }
}
```

### 3.3 RichCdcMultiplexRecord

```java
public class RichCdcMultiplexRecord extends CdcMultiplexRecord {
    private final CdcSchema cdcSchema;  // Schema信息

    public RichCdcMultiplexRecord(
            String databaseName,
            String tableName,
            CdcRecord record,
            CdcSchema cdcSchema) {
        super(databaseName, tableName, record);
        this.cdcSchema = cdcSchema;
    }
}
```

---

## 4. 多表写入机制

### 4.1 CdcRecordStoreMultiWriteOperator核心逻辑

**位置**: `CdcRecordStoreMultiWriteOperator.java:122-189`

```java
@Override
public void processElement(StreamRecord<CdcMultiplexRecord> element) throws Exception {
    CdcMultiplexRecord record = element.getValue();

    String databaseName = record.databaseName();
    String tableName = record.tableName();
    Identifier tableId = Identifier.create(databaseName, tableName);

    // 1. 获取FileStoreTable
    FileStoreTable table = getTable(tableId);

    // 2. 获取或创建Writer
    StoreSinkWrite write = writes.computeIfAbsent(tableId, id ->
        storeSinkWriteProvider.provide(
            table,
            commitUser,
            state,
            getContainingTask().getEnvironment().getIOManager(),
            memoryPoolFactory,
            getMetricGroup()
        )
    );

    // 3. 配置Compaction执行器
    ((StoreSinkWriteImpl) write).withCompactExecutor(compactExecutor);

    // 4. 转换为GenericRow
    int retryCnt = table.coreOptions().toConfiguration().get(MAX_RETRY_NUM_TIMES);
    boolean skipCorruptRecord = table.coreOptions().toConfiguration().get(SKIP_CORRUPT_RECORD);
    boolean logCorruptRecord = table.coreOptions().toConfiguration().get(LOG_CORRUPT_RECORD);

    Optional<GenericRow> optionalConverted =
        toGenericRow(record.record(), table.schema().fields(), logCorruptRecord);

    // 5. 如果转换失败，等待Schema更新后重试
    if (!optionalConverted.isPresent()) {
        FileStoreTable latestTable = table;
        for (int retry = 0; retry < retryCnt; ++retry) {
            latestTable = latestTable.copyWithLatestSchema();
            tables.put(tableId, latestTable);
            optionalConverted =
                toGenericRow(record.record(), latestTable.schema().fields(), logCorruptRecord);
            if (optionalConverted.isPresent()) {
                break;
            }
            Thread.sleep(latestTable.coreOptions().toConfiguration().get(RETRY_SLEEP_TIME).toMillis());
        }
        write.replace(latestTable);
    }

    // 6. 写入数据或跳过
    if (!optionalConverted.isPresent()) {
        if (skipCorruptRecord) {
            LOG.warn("Skipping corrupt or unparsable record {}", record);
        } else {
            throw new RuntimeException("Unable to process element. Possibly a corrupt record: " + record);
        }
    } else {
        try {
            write.write(optionalConverted.get());
        } catch (Exception e) {
            throw new IOException(e);
        }
    }
}
```

### 4.2 getTable方法 - Table获取与等待

**位置**: `CdcRecordStoreMultiWriteOperator.java:191-214`

```java
private FileStoreTable getTable(Identifier tableId) throws InterruptedException {
    FileStoreTable table = tables.get(tableId);
    if (table == null) {
        while (true) {
            try {
                table = (FileStoreTable) catalog.getTable(tableId);
                tables.put(tableId, table);
                break;
            } catch (Catalog.TableNotExistException e) {
                // 表不存在，等待上游创建
                // 新表会由CdcDynamicTableParsingProcessFunction创建
            }
            Thread.sleep(RETRY_SLEEP_TIME.defaultValue().toMillis());
        }
    }

    // 检查Bucket模式
    if (table.bucketMode() != BucketMode.HASH_FIXED) {
        throw new UnsupportedOperationException(
            String.format("Combine mode Sink only supports FIXED bucket mode, but %s is %s",
                table.name(), table.bucketMode()));
    }
    return table;
}
```

### 4.3 prepareCommit方法 - 生成Committable

**位置**: `CdcRecordStoreMultiWriteOperator.java:242-261`

```java
@Override
protected List<MultiTableCommittable> prepareCommit(boolean waitCompaction, long checkpointId)
        throws IOException {
    List<MultiTableCommittable> committables = new LinkedList<>();

    // 遍历所有表的Writer
    for (Map.Entry<Identifier, StoreSinkWrite> entry : writes.entrySet()) {
        Identifier key = entry.getKey();
        StoreSinkWrite write = entry.getValue();
        try {
            // 收集每个表的Committable
            committables.addAll(
                write.prepareCommit(waitCompaction, checkpointId).stream()
                    .map(committable ->
                        MultiTableCommittable.fromCommittable(key, committable))
                    .collect(Collectors.toList())
            );
        } catch (Exception e) {
            throw new IOException("Failed to prepare commit for table: " + key.toString(), e);
        }
    }
    return committables;
}
```

---

## 5. 数据类型转换

### 5.1 toGenericRow方法

**位置**: `CdcRecordUtils.java:82-118`

```java
public static Optional<GenericRow> toGenericRow(
        CdcRecord record, List<DataField> dataFields, boolean logCorruptRecord) {
    // 1. 创建GenericRow，保留RowKind
    GenericRow genericRow = new GenericRow(record.kind(), dataFields.size());
    List<String> fieldNames = dataFields.stream()
        .map(DataField::name)
        .collect(Collectors.toList());

    // 2. 遍历CDC记录的每个字段
    for (Map.Entry<String, String> field : record.data().entrySet()) {
        String key = field.getKey();
        String value = field.getValue();

        // 3. 查找字段在目标Schema中的位置
        int idx = fieldNames.indexOf(key);
        if (idx < 0) {
            // 字段不存在，等待Schema更新
            LOG.info("Field '{}' not found. Waiting for schema update.", key);
            return Optional.empty();
        }

        if (value == null) {
            continue;
        }

        // 4. 类型转换: String -> Paimon类型
        DataType type = dataFields.get(idx).type();
        try {
            genericRow.setField(idx, TypeUtils.castFromCdcValueString(value, type));
        } catch (Exception e) {
            // 类型转换失败，等待Schema更新
            LOG.info("Failed to convert field '{}' value {} to type {}. Waiting for schema update.",
                key, logCorruptRecord ? value : "<redacted>", type, e);
            return Optional.empty();
        }
    }
    return Optional.of(genericRow);
}
```

### 5.2 castFromCdcValueString类型转换

```java
// TypeUtils.castFromCdcValueString 核心逻辑
public static Object castFromCdcValueString(String value, DataType dataType) {
    switch (dataType.getTypeRoot()) {
        case CHAR:
        case VARCHAR:
            return value;  // 字符串直接返回
        case BOOLEAN:
            return Boolean.parseBoolean(value);
        case TINYINT:
            return Byte.parseByte(value);
        case SMALLINT:
            return Short.parseShort(value);
        case INTEGER:
            return Integer.parseInt(value);
        case BIGINT:
            return Long.parseLong(value);
        case FLOAT:
            return Float.parseFloat(value);
        case DOUBLE:
            return Double.parseDouble(value);
        case DECIMAL:
            return new BigDecimal(value);
        case DATE:
            return LocalDate.parse(value);  // 格式: yyyy-MM-dd
        case TIMESTAMP:
            return Timestamp.valueOf(value);  // 格式: yyyy-MM-dd HH:mm:ss.SSS
        case ARRAY:
            return JSONArray.parseArray(value);  // JSON数组
        case MAP:
            return JSON.parseObject(value);  // JSON对象
        default:
            return value;
    }
}
```

---

## 6. Schema变更等待机制

### 6.1 等待重试流程

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                          Schema变更等待流程                                          │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                     │
│  toGenericRow()转换失败                                                             │
│  ┌──────────────────────────────────────────────────────────────────────────────┐  │
│  │ 原因:                                                                         │  │
│  │ 1. 字段不存在: Field 'xxx' not found                                         │  │
│  │ 2. 类型转换失败: Failed to convert field 'xxx' value 'yyy' to type INT      │  │
│  └──────────────────────────────────────────────────────────────────────────────┘  │
│                                      │                                             │
│                                      ▼                                             │
│  进入重试循环                                                                      │
│  ┌──────────────────────────────────────────────────────────────────────────────┐  │
│  │  for (retry = 0; retry < MAX_RETRY_NUM_TIMES; retry++) {                     │  │
│  │      // 1. 获取最新Schema                                                     │  │
│  │      latestTable = table.copyWithLatestSchema();                             │  │
│  │                                                                               │  │
│  │      // 2. 重新尝试转换                                                       │  │
│  │      optionalConverted = toGenericRow(record, latestTable.schema(), ...);    │  │
│  │                                                                               │  │
│  │      // 3. 成功则退出                                                         │  │
│  │      if (optionalConverted.isPresent()) break;                               │  │
│  │                                                                               │  │
│  │      // 4. 等待一段时间                                                      │  │
│  │      Thread.sleep(RETRY_SLEEP_TIME);                                         │  │
│  │  }                                                                            │  │
│  └──────────────────────────────────────────────────────────────────────────────┘  │
│                                      │                                             │
│                                      ▼                                             │
│  检查重试结果                                                                      │
│  ┌──────────────────────────────────────────────────────────────────────────────┐  │
│  │  if (optionalConverted.isPresent()) {                                        │  │
│  │      // 成功: 使用新Schema                                                    │  │
│  │      write.replace(latestTable);                                             │  │
│  │      write.write(genericRow);                                                │  │
│  │  } else {                                                                     │  │
│  │      // 失败: 根据配置决定                                                    │  │
│  │      if (skipCorruptRecord) {                                                │  │
│  │          LOG.warn("Skipping corrupt record");                                │  │
│  │      } else {                                                                 │  │
│  │          throw new RuntimeException("Unable to process element");             │  │
│  │      }                                                                        │  │
│  │  }                                                                            │  │
│  └──────────────────────────────────────────────────────────────────────────────┘  │
│                                                                                     │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

### 6.2 配置参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `sink.cdc.schema-update.retry-num-times` | 3 | Schema更新重试次数 |
| `sink.cdc.schema-update.retry-sleep-time` | 1s | 重试间隔时间 |
| `sink.cdc.skip-corrupt-record` | false | 是否跳过无法解析的记录 |
| `sink.cdc.log-corrupt-record` | false | 是否记录损坏记录的详细信息 |

---

## 7. 多表提交机制

### 7.1 StoreMultiCommitter核心逻辑

```java
public class StoreMultiCommitter implements Committer<MultiTableCommittable, WrappedManifestCommittable> {
    private final CatalogLoader catalogLoader;
    private final boolean eagerInit;
    private final TableFilter tableFilter;

    private final Map<Identifier, StoreCommitter> committers = new HashMap<>();

    @Override
    public void commit(List<MultiTableCommittable> committables) throws Exception {
        // 1. 按Table分组
        Map<Identifier, List<Committable>> grouped = committables.stream()
            .collect(Collectors.groupingBy(
                MultiTableCommittable::table,
                Collectors.mapping(MultiTableCommittable::committable, Collectors.toList())
            ));

        // 2. 为每个表获取或创建Committer
        for (Map.Entry<Identifier, List<Committable>> entry : grouped.entrySet()) {
            Identifier tableId = entry.getKey();
            List<Committable> tableCommittables = entry.getValue();

            StoreCommitter committer = committers.computeIfAbsent(tableId, id -> {
                try {
                    FileStoreTable table = (FileStoreTable) catalog.getTable(id);
                    return new StoreCommitter(table, false, eagerInit);
                } catch (Exception e) {
                    throw new RuntimeException("Failed to create committer for table: " + id, e);
                }
            });

            // 3. 提交变更
            committer.commit(tableCommittables);
        }
    }
}
```

### 7.2 提交流程

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                          多表提交流程                                                 │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                     │
│  MultiTableCommittable[] (输入)                                                    │
│  ┌──────────────────────────────────────────────────────────────────────────────┐  │
│  │  [                                                                            │  │
│  │    MultiTableCommittable(table1, committable1),                              │  │
│  │    MultiTableCommittable(table1, committable2),                              │  │
│  │    MultiTableCommittable(table2, committable3),                              │  │
│  │    MultiTableCommittable(table3, committable4),                              │  │
│  │    ...                                                                        │  │
│  │  ]                                                                            │  │
│  └──────────────────────────────────────────────────────────────────────────────┘  │
│                                      │                                             │
│                                      ▼                                             │
│  按Table分组                                                                        │
│  ┌──────────────────────────────────────────────────────────────────────────────┐  │
│  │  table1: [committable1, committable2]                                        │  │
│  │  table2: [committable3]                                                      │  │
│  │  table3: [committable4]                                                      │  │
│  └──────────────────────────────────────────────────────────────────────────────┘  │
│                                      │                                             │
│                                      ▼                                             │
│  为每个表创建或获取Committer                                                         │
│  ┌──────────────────────────────────────────────────────────────────────────────┐  │
│  │  committers.computeIfAbsent(tableId, id -> createCommitter(table))            │  │
│  │                                                                               │  │
│  │  maintance:                                                                   │  │
│  │    table1 -> StoreCommitter1                                                 │  │
│  │    table2 -> StoreCommitter2                                                 │  │
│  │    table3 -> StoreCommitter3                                                 │  │
│  └──────────────────────────────────────────────────────────────────────────────┘  │
│                                      │                                             │
│                                      ▼                                             │
│  并行提交每个表的变更                                                                │
│  ┌──────────────────────────────────────────────────────────────────────────────┐  │
│  │  for each table:                                                            │  │
│  │    committer.commit(committables)                                           │  │
│  │      1. 收集需要提交的文件                                                    │  │
│  │      2. 更新Manifest                                                         │  │
│  │      3. 触发Compaction（如果需要）                                            │  │
│  │      4. 清理过期文件                                                         │  │
│  └──────────────────────────────────────────────────────────────────────────────┘  │
│                                      │                                             │
│                                      ▼                                             │
│  提交完成                                                                          │
│  ┌──────────────────────────────────────────────────────────────────────────────┐  │
│  │  - 所有表的变更已持久化到文件系统                                              │  │
│  │  - 元数据已更新                                                              │  │
│  │  - 下次Checkpoint可以从此Snapshot恢复                                         │  │
│  └──────────────────────────────────────────────────────────────────────────────┘  │
│                                                                                     │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 8. Exactly-Once语义保证

### 8.1 Checkpoint机制

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                        Exactly-Once语义保证机制                                      │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                     │
│  Flink Checkpoint触发                                                               │
│  ┌──────────────────────────────────────────────────────────────────────────────┐  │
│  │  1. checkpointBegin()                                                        │  │
│  │     - 暂停接收新数据                                                          │  │
│  └──────────────────────────────────────────────────────────────────────────────┘  │
│                                      │                                             │
│                                      ▼                                             │
│  ┌──────────────────────────────────────────────────────────────────────────────┐  │
│  │  2. snapshotState()                                                         │  │
│  │     CdcRecordStoreMultiWriteOperator:                                        │  │
│  │       - snapshot writer states (未提交的文件列表)                              │  │
│  │       - snapshot tables (表引用)                                              │  │
│  │     Global Committer:                                                         │  │
│  │       - snapshot 已提交的Snapshot信息                                          │  │
│  └──────────────────────────────────────────────────────────────────────────────┘  │
│                                      │                                             │
│                                      ▼                                             │
│  ┌──────────────────────────────────────────────────────────────────────────────┐  │
│  │  3. prepareCommit()                                                         │  │
│  │     - 收集所有Writer的未提交文件                                              │  │
│  │     - 生成MultiTableCommittable列表                                          │  │
│  └──────────────────────────────────────────────────────────────────────────────┘  │
│                                      │                                             │
│                                      ▼                                             │
│  ┌──────────────────────────────────────────────────────────────────────────────┐  │
│  │  4. commit()                                                                │  │
│  │     - 原子性提交所有表的变更                                                  │  │
│  │     - 更新Snapshot                                                          │  │
│  └──────────────────────────────────────────────────────────────────────────────┘  │
│                                      │                                             │
│                                      ▼                                             │
│  ┌──────────────────────────────────────────────────────────────────────────────┐  │
│  │  5. checkpointComplete()                                                    │  │
│  │     - 清理已提交的文件状态                                                    │  │
│  │     - 恢复接收新数据                                                          │  │
│  └──────────────────────────────────────────────────────────────────────────────┘  │
│                                                                                     │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

### 8.2 状态管理

```java
// Writer状态
StoreSinkWriteState state = new StoreSinkWriteStateImpl(
    subtaskIndex,
    context,
    (tableName, partition, bucket) -> true  // 过滤条件
);

// 状态包含:
// - 写入中的文件
// - 未提交的文件
// - Checkpoint ID

// Committer状态
CommittableStateManager<WrappedManifestCommittable> stateManager =
    new RestoreAndFailCommittableStateManager<>(
        WrappedManifestCommittableSerializer::new,
        true
    );

// 状态包含:
// - 已提交的Snapshot列表
// - 当前Checkpoint ID
```

---

## 9. Bucket模式支持

### 9.1 支持的Bucket模式

| Bucket模式 | CDC支持 | 说明 |
|------------|---------|------|
| `HASH_FIXED` | 支持 | 默认模式，Bucket数量固定 |
| `HASH_DYNAMIC` | 部分支持 | 需要使用CdcDynamicBucketSink |
| `BUCKET_UNAWARE` | 支持 | 无主键表使用CdcAppendTableSink |
| `POSTPONE_MODE` | 支持 | 延迟Bucket分配 |
| `KEY_DYNAMIC` | 不支持 | - |

### 9.2 Fixed Bucket模式

```java
if (table.bucketMode() == BucketMode.HASH_FIXED) {
    // 使用CdcFixedBucketSink
    DataStream<CdcRecord> partitioned = partition(
        parsed,
        new CdcRecordChannelComputer(table.schema()),
        parallelism
    );
    new CdcFixedBucketSink(table).sinkFrom(partitioned);
}
```

### 9.3 Bucket路由

```java
// CdcRecordChannelComputer - 根据主键计算Bucket
public int channel(CdcRecord record) {
    Object[] keyFields = new Object[primaryKeys.size()];
    for (int i = 0; i < primaryKeys.size(); i++) {
        keyFields[i] = record.data().get(primaryKeys.get(i));
    }
    int bucket = BucketComputer.bucket(keyFields, numBuckets);
    return bucket % numChannels;
}
```

---

## 10. 性能优化

### 10.1 Writer缓存

```java
// 每个表维护独立的Writer实例
private Map<Identifier, StoreSinkWrite> writes = new HashMap<>();

Writer write = writes.computeIfAbsent(tableId, id -> createWriter(table));
```

**优势**:
- 避免重复创建Writer
- 复用内存池和IOManager
- 减少元数据查询

### 10.2 Table引用缓存

```java
private Map<Identifier, FileStoreTable> tables = new HashMap<>();

FileStoreTable table = tables.get(tableId);
if (table == null) {
    table = catalog.getTable(tableId);
    tables.put(tableId, table);
}
```

### 10.3 异步Compaction

```java
// 配置专用线程执行Compaction
ExecutorService compactExecutor = Executors.newSingleThreadScheduledExecutor(
    new ExecutorThreadFactory(Thread.currentThread().getName() + "-Compaction")
);

((StoreSinkWriteImpl) write).withCompactExecutor(compactExecutor);
```

**优势**:
- Compaction不阻塞写入
- 提高写入吞吐量
- 降低延迟

---

## 11. 错误处理

### 11.1 损坏记录处理

```java
if (!optionalConverted.isPresent()) {
    if (skipCorruptRecord) {
        LOG.warn("Skipping corrupt or unparsable record {}", record);
    } else {
        throw new RuntimeException(
            "Unable to process element. Possibly a corrupt record: " + record
        );
    }
}
```

### 11.2 表不存在处理

```java
while (true) {
    try {
        table = (FileStoreTable) catalog.getTable(tableId);
        break;
    } catch (Catalog.TableNotExistException e) {
        // 等待上游创建表
        Thread.sleep(RETRY_SLEEP_TIME.defaultValue().toMillis());
    }
}
```

### 11.3 Schema不兼容处理

```java
try {
    genericRow.setField(idx, TypeUtils.castFromCdcValueString(value, type));
} catch (Exception e) {
    LOG.info("Failed to convert field '{}' value {} to type {}. Waiting for schema update.",
        key, value, type, e);
    return Optional.empty();  // 触发Schema更新等待
}
```

---

## 12. 总结

Paimon CDC Sink写入机制的核心特点：

1. **多表写入**: 单个Operator支持多表写入，维护Table->Writer映射
2. **Schema等待**: 自动检测Schema变更并等待更新
3. **Exactly-Once**: 基于Flink Checkpoint保证数据一致性
4. **Bucket路由**: 按照主键计算Bucket，确保数据正确分布
5. **错误容错**: 支持重试、跳过损坏记录等容错机制

**完整数据流**:
```
CdcMultiplexRecord -> Case Convert -> Partition
    -> CdcRecordStoreMultiWriteOperator
    -> toGenericRow (with Schema wait)
    -> StoreSinkWrite.write()
    -> prepareCommit() -> MultiTableCommittable
    -> StoreMultiCommitter.commit()
    -> Paimon Storage
```
