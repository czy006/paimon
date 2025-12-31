# Paimon Flink CDC Debezium数据流全局架构分析

> **文档版本**: v1.0
> **分析日期**: 2025-12-24
> **分析范围**: Paimon Flink CDC从Kafka消费Debezium数据并写入Paimon的完整流程

---

## 1. 概述

Apache Paimon的Flink CDC模块提供了从Kafka消费Debezium格式数据并实时写入Paimon数据湖的能力。整个架构基于Flink流处理框架，支持：
- **Debezium数据格式解析**：支持JSON、Avro、BSON等多种格式
- **Schema演变**：自动处理表结构变更
- **多表同步**：支持单表同步和数据库级别同步
- **Exactly-Once语义**：基于Flink Checkpoint保证数据一致性

---

## 2. 核心架构设计

### 2.1 整体架构图

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                         Paimon Flink CDC 整体架构                                     │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                     │
│  ┌──────────────┐      ┌──────────────┐      ┌──────────────┐                      │
│  │    Kafka     │──────▶│  Flink Kafka │──────▶  Debezium   │                      │
│  │   Source     │      │    Source    │      │  Deserializer│                      │
│  └──────────────┘      └──────────────┘      └──────────────┘                      │
│                                                         │                           │
│                                                         ▼                           │
│  ┌──────────────────────────────────────────────────────────────────────┐         │
│  │                      CDC Action Layer                                │         │
│  │  ┌──────────────────────────────────────────────────────────────┐   │         │
│  │  │  KafkaSyncTableAction / KafkaSyncDatabaseAction               │   │         │
│  │  │         (继承自SyncTableActionBase / SyncDatabaseActionBase)  │   │         │
│  │  └──────────────────────────────────────────────────────────────┘   │         │
│  │                              │                                       │         │
│  │                              ▼                                       │         │
│  │  ┌──────────────────────────────────────────────────────────────┐   │         │
│  │  │  SyncJobHandler (根据SourceType分发处理逻辑)                 │   │         │
│  │  │    - provideSource()      创建Kafka Source                    │   │         │
│  │  │    - provideRecordParser() 提供Record解析器                   │   │         │
│  │  │    - provideDataFormat()  提供数据格式(DebeziumJson等)        │   │         │
│  │  └──────────────────────────────────────────────────────────────┘   │         │
│  └──────────────────────────────────────────────────────────────────────┘         │
│                              │                                                     │
│                              ▼                                                     │
│  ┌──────────────────────────────────────────────────────────────────────┐         │
│  │                   Format & Parser Layer                                │         │
│  │  ┌──────────────────────────────────────────────────────────────┐   │         │
│  │  │  DataFormat (DebeziumJsonDataFormat等)                        │   │         │
│  │  │    - createKafkaDeserializer()  创建Kafka反序列化器          │   │         │
│  │  │    - createParser()              创建RecordParser            │   │         │
│  │  └──────────────────────────────────────────────────────────────┘   │         │
│  │                              │                                       │         │
│  │                              ▼                                       │         │
│  │  ┌──────────────────────────────────────────────────────────────┐   │         │
│  │  │  DebeziumJsonRecordParser (Debezium数据解析)                 │   │         │
│  │  │    - extractRecords()    提取CDC记录                         │   │         │
│  │  │    - parseSchema()       解析Schema                          │   │         │
│  │  │    - extractRowData()    提取行数据                          │   │         │
│  │  └──────────────────────────────────────────────────────────────┘   │         │
│  └──────────────────────────────────────────────────────────────────────┘         │
│                              │                                                     │
│                              ▼                                                     │
│  ┌──────────────────────────────────────────────────────────────────────┐         │
│  │                     Event Parser Layer                                 │         │
│  │  ┌──────────────────────────────────────────────────────────────┐   │         │
│  │  │  RichCdcMultiplexRecordEventParser                           │   │         │
│  │  │    - parseRecords()       解析CDC记录                        │   │         │
│  │  │    - parseSchemaChange()  解析Schema变更                     │   │         │
│  │  │    - parseNewTable()      解析新表创建                       │   │         │
│  │  └──────────────────────────────────────────────────────────────┘   │         │
│  └──────────────────────────────────────────────────────────────────────┘         │
│                              │                                                     │
│                              ▼                                                     │
│  ┌──────────────────────────────────────────────────────────────────────┐         │
│  │                  Parsing Process Function                             │         │
│  │  ┌──────────────────────────────────────────────────────────────┐   │         │
│  │  │  CdcDynamicTableParsingProcessFunction                        │   │         │
│  │  │    - DYNAMIC_OUTPUT_TAG              新增表数据输出           │   │         │
│  │  │    - DYNAMIC_SCHEMA_CHANGE_OUTPUT_TAG Schema变更输出          │   │         │
│  │  └──────────────────────────────────────────────────────────────┘   │         │
│  └──────────────────────────────────────────────────────────────────────┘         │
│                              │                                                     │
│                              ▼                                                     │
│  ┌──────────────────────────────────────────────────────────────────────┐         │
│  │                    Schema Evolution Layer                              │         │
│  │  ┌──────────────────────────────────────────────────────────────┐   │         │
│  │  │  MultiTableUpdatedDataFieldsProcessFunction                   │   │         │
│  │  │    - actualUpdatedDataFields() 计算实际Schema变更             │   │         │
│  │  │    - extractSchemaChanges()   提取SchemaChange                │   │         │
│  │  │    - applySchemaChange()     应用Schema变更                   │   │         │
│  │  └──────────────────────────────────────────────────────────────┘   │         │
│  └──────────────────────────────────────────────────────────────────────┘         │
│                              │                                                     │
│                              ▼                                                     │
│  ┌──────────────────────────────────────────────────────────────────────┐         │
│  │                       Sink Layer                                       │         │
│  │  ┌──────────────────────────────────────────────────────────────┐   │         │
│  │  │  CdcRecordStoreMultiWriteOperator                            │   │         │
│  │  │    - processElement()  处理CDC记录写入                       │   │         │
│  │  │    - toGenericRow()     转换为GenericRow                     │   │         │
│  │  │    - write()           写入StoreSinkWrite                   │   │         │
│  │  └──────────────────────────────────────────────────────────────┘   │         │
│  │                              │                                       │         │
│  │                              ▼                                       │         │
│  │  ┌──────────────────────────────────────────────────────────────┐   │         │
│  │  │  StoreMultiCommitter (多表Committer)                         │   │         │
│  │  │    - commit()          提交多表变更                          │   │         │
│  │  └──────────────────────────────────────────────────────────────┘   │         │
│  └──────────────────────────────────────────────────────────────────────┘         │
│                              │                                                     │
│                              ▼                                                     │
│  ┌──────────────┐      ┌──────────────┐      ┌──────────────┐                      │
│  │   Paimon     │──────▶│   FileStore  │──────▶│    Storage   │                      │
│  │    Table     │      │    (LSM)     │      │   (OSS/HDFS) │                      │
│  └──────────────┘      └──────────────┘      └──────────────┘                      │
│                                                                                     │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. 核心入口类分析

### 3.1 KafkaSyncTableAction (表级别同步)

**路径**: `/paimon-flink/paimon-flink-cdc/src/main/java/org/apache/paimon/flink/action/cdc/kafka/KafkaSyncTableAction.java`

**职责**: 同步单个Kafka Topic的数据到一个Paimon表

**类继承关系**:
```
KafkaSyncTableAction
    └── MessageQueueSyncTableActionBase
            └── SyncTableActionBase
                    └── SynchronizationActionBase
```

**核心功能**:
- 通过`SyncJobHandler`创建Kafka Source和RecordParser
- 从Kafka消费Debezium数据并解析
- 支持表自动创建（如果不存在）
- 支持Schema演变

### 3.2 KafkaSyncDatabaseAction (数据库级别同步)

**路径**: `/paimon-flink/paimon-flink-cdc/src/main/java/org/apache/paimon/flink/action/cdc/kafka/KafkaSyncDatabaseAction.java`

**职责**: 同步多个Kafka Topic的数据到一个Paimon数据库（多个表）

**类继承关系**:
```
KafkaSyncDatabaseAction
    └── SyncDatabaseActionBase
            └── SynchronizationActionBase
```

**核心功能**:
- 支持多Topic同步
- 表名映射和转换（前缀、后缀）
- 表过滤（include/exclude模式）
- 新表自动发现和创建

---

## 4. 数据流处理流程

### 4.1 完整数据流

```
Kafka Topic (Debezium Format)
        │
        ▼
┌─────────────────────────────────────────────────────────────┐
│ 1. Flink Kafka Source                                        │
│    - 反序列化JSON/Avro/BSON消息                              │
│    - 输出: CdcSourceRecord                                    │
└─────────────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────────────┐
│ 2. DebeziumJsonRecordParser (Record解析)                    │
│    - 解析op类型: INSERT/UPDATE/DELETE/READ                   │
│    - 提取before/after数据                                    │
│    - 解析schema信息                                          │
│    - 输出: RichCdcMultiplexRecord                            │
└─────────────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────────────┐
│ 3. CdcDynamicTableParsingProcessFunction                    │
│    - 检查新表创建 (parseNewTable)                            │
│    - 解析Schema变更 (parseSchemaChange)                      │
│    - 输出到Side Output:                                      │
│      * DYNAMIC_OUTPUT_TAG (数据)                             │
│      * DYNAMIC_SCHEMA_CHANGE_OUTPUT_TAG (Schema变更)         │
└─────────────────────────────────────────────────────────────┘
        │                          │
        ▼                          ▼
┌──────────────────┐    ┌──────────────────────────────────────┐
│ 4a. Schema       │    │ 4b. 数据流                           │
│     Evolution    │    │   CaseSensitiveUtils.cdcMultiplex... │
│     (并行度=1)   │    │   大小写转换                          │
└──────────────────┘    └──────────────────────────────────────┘
        │                          │
        ▼                          ▼
┌──────────────────┐    ┌──────────────────────────────────────┐
│ 5a. 更新Paimon   │    │ 5b. 分区                              │
│     Table Schema │    │   CdcMultiplexRecordChannelComputer  │
└──────────────────┘    └──────────────────────────────────────┘
        │                          │
        └──────────┬───────────────┘
                   ▼
┌─────────────────────────────────────────────────────────────┐
│ 6. CdcRecordStoreMultiWriteOperator                         │
│    - 获取FileStoreTable                                      │
│    - 转换为GenericRow                                        │
│    - 写入StoreSinkWrite                                     │
│    - 支持Schema变更等待和重试                                │
└─────────────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────────────┐
│ 7. StoreMultiCommitter                                      │
│    - 收集MultiTableCommittable                               │
│    - 提交到Paimon文件系统                                    │
└─────────────────────────────────────────────────────────────┘
        │
        ▼
   Paimon Storage
```

### 4.2 Schema演变处理流程

```
Debezium Schema Change Event
        │
        ▼
┌─────────────────────────────────────────────────────────────┐
│ RichCdcMultiplexRecordEventParser.parseSchemaChange()       │
│   - 比较新Schema与当前Schema                                 │
│   - 生成CdcSchema对象                                        │
└─────────────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────────────┐
│ CdcDynamicTableParsingProcessFunction                       │
│   - 输出到DYNAMIC_SCHEMA_CHANGE_OUTPUT_TAG                   │
└─────────────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────────────┐
│ MultiTableUpdatedDataFieldsProcessFunction                  │
│   - actualUpdatedDataFields() 计算实际变更字段              │
│   - extractSchemaChanges() 生成SchemaChange列表             │
│   - applySchemaChange() 应用变更到Paimon                    │
└─────────────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────────────┐
│ 支持的Schema变更类型:                                        │
│   * AddColumn        添加新列                               │
│   * AlterColumn      修改列类型(仅兼容类型)                 │
│   * DropColumn       删除列                                 │
│   * RenameColumn     重命名列                               │
│   * SetOption        设置表属性                             │
└─────────────────────────────────────────────────────────────┘
```

---

## 5. 关键类职责说明

### 5.1 Action层

| 类名 | 职责 |
|------|------|
| `KafkaSyncTableAction` | 单表同步入口，配置Kafka消费参数和Paimon表信息 |
| `KafkaSyncDatabaseAction` | 多表同步入口，支持表名映射和过滤 |
| `SyncJobHandler` | 根据SourceType分发处理逻辑，创建Source和Parser |

### 5.2 Format层

| 类名 | 职责 |
|------|------|
| `DebeziumJsonDataFormat` | Debezium JSON格式定义 |
| `DebeziumJsonRecordParser` | 解析Debezium JSON数据，提取Schema和记录 |
| `DebeziumSchemaUtils` | Debezium类型到Paimon类型转换 |

### 5.3 Parser层

| 类名 | 职责 |
|------|------|
| `RichCdcMultiplexRecordEventParser` | 多表事件解析器，处理表过滤和Schema变更检测 |
| `NewTableSchemaBuilder` | 根据源Schema构建Paimon表Schema |

### 5.4 ProcessFunction层

| 类名 | 职责 |
|------|------|
| `CdcDynamicTableParsingProcessFunction` | 动态表解析，处理新表创建和Schema变更 |
| `MultiTableUpdatedDataFieldsProcessFunction` | Schema演变处理，应用Schema变更到Paimon |

### 5.5 Sink层

| 类名 | 职责 |
|------|------|
| `FlinkCdcMultiTableSink` | 多表Sink构建器 |
| `CdcRecordStoreMultiWriteOperator` | 多表写入Operator，支持Schema变更等待 |
| `StoreMultiCommitter` | 多表提交器，原子性提交多表变更 |

---

## 6. Flink交互流程

### 6.1 Flink DAG拓扑 (Database Sync - Combined Mode)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          Flink Job DAG                                       │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  Kafka Source                                                               │
│  (并行度: N)                                                                │
│      │                                                                      │
│      │ CdcSourceRecord                                                      │
│      ▼                                                                      │
│  ┌──────────────────────────────────────────────────────────────────────┐  │
│  │ Side Output (CdcDynamicTableParsingProcessFunction)                  │  │
│  │ 并行度: N                                                              │  │
│  │                                                                       │  │
│  │  主输出: Void                                                          │  │
│  │  Side Output 1: CdcMultiplexRecord (新增表数据)                       │  │
│  │  Side Output 2: Tuple2<Identifier, CdcSchema> (Schema变更)            │  │
│  └──────────────────────────────────────────────────────────────────────┘  │
│      │                                  │                                   │
│      │                                  │ Schema Change                    │
│      │                                  ▼                                   │
│      │                        ┌─────────────────────┐                      │
│      │                        │ Schema Evolution    │                      │
│      │                        │ 并行度: 1           │                      │
│      │                        │ (MultiTableUpdated  │                      │
│      │                        │  DataFieldsProcess  │                      │
│      │                        │  Function)          │                      │
│      │                        └─────────────────────┘                      │
│      │ Data                                                                 │
│      ▼                                                                      │
│  ┌──────────────────────────────────────────────────────────────────────┐  │
│  │ Case Sensitive Convert                                                │  │
│  │ (处理大小写转换)                                                       │  │
│  └──────────────────────────────────────────────────────────────────────┘  │
│      │                                                                      │
│      ▼                                                                      │
│  ┌──────────────────────────────────────────────────────────────────────┐  │
│  │ Partition (CdcMultiplexRecordChannelComputer)                        │  │
│  │ 按Table+Partition+Bucket分区                                          │  │
│  └──────────────────────────────────────────────────────────────────────┘  │
│      │                                                                      │
│      ▼                                                                      │
│  ┌──────────────────────────────────────────────────────────────────────┐  │
│  │ CDC MultiplexWriter (CdcRecordStoreMultiWriteOperator)               │  │
│  │ 并行度: N                                                              │  │
│  │   - 维护多表Writer映射                                                  │  │
│  │   - Schema变更等待和重试                                               │  │
│  │   - 生成MultiTableCommittable                                          │  │
│  └──────────────────────────────────────────────────────────────────────┘  │
│      │                                                                      │
│      ▼                                                                      │
│  ┌──────────────────────────────────────────────────────────────────────┐  │
│  │ Partition (MultiTableCommittableChannelComputer)                     │  │
│  │ 按Table分区                                                            │  │
│  └──────────────────────────────────────────────────────────────────────┘  │
│      │                                                                      │
│      ▼                                                                      │
│  ┌──────────────────────────────────────────────────────────────────────┐  │
│  │ Global Committer (StoreMultiCommitter)                              │  │
│  │ 并行度: 1                                                              │  │
│  │   - 原子性提交多表变更                                                  │  │
│  └──────────────────────────────────────────────────────────────────────┘  │
│      │                                                                      │
│      ▼                                                                      │
│  DiscardingSink (结束)                                                     │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 6.2 Checkpoint机制

1. **Writer State**: 记录每个表的写入状态和未提交文件
2. **Committer State**: 记录已提交的Snapshot信息
3. **Exactly-Once**: 基于Flink Checkpoint实现Exactly-Once语义

---

## 7. 支持的Debezium数据格式

### 7.1 格式支持矩阵

| 格式 | 支持状态 | Factory类 | Parser类 |
|------|----------|-----------|----------|
| debezium-json | 支持 | DebeziumJsonDataFormatFactory | DebeziumJsonRecordParser |
| debezium-avro | 支持 | DebeziumAvroDataFormatFactory | DebeziumAvroRecordParser |
| debezium-bson | 支持 | DebeziumBsonDataFormatFactory | DebeziumBsonRecordParser |

### 7.2 Debezium JSON消息结构

```json
{
  "schema": {
    "fields": [{
      "field": "after",
      "fields": [{
        "field": "id",
        "type": "int32",
        "name": "field"
      }]
    }]
  },
  "payload": {
    "before": null,
    "after": {
      "id": 1,
      "name": "test"
    },
    "source": {
      "db": "mydb",
      "table": "mytable"
    },
    "op": "c",  // c=create, r=read, u=update, d=delete
    "ts_ms": 123456789
  }
}
```

---

## 8. Schema演变支持

### 8.1 支持的Schema变更类型

| 变更类型 | 支持状态 | 限制条件 |
|----------|----------|----------|
| 添加列 | 支持 | 无 |
| 修改列类型 | 部分支持 | 仅兼容类型升级 |
| 删除列 | 支持 | 无 |
| 重命名列 | 支持 | 无 |
| 修改主键 | 不支持 | - |

### 8.2 类型兼容性规则

- 字符串类型: CHAR -> VARCHAR -> TEXT (长度递增)
- 整数类型: TINYINT -> SMALLINT -> INT -> BIGINT (范围递增)
- 浮点类型: FLOAT -> DOUBLE (精度递增)
- 二进制类型: BINARY -> VARBINARY -> BLOB (长度递增)

---

## 9. 关键设计模式

### 9.1 Factory模式
- `DataFormat`通过Factory创建
- `EventParser`通过Factory创建
- 支持运行时动态格式选择

### 9.2 Strategy模式
- `SyncJobHandler`根据SourceType分发处理逻辑
- 不同数据源使用不同解析策略

### 9.3 Builder模式
- `FlinkCdcSyncDatabaseSinkBuilder`构建Sink
- `NewTableSchemaBuilder`构建Schema

### 9.4 Template Method模式
- `SyncDatabaseActionBase`定义同步流程模板
- 子类实现特定逻辑

---

## 10. 待深入分析模块

以下模块将在后续文档中详细分析：

1. **Debezium格式解析模块** - 详细分析各种Debezium格式的解析逻辑
2. **Schema演变处理模块** - 详细分析Schema变更的检测和应用机制
3. **Sink写入模块** - 详细分析多表写入和提交机制
4. **序列化与数据转换** - 详细分析数据类型转换和序列化机制

---

## 11. 参考资料

- Paimon官方文档: https://paimon.apache.org/docs/master/
- Debezium文档: https://debezium.io/documentation/
- Flink CDC文档: https://nightlies.apache.org/flink/flink-cdc-docs-stable/
