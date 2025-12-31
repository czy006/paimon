# Paimon Flink CDC Debezium格式解析模块分析

> **文档版本**: v1.0
> **分析日期**: 2025-12-24
> **模块路径**: `/paimon-flink/paimon-flink-cdc/src/main/java/org/apache/paimon/flink/action/cdc/format/debezium`

---

## 1. 概述

Debezium格式解析模块负责将从Kafka消费的Debezium格式数据（JSON/Avro/BSON）解析为Paimon内部可处理的数据结构。该模块是整个CDC流程中的关键环节，直接影响数据同步的正确性和性能。

---

## 2. 模块架构

### 2.1 类结构图

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                     Debezium Format 解析模块架构                                     │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                     │
│  ┌──────────────────────────────────────────────────────────────────────────────┐  │
│  │                        DataFormat (接口层)                                  │  │
│  │  ┌────────────────────────────────────────────────────────────────────────┐  │  │
│  │  │  AbstractJsonDataFormat                                                 │  │  │
│  │  │    - createKafkaDeserializer()                                          │  │  │
│  │  │    - createParser()                                                     │  │  │
│  │  └────────────────────────────────────────────────────────────────────────┘  │  │
│  │                              ▲                                               │  │
│  │                              │ 继承                                          │  │  │
│  │  ┌────────────────────────────────────────────────────────────────────────┐  │  │
│  │  │  DebeziumJsonDataFormat                                                 │  │  │
│  │  │  DebeziumAvroDataFormat                                                 │  │  │
│  │  │  DebeziumBsonDataFormat                                                 │  │  │
│  │  └────────────────────────────────────────────────────────────────────────┘  │  │
│  └──────────────────────────────────────────────────────────────────────────────┘  │
│                              │ 创建Parser                                         │
│                              ▼                                                     │
│  ┌──────────────────────────────────────────────────────────────────────────────┐  │
│  │                    RecordParser (解析层)                                     │  │
│  │  ┌────────────────────────────────────────────────────────────────────────┐  │  │
│  │  │  AbstractRecordParser                                                   │  │  │
│  │  │    - setRawEvent()                                                      │  │  │
│  │  │    - extractRecords()                                                   │  │  │
│  │  │    - extractPrimaryKeys()                                               │  │  │
│  │  │    - getTableName() / getDatabaseName()                                 │  │  │
│  │  └────────────────────────────────────────────────────────────────────────┘  │  │
│  │                              ▲                                               │  │
│  │                              │                                               │  │
│  │  ┌────────────────────────────────────────────────────────────────────────┐  │  │
│  │  │  AbstractJsonRecordParser                                              │  │  │
│  │  │    - extractRowData()       提取行数据                                  │  │  │
│  │  │    - fillDefaultTypes()    填充默认类型                                │  │  │
│  │  │    - processRecord()       处理记录                                    │  │  │
│  │  └────────────────────────────────────────────────────────────────────────┘  │  │
│  │                              ▲                                               │  │
│  │                              │                                               │  │
│  │  ┌────────────────────────────────────────────────────────────────────────┐  │  │
│  │  │  DebeziumJsonRecordParser                                              │  │  │
│  │  │    - extractRecords()       根据op类型分发处理                          │  │  │
│  │  │    - parseSchema()          解析schema字段                              │  │  │
│  │  │    - extractRowData()       使用schema信息提取数据                      │  │  │
│  │  └────────────────────────────────────────────────────────────────────────┘  │  │
│  │  ┌────────────────────────────────────────────────────────────────────────┐  │  │
│  │  │  DebeziumAvroRecordParser                                              │  │  │
│  │  │  DebeziumBsonRecordParser                                              │  │  │
│  │  └────────────────────────────────────────────────────────────────────────┘  │  │
│  └──────────────────────────────────────────────────────────────────────────────┘  │
│                              │                                                   │
│                              ▼                                                     │
│  ┌──────────────────────────────────────────────────────────────────────────────┐  │
│  │                    DebeziumSchemaUtils (工具类)                              │  │
│  │  ┌────────────────────────────────────────────────────────────────────────┐  │  │
│  │  │  toDataType()                  Debezium类型 -> Paimon类型               │  │  │
│  │  │  transformRawValue()           原始值转换                               │  │  │
│  │  │  avroToPaimonDataType()        Avro类型 -> Paimon类型                  │  │  │
│  │  │  convertAvroObjectToJsonCompatible()  Avro对象 -> JSON兼容对象          │  │  │
│  │  └────────────────────────────────────────────────────────────────────────┘  │  │
│  └──────────────────────────────────────────────────────────────────────────────┘  │
│                                                                                     │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

### 2.2 核心类说明

| 类名 | 文件路径 | 职责 |
|------|----------|------|
| `DebeziumJsonDataFormat` | `debezium/DebeziumJsonDataFormat.java` | Debezium JSON格式定义 |
| `DebeziumJsonRecordParser` | `debezium/DebeziumJsonRecordParser.java` | Debezium JSON解析器 |
| `DebeziumSchemaUtils` | `debezium/DebeziumSchemaUtils.java` | Debezium类型转换工具类 |
| `AbstractJsonRecordParser` | `format/AbstractJsonRecordParser.java` | JSON解析器基类 |
| `AbstractRecordParser` | `format/AbstractRecordParser.java` | 解析器抽象基类 |

---

## 3. Debezium JSON格式解析流程

### 3.1 完整解析流程

```
Kafka Message (JSON String)
        │
        ▼
┌─────────────────────────────────────────────────────────────────────────────────────┐
│ 1. Flink Kafka Deserialization                                                      │
│    JsonDebeziumDeserializationSchema.deserialize()                                   │
│    输出: CdcSourceRecord (包含JsonNode)                                             │
└─────────────────────────────────────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────────────────────────────────────┐
│ 2. DebeziumJsonRecordParser.setRoot()                                              │
│    - 检查是否存在schema字段                                                          │
│    - 如果存在，提取payload并解析schema                                               │
│    - 如果不存在，直接使用整个消息                                                    │
└─────────────────────────────────────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────────────────────────────────────┐
│ 3. DebeziumJsonRecordParser.parseSchema() (如果hasSchema)                          │
│    - 解析schema.fields数组                                                          │
│    - 找到after/before字段的field定义                                                 │
│    - 提取每个字段的type, name, parameters                                           │
│    - 存储到:                                                                        │
│      * debeziumTypes: Map<String, String>       字段名 -> debezium类型               │
│      * classNames: Map<String, String>         字段名 -> Java类名                   │
│      * parameters: Map<String, Map<String, String>> 字段名 -> 参数                   │
└─────────────────────────────────────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────────────────────────────────────┐
│ 4. DebeziumJsonRecordParser.extractRecords()                                       │
│    - 获取op字段                                                                     │
│    - 根据op类型分发:                                                                 │
│      * c/r (INSERT/READ) -> processRecord(after, INSERT)                            │
│      * u (UPDATE)         -> processRecord(after, INSERT) + processRecord(old, DELETE)│
│      * d (DELETE)         -> processRecord(before, DELETE)                          │
└─────────────────────────────────────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────────────────────────────────────┐
│ 5. DebeziumJsonRecordParser.extractRowData()                                       │
│    if (hasSchema) {                                                                 │
│      - 遍历每个字段                                                                 │
│      - 调用transformRawValue()进行值转换                                            │
│      - 调用toDataType()获取Paimon类型                                               │
│      - 添加到schemaBuilder                                                         │
│    } else {                                                                         │
│      - 使用fillDefaultTypes()填充STRING类型                                         │
│      - 直接调用toString()转换值                                                     │
│    }                                                                                │
│    - evalComputedColumns() 计算计算列                                              │
└─────────────────────────────────────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────────────────────────────────────┐
│ 6. 创建 RichCdcMultiplexRecord                                                      │
│    - databaseName: source.db                                                       │
│    - tableName: source.table                                                       │
│    - rowData: Map<String, String> 转换后的数据                                      │
│    - schema: CdcSchema                                                             │
│    - primaryKeys: pkNames                                                          │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

### 3.2 操作类型处理矩阵

| Op Type | 操作 | Before字段 | After字段 | 生成记录 |
|---------|------|------------|-----------|----------|
| `c` | CREATE | null | 新数据 | 1条 INSERT |
| `r` | READ (快照) | null | 数据 | 1条 INSERT |
| `u` | UPDATE | 旧数据 | 新数据 | 1条 DELETE + 1条 INSERT |
| `d` | DELETE | 旧数据 | null | 1条 DELETE |
| `t` | TRUNCATE | - | - | 忽略 |
| `m` | MESSAGE | - | - | 忽略 |

---

## 4. DebeziumSchemaUtils 类型转换详解

### 4.1 类型转换流程图

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                        Debezium类型到Paimon类型转换                                 │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                     │
│  Debezium Type                                                                      │
│  ┌──────────────────────────────────────────────────────────────────────────────┐  │
│  │                                                                               │  │
│  │  输入参数:                                                                    │  │
│  │  - debeziumType: String (如 "int32", "bytes", "string")                        │  │
│  │  - className: String (如 "org.apache.kafka.connect.data.Decimal")              │  │
│  │  - parameters: Map<String, String> (如 {"scale": "2", "precision": "10"})     │  │
│  │                                                                               │  │
│  └──────────────────────────────────────────────────────────────────────────────┘  │
│                                      │                                             │
│                                      ▼                                             │
│  ┌──────────────────────────────────────────────────────────────────────────────┐  │
│  │                                                                               │  │
│  │  if (className == null) {                                                     │  │
│  │      return fromDebeziumType(debeziumType);     // 基础类型转换               │  │
│  │  } else {                                                                     │  │
│  │      // 逻辑类型转换 (根据className)                                          │  │
│  │  }                                                                            │  │
│  │                                                                               │  │
│  └──────────────────────────────────────────────────────────────────────────────┘  │
│                                      │                                             │
│                ┌─────────────────────┼─────────────────────┐                       │
│                ▼                     ▼                     ▼                       │
│  ┌───────────────────┐   ┌───────────────────┐   ┌───────────────────┐            │
│  │  基础类型          │   │  逻辑类型          │   │  Decimal类型       │            │
│  │                   │   │                   │   │                   │            │
│  │ int8  -> TINYINT  │   │ Date -> DATE      │   │ Decimal(p,s) ->   │            │
│  │ int16 -> SMALLINT │   │ Timestamp ->      │   │   DECIMAL(p,s)    │            │
│  │ int32 -> INT      │   │   TIMESTAMP(3)    │   │                   │            │
│  │ int64 -> BIGINT   │   │ MicroTimestamp -> │   │ 如果p > 38:       │            │
│  │ float -> FLOAT    │   │   TIMESTAMP(6)    │   │   -> STRING       │            │
│  │ double-> DOUBLE   │   │ ZonedTimestamp -> │   │                   │            │
│  │ bytes -> BYTES    │   │   TIMESTAMP(6)    │   │                   │            │
│  │ string-> STRING   │   │ MicroTime -> TIME │   │                   │            │
│  │                   │   │ Bits -> BINARY    │   │                   │            │
│  │                   │   │ Point/Geometry -> │   │                   │            │
│  │                   │   │   STRING (JSON)   │   │                   │            │
│  └───────────────────┘   └───────────────────┘   └───────────────────┘            │
│                                                                                     │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

### 4.2 基础类型映射表

| Debezium Type | Paimon DataType | 说明 |
|---------------|-----------------|------|
| `int8` | `TINYINT` | 8位整数 |
| `int16` | `SMALLINT` | 16位整数 |
| `int32` | `INT` | 32位整数 |
| `int64` | `BIGINT` | 64位整数 |
| `float`/`float32`/`float64` | `FLOAT` | 单精度浮点 |
| `double` | `DOUBLE` | 双精度浮点 |
| `boolean` | `BOOLEAN` | 布尔值 |
| `bytes` | `BYTES` | 字节数组 |
| `string` | `STRING` | 字符串 |

### 4.3 逻辑类型映射表

| ClassName (io.debezium.*) | Paimon DataType | 说明 |
|---------------------------|-----------------|------|
| `io.debezium.time.Date` | `DATE` | 日期 |
| `io.debezium.time.Timestamp` | `TIMESTAMP(3)` | 时间戳(毫秒精度) |
| `io.debezium.time.MicroTimestamp` | `TIMESTAMP(6)` | 时间戳(微秒精度) |
| `io.debezium.time.ZonedTimestamp` | `TIMESTAMP(6)` | 带时区时间戳 |
| `io.debezium.time.MicroTime` | `TIME` | 时间(微秒精度) |
| `io.debezium.data.Bits` | `BINARY((length+7)/8)` | 位类型 |
| `org.apache.kafka.connect.data.Decimal` | `DECIMAL(p, s)` | 高精度小数 |
| `io.debezium.data.geometry.Point` | `STRING` | WKB格式JSON字符串 |
| `io.debezium.data.geometry.Geometry` | `STRING` | WKB格式JSON字符串 |

### 4.4 值转换处理

```java
// transformRawValue 核心逻辑流程
public static String transformRawValue(rawValue, debeziumType, className, ...) {
    if (rawValue == null) return null;

    // 1. Bits类型: 小端序转大端序
    if (Bits.LOGICAL_NAME.equals(className)) {
        byte[] littleEndian = Base64.getDecoder().decode(rawValue);
        byte[] bigEndian = reverse(littleEndian);
        return encodeBytes(bigEndian);
    }

    // 2. Binary类型: Base64解码转字符串
    if ("bytes".equals(debeziumType) && className == null) {
        return new String(Base64.getDecoder().decode(rawValue));
    }

    // 3. Decimal类型: 验证数值格式
    if ("bytes".equals(debeziumType) && className.endsWith("Decimal")) {
        new BigDecimal(rawValue); // 验证格式
    }

    // 4. Date类型: Epoch Day转日期字符串
    if (Date.SCHEMA_NAME.equals(className)) {
        return DateTimeUtils.toLocalDate(Integer.parseInt(rawValue)).toString();
    }

    // 5. Timestamp类型: 毫秒/微秒转时间戳字符串
    if (Timestamp.SCHEMA_NAME.equals(className)) {
        return formatLocalDateTime(toLocalDateTime(Long.parseLong(rawValue)), 3);
    }

    // 6. MicroTimestamp类型: 微秒转时间戳字符串
    if (MicroTimestamp.SCHEMA_NAME.equals(className)) {
        long microseconds = Long.parseLong(rawValue);
        Instant instant = Instant.ofEpochSecond(
            microseconds / 1_000_000,
            (microseconds % 1_000_000) * 1_000
        );
        return formatLocalDateTime(instant, 6);
    }

    // 7. ZonedTimestamp类型: ISO-8601字符串转本地时间戳
    if (ZonedTimestamp.SCHEMA_NAME.equals(className)) {
        return formatLocalDateTime(
            Instant.parse(rawValue).atZone(serverTimeZone).toLocalDateTime(),
            6
        );
    }

    // 8. Geometry类型: WKB转GeoJSON
    if (Point.LOGICAL_NAME.equals(className) || Geometry.LOGICAL_NAME.equals(className)) {
        return MySqlTypeUtils.convertWkbArray(geometryGetter.get());
    }

    return rawValue;
}
```

---

## 5. Avro格式支持

### 5.1 Avro类型转换流程

```
Avro Schema
        │
        ▼
┌─────────────────────────────────────────────────────────────────────────────────────┐
│ 1. 获取connect.parameters (如果启用column.propagate.source.type)                    │
│    - __debezium.source.column.type: MySQL原始类型                                   │
│    - __debezium.source.column.length: 长度                                          │
│    - __debezium.source.column.scale: 小数位数                                       │
└─────────────────────────────────────────────────────────────────────────────────────┘
        │
        ▼ (如果parameters存在)
┌─────────────────────────────────────────────────────────────────────────────────────┐
│ 2a. 使用MySQL类型映射                                                               │
│    MySqlTypeUtils.toDataType(typeName, length, scale)                               │
└─────────────────────────────────────────────────────────────────────────────────────┘
        │
        ▼ (如果parameters不存在)
┌─────────────────────────────────────────────────────────────────────────────────────┐
│ 2b. 使用Avro类型映射                                                                │
│    fromDebeziumAvroType(avroSchema)                                                │
└─────────────────────────────────────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────────────────────────────────────┐
│ 3. Avro Logical Type处理                                                            │
│    - Date -> DATE                                                                  │
│    - TimestampMillis -> TIMESTAMP_MILLIS                                           │
│    - TimestampMicros -> TIMESTAMP                                                   │
│    - Decimal -> DECIMAL(p, s)                                                       │
│    - TimeMillis -> TIME(3)                                                          │
│    - TimeMicros -> TIME(6)                                                          │
│    - LocalTimestampMicros -> TIMESTAMP_WITH_LOCAL_TIME_ZONE                         │
└─────────────────────────────────────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────────────────────────────────────┐
│ 4. Avro Primitive Type处理                                                          │
│    - BOOLEAN -> BOOLEAN                                                            │
│    - BYTES/FIXED -> BYTES                                                          │
│    - DOUBLE -> DOUBLE                                                              │
│    - FLOAT -> FLOAT                                                                │
│    - INT -> INT                                                                    │
│    - LONG -> BIGINT                                                                │
│    - STRING/ENUM -> STRING                                                         │
└─────────────────────────────────────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────────────────────────────────────┐
│ 5. Avro Complex Type处理                                                            │
│    - RECORD -> ROW                                                                 │
│    - ARRAY -> ARRAY                                                                │
│    - MAP -> MAP                                                                    │
│    - UNION (with NULL) -> nullable(type)                                            │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

### 5.2 Avro对象到JSON转换

```java
// convertAvroObjectToJsonCompatible 递归转换
public static Object convertAvroObjectToJsonCompatible(Object avroObject) {
    if (avroObject instanceof GenericData.Record) {
        // RECORD -> Map
        return convertRecord((GenericData.Record) avroObject);
    } else if (avroObject instanceof GenericData.Array) {
        // ARRAY -> List
        return convertArray((GenericData.Array<?>) avroObject);
    } else if (avroObject instanceof Utf8) {
        // Utf8 -> String
        return avroObject.toString();
    } else if (avroObject instanceof Map) {
        // Map -> Map (递归转换key和value)
        return convertMap((Map<Object, Object>) avroObject);
    } else if (avroObject instanceof List) {
        // List -> List (递归转换元素)
        return convertList((List<Object>) avroObject);
    } else {
        return avroObject;
    }
}
```

---

## 6. Schema解析详解

### 6.1 Debezium JSON Schema结构

```json
{
  "schema": {
    "type": "struct",
    "fields": [
      {
        "type": "struct",
        "fields": [
          {
            "type": "int32",
            "optional": true,
            "field": "id"
          },
          {
            "type": "string",
            "optional": false,
            "field": "name"
          },
          {
            "type": "bytes",
            "optional": false,
            "field": "amount",
            "name": "org.apache.kafka.connect.data.Decimal"
          }
        ],
        "optional": false,
        "field": "after"
      },
      {
        "type": "struct",
        "fields": [...],
        "optional": false,
        "field": "before"
      },
      {
        "type": "string",
        "optional": false,
        "field": "source"
      },
      {
        "type": "string",
        "optional": false,
        "field": "op"
      }
    ],
    "optional": false,
    "name": "..."
  },
  "payload": {...}
}
```

### 6.2 Schema解析代码流程

```java
private void parseSchema(JsonNode schema) {
    // 1. 清空之前的缓存
    debeziumTypes.clear();
    classNames.clear();
    parameters.clear();

    // 2. 获取顶层fields
    ArrayNode schemaFields = schema.get("fields");

    // 3. 找到after或before字段的定义
    ArrayNode fields = null;
    for (JsonNode node : schemaFields) {
        String fieldName = node.get("field").asText();
        if ("after".equals(fieldName) || "before".equals(fieldName)) {
            fields = node.get("fields");
            break;
        }
    }

    // 4. 解析每个字段
    for (JsonNode node : fields) {
        String fieldName = node.get("field").asText();
        String debeziumType = node.get("type").asText();
        String className = getNullable(node, "name");
        JsonNode parametersNode = node.get("parameters");

        // 存储解析结果
        debeziumTypes.put(fieldName, debeziumType);
        classNames.put(fieldName, className);
        parameters.put(fieldName, parseParameters(parametersNode));
    }
}
```

---

## 7. 关键代码片段分析

### 7.1 DebeziumJsonRecordParser.extractRecords()

**位置**: `DebeziumJsonRecordParser.java:90-110`

```java
@Override
public List<RichCdcMultiplexRecord> extractRecords() {
    String operation = getAndCheck(FIELD_TYPE).asText();
    List<RichCdcMultiplexRecord> records = new ArrayList<>();

    switch (operation) {
        case OP_INSERT:
        case OP_READE:
            // INSERT和READ都生成INSERT记录
            processRecord(getData(), RowKind.INSERT, records);
            break;
        case OP_UPDATE:
            // UPDATE需要生成DELETE + INSERT两条记录
            // 首先用before数据填充完整记录作为DELETE
            processRecord(
                mergeOldRecord(getData(), getBefore(operation)),
                RowKind.DELETE,
                records
            );
            // 然后用after数据作为INSERT
            processRecord(getData(), RowKind.INSERT, records);
            break;
        case OP_DELETE:
            // DELETE生成DELETE记录
            processRecord(getBefore(operation), RowKind.DELETE, records);
            break;
        default:
            throw new UnsupportedOperationException("Unknown record operation: " + operation);
    }
    return records;
}
```

**关键点**:
1. UPDATE操作被拆分为DELETE + INSERT，这是因为Paimon的LSM树结构
2. `mergeOldRecord()`将before字段合并到完整记录中
3. `processRecord()`负责提取数据并创建`RichCdcMultiplexRecord`

### 7.2 extractRowData() - Schema模式

**位置**: `DebeziumJsonRecordParser.java:184-217`

```java
@Override
protected Map<String, String> extractRowData(JsonNode record, CdcSchema.Builder schemaBuilder) {
    if (!hasSchema) {
        // 没有schema信息，使用默认处理
        return super.extractRowData(record, schemaBuilder);
    }

    // 有schema信息，使用类型感知处理
    Map<String, Object> recordMap = convertValue(record, new TypeReference<Map<String, Object>>() {});
    LinkedHashMap<String, String> resultMap = new LinkedHashMap<>();

    for (Map.Entry<String, Object> entry : recordMap.entrySet()) {
        String fieldName = entry.getKey();
        String rawValue = Objects.toString(entry.getValue(), null);
        String debeziumType = debeziumTypes.get(fieldName);
        String className = classNames.get(fieldName);

        // 1. 使用schema信息转换值
        String transformed = DebeziumSchemaUtils.transformRawValue(
            rawValue, debeziumType, className, typeMapping,
            record.get(fieldName), ZoneOffset.UTC
        );
        resultMap.put(fieldName, transformed);

        // 2. 使用schema信息推断类型
        schemaBuilder.column(
            fieldName,
            DebeziumSchemaUtils.toDataType(debeziumType, className, parameters.get(fieldName))
        );
    }

    // 3. 计算计算列
    evalComputedColumns(resultMap, schemaBuilder);

    return resultMap;
}
```

---

## 8. 性能优化点

### 8.1 缓存机制

```java
// DebeziumJsonRecordParser中的缓存
private boolean hasSchema;                          // 是否包含schema
private final Map<String, String> debeziumTypes;   // 字段名->debezium类型缓存
private final Map<String, String> classNames;       // 字段名->类名缓存
private final Map<String, Map<String, String>> parameters;  // 字段名->参数缓存
```

**优化效果**:
- 每条消息只解析一次schema
- 类型信息在解析后缓存，避免重复查询

### 8.2 类型转换优化

```java
// 使用toDataType()预先计算类型，避免运行时反射
DataType dataType = DebeziumSchemaUtils.toDataType(debeziumType, className, params);
schemaBuilder.column(fieldName, dataType);
```

### 8.3 避免不必要的对象创建

```java
// 重用LinkedHashMap和ArrayList
LinkedHashMap<String, String> resultMap = new LinkedHashMap<>();
List<RichCdcMultiplexRecord> records = new ArrayList<>();
```

---

## 9. 错误处理

### 9.1 必填字段检查

```java
// AbstractJsonRecordParser中的检查方法
protected JsonNode getAndCheck(String key) {
    JsonNode node = root.get(key);
    if (isNull(node)) {
        throw new RuntimeException(
            String.format("Invalid %s format: missing '%s' field.", format(), key)
        );
    }
    return node;
}

// 条件检查
protected JsonNode getAndCheck(String key, String conditionKey, String conditionValue) {
    JsonNode node = root.get(key);
    if (isNull(node)) {
        throw new RuntimeException(
            String.format(
                "Invalid %s format: missing '%s' field when '%s' is '%s'.",
                format(), key, conditionKey, conditionValue
            )
        );
    }
    return node;
}
```

### 9.2 Decimal类型验证

```java
if ("bytes".equals(debeziumType) && className.endsWith("Decimal")) {
    try {
        new BigDecimal(rawValue);
    } catch (NumberFormatException e) {
        throw new IllegalArgumentException(
            "Invalid big decimal value " + rawValue +
            ". Make sure that in the `customConverterConfigs` " +
            "set 'decimal.format' to 'numeric'",
            e
        );
    }
}
```

---

## 10. 总结

Debezium格式解析模块是Paimon CDC数据同步的核心组件，主要特点：

1. **多格式支持**: JSON、Avro、BSON三种格式
2. **类型感知**: 通过schema信息实现精确的类型转换
3. **操作类型识别**: 正确处理INSERT/UPDATE/DELETE/READ操作
4. **性能优化**: 缓存机制减少重复计算
5. **错误处理**: 完善的字段检查和异常处理

**关键数据流**:
```
Kafka Message -> CdcSourceRecord -> DebeziumJsonRecordParser
    -> extractRecords() -> RichCdcMultiplexRecord -> 下游处理
```
