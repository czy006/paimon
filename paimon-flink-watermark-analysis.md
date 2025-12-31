# Paimon Flink CDC Watermark机制分析

> **文档版本**: v1.0
> **分析日期**: 2025-12-24
> **模块路径**: `/paimon-flink/paimon-flink-cdc/src/main/java/org/apache/paimon/flink/action/cdc/watermark`

---

## 1. 概述

Watermark机制在Paimon CDC中用于支持基于事件时间的自动Tag创建。当配置`TAG_AUTOMATIC_CREATION = WATERMARK`时，Paimon会根据CDC数据的事件时间自动创建时间标签（Tags），用于数据生命周期管理、时间旅行查询等场景。

---

## 2. Watermark在CDC中的核心作用

### 2.1 作用概述

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                    Watermark在CDC订阅中的作用                                      │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                     │
│  1. 事件时间提取                                                                   │
│     从CDC消息中提取事件发生时间（如MySQL的ts_ms字段）                              │
│                          │                                                         │
│                          ▼                                                         │
│  2. Watermark生成                                                                 │
│     基于事件时间生成Flink Watermark，驱动流处理                                   │
│                          │                                                         │
│                          ▼                                                         │
│  3. 时间对齐                                                                       │
│     多分区/多Source情况下，确保Watermark对齐，避免乱序                            │
│                          │                                                         │
│                          ▼                                                         │
│  4. Tag自动创建                                                                   │
│     根据Watermark时间自动创建Paimon Tag，标记数据时间点                           │
│                          │                                                         │
│                          ▼                                                         │
│  5. 数据生命周期管理                                                               │
│     基于Tag实现数据过期、清理、时间旅行等                                         │
│                                                                                     │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

### 2.2 核心配置

| 配置项 | 说明 | 默认值 |
|--------|------|--------|
| `tag.automatic-creation` | Tag创建模式 | `NONE` |
| `tag.creation-period` | Tag创建周期 | `DAILY` |
| `tag.period-formatter` | Tag名称格式 | `WITH_DASHES` |
| `scan.watermark.idle-timeout` | 空闲超时 | 无 |
| `scan.watermark.alignment.group` | 对齐组 | 无 |
| `scan.watermark.alignment.max-drift` | 最大漂移 | 无 |
| `scan.watermark.alignment.update-interval` | 更新间隔 | 1s |

---

## 3. Watermark模块架构

### 3.1 整体架构图

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                        CDC Watermark模块架构                                        │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                     │
│  CDC Source Record                                                                 │
│  ┌──────────────────────────────────────────────────────────────────────────────┐  │
│  │  Kafka/Pulsar Message:                                                       │  │
│  │  {                                                                            │  │
│  │    "payload": {                                                               │  │
│  │      "ts_ms": 1735039200000,  <-- 事件时间戳                                  │  │
│  │      ...                                                                      │  │
│  │    }                                                                          │  │
│  │  }                                                                            │  │
│  └──────────────────────────────────────────────────────────────────────────────┘  │
│                                      │                                             │
│                                      ▼                                             │
│  ┌──────────────────────────────────────────────────────────────────────────────┐  │
│  │  CdcTimestampExtractor (时间戳提取器接口)                                   │  │
│  │  ┌────────────────────────────────────────────────────────────────────────┐  │  │
│  │  │  + extractTimestamp(CdcSourceRecord): long                             │  │  │
│  │  └────────────────────────────────────────────────────────────────────────┘  │  │
│  │                              ▲                                               │  │
│  │                              │ 实现                                          │  │  │
│  │  ┌────────────────────────────────────────────────────────────────────────┐  │  │
│  │  │  MysqlCdcTimestampExtractor (MySQL CDC)                                │  │
│  │  │  - 提取 payload.ts_ms                                                   │  │
│  │  └────────────────────────────────────────────────────────────────────────┘  │  │
│  │  ┌────────────────────────────────────────────────────────────────────────┐  │  │
│  │  │  MessageQueueCdcTimestampExtractor (Kafka/Pulsar)                      │  │
│  │  │  - 支持 Canal/OGG/Maxwell/Debezium/阿里yun格式                          │  │
│  │  └────────────────────────────────────────────────────────────────────────┘  │  │
│  │  ┌────────────────────────────────────────────────────────────────────────┐  │  │
│  │  │  MongoDBTimestampExtractor (MongoDB)                                   │  │
│  │  │  - 提取 clusterTime/_ts_ms                                             │  │
│  │  └────────────────────────────────────────────────────────────────────────┘  │  │
│  └──────────────────────────────────────────────────────────────────────────────┘  │
│                                      │                                             │
│                                      ▼                                             │
│  ┌──────────────────────────────────────────────────────────────────────────────┐  │
│  │  CdcWatermarkStrategy (Flink WatermarkStrategy实现)                        │  │
│  │  ┌────────────────────────────────────────────────────────────────────────┐  │  │
│  │  │  WatermarkGenerator<CdcSourceRecord>                                  │  │  │
│  │  │    - onEvent(record, timestamp, output):                             │  │  │
│  │  │      1. extractTimestamp()提取时间                                   │  │
│  │  │      2. 更新currentMaxTimestamp                                      │  │
│  │  │      3. 发出Watermark(currentMaxTimestamp - 1)                       │  │
│  │  │    - onPeriodicEmit(output):                                         │  │
│  │  │      1. 定期发出系统时间Watermark                                    │  │
│  │  │      2. 处理空闲分区                                                │  │
│  │  └────────────────────────────────────────────────────────────────────────┘  │  │
│  └──────────────────────────────────────────────────────────────────────────────┘  │
│                                      │                                             │
│                                      ▼                                             │
│  ┌──────────────────────────────────────────────────────────────────────────────┐  │
│  │  Flink DataStream (带Watermark)                                            │  │
│  │  DataStream<CdcSourceRecord>                                               │  │
│  │    .withWatermarkAlignment()  (可选)                                       │  │
│  │    .withIdleness()            (可选)                                       │  │
│  └──────────────────────────────────────────────────────────────────────────────┘  │
│                                      │                                             │
│                                      ▼                                             │
│  ┌──────────────────────────────────────────────────────────────────────────────┐  │
│  │  Paimon Sink                                                                  │  │
│  │  CdcRecordStoreMultiWriteOperator                                         │  │
│  │    - 收集记录及其Watermark                                                  │  │
│  │    - 在Checkpoint时提交Committable                                        │  │
│  │    - Committable包含Watermark时间                                          │  │
│  └──────────────────────────────────────────────────────────────────────────────┘  │
│                                      │                                             │
│                                      ▼                                             │
│  ┌──────────────────────────────────────────────────────────────────────────────┐  │
│  │  TagAutoManager                                                              │  │
│  │  ┌────────────────────────────────────────────────────────────────────────┐  │  │
│  │  │  TagPeriodHandler                                                      │  │
│  │  │    - HOURLY:    格式 "2025-12-24 13"                                   │  │
│  │  │    - DAILY:     格式 "2025-12-24"                                     │  │
│  │  │    - TWO_HOURS: 格式 "2025-12-24 12"                                  │  │
│  │  │    - 自定义:    分钟级别 "202512241315"                               │  │
│  │  └────────────────────────────────────────────────────────────────────────┘  │  │
│  │  ┌────────────────────────────────────────────────────────────────────────┐  │  │
│  │  │  TagAutoCreation                                                       │  │
│  │  │    - 根据Watermark周期性创建Tag                                        │  │
│  │  │    - 过期旧Tag                                                         │  │
│  │  └────────────────────────────────────────────────────────────────────────┘  │  │
│  └──────────────────────────────────────────────────────────────────────────────┘  │
│                                      │                                             │
│                                      ▼                                             │
│   Paimon Storage                                                                  │
│   Tags: "2025-12-24", "2025-12-23", ...                                         │
│                                                                                     │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

### 3.2 核心类说明

| 类名 | 职责 |
|------|------|
| `CdcTimestampExtractor` | 时间戳提取器接口 |
| `CdcDebeziumTimestampExtractor` | Debezium格式时间戳提取器基类 |
| `MessageQueueCdcTimestampExtractor` | Kafka/Pulsar消息时间戳提取器 |
| `MysqlCdcTimestampExtractor` | MySQL CDC时间戳提取器 |
| `CdcWatermarkStrategy` | Flink WatermarkStrategy实现 |

---

## 4. 时间戳提取器详解

### 4.1 CdcTimestampExtractor接口

**位置**: `CdcTimestampExtractor.java`

```java
public interface CdcTimestampExtractor extends Serializable {
    /**
     * 从CDC记录中提取时间戳
     * @param record CDC源记录
     * @return 时间戳（毫秒），如果返回Long.MIN_VALUE表示忽略该记录
     */
    long extractTimestamp(CdcSourceRecord record) throws JsonProcessingException;
}
```

### 4.2 MysqlCdcTimestampExtractor

**位置**: `MysqlCdcTimestampExtractor.java`

```java
public class MysqlCdcTimestampExtractor extends CdcDebeziumTimestampExtractor {
    @Override
    public long extractTimestamp(CdcSourceRecord record) throws JsonProcessingException {
        JsonNode json = JsonSerdeUtil.fromJson((String) record.getValue(), JsonNode.class);
        // 从Debezium JSON的payload.ts_ms字段提取
        return JsonSerdeUtil.extractValueOrDefault(
            json, Long.class, Long.MIN_VALUE, "payload", "ts_ms"
        );
    }
}
```

**提取的时间戳示例**:
```json
{
  "payload": {
    "ts_ms": 1735039200000,  <-- 提取这个时间戳
    "before": {...},
    "after": {...}
  }
}
```

### 4.3 MessageQueueCdcTimestampExtractor

**位置**: `MessageQueueCdcTimestampExtractor.java`

支持多种CDC格式的提取：

```java
public long extractTimestamp(CdcSourceRecord cdcSourceRecord) throws JsonProcessingException {
    JsonNode record = (JsonNode) cdcSourceRecord.getValue();

    // 1. Canal JSON格式
    if (JsonSerdeUtil.isNodeExists(record, "mysqlType")) {
        return JsonSerdeUtil.extractValue(record, Long.class, "ts");
    }

    // 2. OGG JSON格式
    if (JsonSerdeUtil.isNodeExists(record, "pos")) {
        String dateTimeString = JsonSerdeUtil.extractValue(record, String.class, "op_ts");
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");
        LocalDateTime dateTime = LocalDateTime.parse(dateTimeString, formatter);
        return dateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
    }

    // 3. Maxwell JSON格式
    if (JsonSerdeUtil.isNodeExists(record, "xid")) {
        return JsonSerdeUtil.extractValue(record, Long.class, "ts") * 1000;
    }

    // 4. Debezium JSON格式 (带payload)
    if (JsonSerdeUtil.isNodeExists(record, "payload", "source", "connector")) {
        return JsonSerdeUtil.extractValue(record, Long.class, "payload", "ts_ms");
    }

    // 5. Debezium JSON格式 (不带payload)
    if (JsonSerdeUtil.isNodeExists(record, "source", "connector")) {
        return JsonSerdeUtil.extractValue(record, Long.class, "ts_ms");
    }

    // 6. 阿里yun DTS格式
    if (JsonSerdeUtil.isNodeExists(record, "payload", "timestamp")) {
        return JsonSerdeUtil.extractValue(record, Long.class, "payload", "timestamp", "systemTime");
    }

    throw new RuntimeException("Unsupported CDC format");
}
```

### 4.4 各种格式的时间戳字段位置

| CDC格式 | 时间戳字段 | 示例值 |
|---------|-----------|--------|
| Canal JSON | `ts` | 1735039200 |
| OGG JSON | `op_ts` | "2025-12-24 10:30:00.000000" |
| Maxwell JSON | `ts` | 1735039200 (秒级) |
| Debezium JSON | `ts_ms` | 1735039200000 (毫秒级) |
| 阿里yun DTS | `payload.timestamp.systemTime` | 1735039200000 |

---

## 5. CdcWatermarkStrategy详解

### 5.1 Watermark生成逻辑

**位置**: `CdcWatermarkStrategy.java:45-73`

```java
public WatermarkGenerator<CdcSourceRecord> createWatermarkGenerator(
        WatermarkGeneratorSupplier.Context context) {
    return new WatermarkGenerator<CdcSourceRecord>() {

        @Override
        public void onEvent(CdcSourceRecord record, long timestamp, WatermarkOutput output) {
            long tMs;
            try {
                // 1. 提取时间戳
                tMs = timestampExtractor.extractTimestamp(record);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }

            // 2. 如果是Schema变更事件，忽略（ts_ms为null）
            if (tMs != Long.MIN_VALUE) {
                // 3. 更新最大时间戳
                currentMaxTimestamp = Math.max(currentMaxTimestamp, tMs);

                // 4. 发出Watermark (减1表示包含当前时间之前的所有数据)
                output.emitWatermark(new Watermark(currentMaxTimestamp - 1));
            }
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            // 定期发出系统时间Watermark
            // 处理空闲分区或没有新数据的情况
            long timeMillis = System.currentTimeMillis();
            currentMaxTimestamp = Math.max(timeMillis, currentMaxTimestamp);
            output.emitWatermark(new Watermark(currentMaxTimestamp - 1));
        }
    };
}
```

### 5.2 Watermark生成流程图

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                        Watermark生成流程                                            │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                     │
│  CDC Record到达                                                                     │
│      │                                                                             │
│      ▼                                                                             │
│  ┌──────────────────────────────────────────────────────────────────────────────┐  │
│  │  CdcTimestampExtractor.extractTimestamp()                                   │  │
│  │    - 根据CDC格式提取时间戳                                                   │  │
│  │    - 返回: Long.MIN_VALUE (Schema变更, 忽略)                                │  │
│  │           或 具体时间戳 (毫秒)                                              │  │
│  └──────────────────────────────────────────────────────────────────────────────┘  │
│      │                                                                             │
│      ▼                                                                             │
│  ┌──────────────────────────────────────────────────────────────────────────────┐  │
│  │  判断时间戳值                                                                 │  │
│  │  if (tMs == Long.MIN_VALUE) {                                                │  │
│  │      // Schema变更事件，忽略Watermark更新                                    │  │
│  │      return;                                                                  │  │
│  │  }                                                                            │  │
│  └──────────────────────────────────────────────────────────────────────────────┘  │
│      │                                                                             │
│      ▼                                                                             │
│  ┌──────────────────────────────────────────────────────────────────────────────┐  │
│  │  更新最大时间戳                                                               │  │
│  │  currentMaxTimestamp = Math.max(currentMaxTimestamp, tMs);                   │  │
│  └──────────────────────────────────────────────────────────────────────────────┘  │
│      │                                                                             │
│      ▼                                                                             │
│  ┌──────────────────────────────────────────────────────────────────────────────┐  │
│  │  发出Watermark                                                                │  │
│  │  output.emitWatermark(new Watermark(currentMaxTimestamp - 1));                │  │
│  │                                                                              │  │
│  │  注意: 减1的原因是Flink Watermark语义                                        │  │
│  │  "Watermark W表示时间 <= W的事件都已到达"                                   │  │
│  │  所以用 maxTimestamp - 1 确保当前时间戳的数据被包含                          │  │
│  └──────────────────────────────────────────────────────────────────────────────┘  │
│      │                                                                             │
│      ▼                                                                             │
│   Watermark向下传递                                                                │
│                                                                                     │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

### 5.3 Watermark对齐（Watermark Alignment）

当配置了`SCAN_WATERMARK_ALIGNMENT_GROUP`时，Watermark会对齐以处理多分区/多Source的乱序问题：

```java
if (watermarkAlignGroup != null) {
    new CdcWatermarkStrategy(createCdcTimestampExtractor())
        .withWatermarkAlignment(
            watermarkAlignGroup,                    // 对齐组名称
            options.get(SCAN_WATERMARK_ALIGNMENT_MAX_DRIFT),  // 最大漂移
            options.get(SCAN_WATERMARK_ALIGNMENT_UPDATE_INTERVAL)  // 更新间隔
        )
}
```

**对齐机制**:
- 同一组内的所有Source使用相同的Watermark
- 当某个Source的Watermark落后超过`maxDrift`时，暂停该Source
- 定期更新对齐后的Watermark

---

## 6. Watermark与Tag创建的关联

### 6.1 启用条件

在`SynchronizationActionBase.buildDataStreamSource()`中判断：

```java
boolean isAutomaticWatermarkCreationEnabled =
    tableConfig.containsKey(CoreOptions.TAG_AUTOMATIC_CREATION.key())
        && Objects.equals(
            tableConfig.get(CoreOptions.TAG_AUTOMATIC_CREATION.key()),
            TagCreationMode.WATERMARK.toString());  // 必须是WATERMARK模式
```

### 6.2 Watermark到Tag的转换流程

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                    Watermark到Tag的转换流程                                        │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                     │
│  Flink Watermark (long)                                                            │
│  ┌──────────────────────────────────────────────────────────────────────────────┐  │
│  │  Watermark = 1735039200000                                                    │  │
│  │  (2025-12-24 12:00:00)                                                         │  │
│  └──────────────────────────────────────────────────────────────────────────────┘  │
│                              │                                                     │
│                              ▼                                                     │
│  ┌──────────────────────────────────────────────────────────────────────────────┐  │
│  │  TableCommitImpl.commit()                                                    │  │
│  │  - 提交Committable，包含Watermark时间                                        │  │
│  │  - 调用TagAutoManager.run()                                                  │  │
│  └──────────────────────────────────────────────────────────────────────────────┘  │
│                              │                                                     │
│                              ▼                                                     │
│  ┌──────────────────────────────────────────────────────────────────────────────┐  │
│  │  TagAutoManager                                                              │  │
│  │  ┌────────────────────────────────────────────────────────────────────────┐  │  │
│  │  │  TagAutoCreation.run()                                                 │  │
│  │  │    1. 获取最新Snapshot的Watermark时间                                   │  │
│  │  │    2. 根据TAG_CREATION_PERIOD创建Tag                                   │  │
│  │  │    3. 调用TagPeriodHandler.timeToTag()生成Tag名称                     │  │
│  │  │    4. 创建Tag: tagManager.createTag(tagName, snapshot)                │  │
│  │  │    5. 过期旧Tag                                                         │  │
│  │  └────────────────────────────────────────────────────────────────────────┘  │  │
│  └──────────────────────────────────────────────────────────────────────────────┘  │
│                              │                                                     │
│                              ▼                                                     │
│  ┌──────────────────────────────────────────────────────────────────────────────┐  │
│  │  TagPeriodHandler (根据周期格式化时间)                                      │  │
│  │  ┌────────────────────────────────────────────────────────────────────────┐  │  │
│  │  │  HOURLY + WITH_DASHES:                                                   │  │
│  │  │    "2025-12-24 12"                                                       │  │
│  │  │                                                                          │  │
│  │  │  HOURLY + WITHOUT_DASHES:                                               │  │
│  │  │    "20251224 12"                                                        │  │
│  │  │                                                                          │  │
│  │  │  DAILY + WITH_DASHES:                                                    │  │
│  │  │    "2025-12-24"                                                         │  │
│  │  │                                                                          │  │
│  │  │  DAILY + WITHOUT_DASHES:                                                │  │
│  │  │    "20251224"                                                           │  │
│  │  │                                                                          │  │
│  │  │  自定义周期 (如30分钟):                                                   │  │
│  │  │    "202512241215"  (YYYYMMddHHmm)                                       │  │
│  │  └────────────────────────────────────────────────────────────────────────┘  │  │
│  └──────────────────────────────────────────────────────────────────────────────┘  │
│                              │                                                     │
│                              ▼                                                     │
│   Paimon Tag创建                                                                    │
│   ┌──────────────────────────────────────────────────────────────────────────────┐  │
│   │  Tags:                                                                       │  │
│   │  - "2025-12-24"  (最新)                                                     │  │
│   │  - "2025-12-23"                                                             │  │
│   │  - "2025-12-22"                                                             │  │
│   │  - ...                                                                      │  │
│   └──────────────────────────────────────────────────────────────────────────────┘  │
│                                                                                     │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

### 6.3 Tag创建示例

**配置**:
```java
tableConfig.put("tag.automatic-creation", "watermark");
tableConfig.put("tag.creation-period", "daily");
tableConfig.put("tag.period-formatter", "with-dashes");
tableConfig.put("tag.num-retained.max", "7");
```

**执行流程**:
```java
// 1. CDC消息到达，ts_ms = 1735039200000 (2025-12-24 12:00:00)
CdcSourceRecord record = ...;

// 2. 提取Watermark
long watermark = timestampExtractor.extractTimestamp(record);  // 1735039200000

// 3. Flink发出Watermark
output.emitWatermark(new Watermark(watermark - 1));

// 4. Sink提交时使用Watermark
ManifestCommittable committable = new ManifestCommittable(checkpointId, watermark);

// 5. TagAutoManager检查是否需要创建Tag
if (isNewPeriod(watermark)) {
    String tagName = TagPeriodHandler.timeToTag(watermark);  // "2025-12-24"
    tagManager.createTag(tagName, snapshot);
}

// 6. 过期旧Tag
tagManager.expireOldTags(retainedMax);  // 保留最近7个Tag
```

### 6.4 不同周期下的Tag创建

| 配置周期 | Watermark时间 | 创建的Tag | 说明 |
|---------|--------------|----------|------|
| `DAILY` | 2025-12-24 12:00:00 | `2025-12-24` | 按天创建 |
| `HOURLY` | 2025-12-24 12:30:00 | `2025-12-24 12` | 按小时创建 |
| `TWO_HOURS` | 2025-12-24 14:00:00 | `2025-12-24 14` | 按2小时创建 |
| `30-minutes` | 2025-12-24 13:15:00 | `202512241315` | 按自定义周期创建 |

---

## 7. 在不同数据源中的实现

### 7.1 MySQL CDC

**位置**: `MySqlSyncTableAction.java:108-109`

```java
@Override
protected CdcTimestampExtractor createCdcTimestampExtractor() {
    return MySqlActionUtils.createCdcTimestampExtractor();
    // 返回 MysqlCdcTimestampExtractor
}
```

**提取时间戳**: `payload.ts_ms` (Debezium格式)

### 7.2 Kafka CDC

**位置**: `KafkaSyncDatabaseAction.java:37-39`

```java
@Override
protected CdcTimestampExtractor createCdcTimestampExtractor() {
    return new MessageQueueCdcTimestampExtractor();
    // 支持Canal/OGG/Maxwell/Debezium/阿里yunDTS格式
}
```

### 7.3 Pulsar CDC

**位置**: `PulsarSyncDatabaseAction.java:37-39`

```java
@Override
protected CdcTimestampExtractor createCdcTimestampExtractor() {
    return new MessageQueueCdcTimestampExtractor();
    // 与Kafka相同
}
```

### 7.4 MongoDB CDC

**位置**: `MongoDBSyncTableAction.java:67-68`

```java
@Override
protected CdcTimestampExtractor createCdcTimestampExtractor() {
    return MongoDBActionUtils.createCdcTimestampExtractor();
    // 提取clusterTime或_ts_ms
}
```

---

## 8. Watermark与Schema变更的关系

### 8.1 Schema变更事件处理

```java
@Override
public void onEvent(CdcSourceRecord record, long timestamp, WatermarkOutput output) {
    long tMs = timestampExtractor.extractTimestamp(record);

    // Schema变更事件返回Long.MIN_VALUE
    if (tMs != Long.MIN_VALUE) {
        currentMaxTimestamp = Math.max(currentMaxTimestamp, tMs);
        output.emitWatermark(new Watermark(currentMaxTimestamp - 1));
    }
    // 如果是Schema变更事件，不更新Watermark
}
```

**原因**: Schema变更事件没有实际的业务数据时间戳，如果用它更新Watermark会导致Watermark不准确。

### 8.2 多类型事件的时间戳

| 事件类型 | 时间戳字段 | Watermark处理 |
|---------|-----------|--------------|
| INSERT/UPDATE/DELETE | `ts_ms` | 正常提取并更新Watermark |
| CREATE/DROP TABLE | `ts_ms` | 正常提取并更新Watermark |
| ALTER TABLE | `ts_ms` | 正常提取并更新Watermark |
| Schema Snapshot | 无 | 返回Long.MIN_VALUE，忽略 |

---

## 9. 空闲分区处理

### 9.1 空闲超时配置

```java
Duration idleTimeout = options.get(SCAN_WATERMARK_IDLE_TIMEOUT);
if (idleTimeout != null) {
    watermarkStrategy = watermarkStrategy.withIdleness(idleTimeout);
}
```

### 9.2 空闲分区处理机制

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                        空闲分区处理机制                                            │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                     │
│  分区A (活跃)                    分区B (空闲)                                      │
│  ┌──────────────────────┐    ┌──────────────────────┐                            │
│  │ 10:00 -> Watermark   │    │ (无数据到达)         │                            │
│  │ 10:01 -> Watermark   │    │                      │                            │
│  │ 10:02 -> Watermark   │    │ 超过空闲超时         │                            │
│  │ 10:03 -> Watermark   │◀───┼────┐                │                            │
│  └──────────────────────┘    │    ▼                │                            │
│                              │  Watermark推进      │                            │
│                              │  使用onPeriodicEmit  │                            │
│                              │  发出系统时间        │                            │
│                              └──────────────────────┘                            │
│                                                                                     │
│  结果: 分区B的空闲不会阻塞整个流的Watermark推进                                 │
│                                                                                     │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

### 9.3 onPeriodicEmit的作用

```java
@Override
public void onPeriodicEmit(WatermarkOutput output) {
    // 使用系统时间作为兜底
    long timeMillis = System.currentTimeMillis();
    currentMaxTimestamp = Math.max(timeMillis, currentMaxTimestamp);
    output.emitWatermark(new Watermark(currentMaxTimestamp - 1));
}
```

**作用**:
1. 处理空闲分区：当某个分区长时间无数据时，用系统时间推进Watermark
2. 定期发出：Flink会定期调用此方法（默认200ms）
3. 最大值保护：确保Watermark不会落后于系统时间

---

## 10. 完整数据流示例

### 10.1 端到端Watermark流

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                        端到端Watermark数据流                                       │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                     │
│  1. MySQL CDC Event                                                               │
│     ┌─────────────────────────────────────────────────────────────────────────┐    │
│     │ Debezium Message:                                                        │    │
│     │ {                                                                         │    │
│     │   "payload": {                                                            │    │
│     │     "before": {"id": 1, "name": "Alice"},                                │    │
│     │     "after": {"id": 1, "name": "Bob"},                                   │    │
│     │     "ts_ms": 1735039200000  <-- 事件时间                                  │    │
│     │   }                                                                       │    │
│     │ }                                                                         │    │
│     └─────────────────────────────────────────────────────────────────────────┘    │
│                                      │                                             │
│                                      ▼                                             │
│  2. Flink Kafka Source                                                           │
│     ┌─────────────────────────────────────────────────────────────────────────┐    │
│     │ CdcSourceRecord(topic="order_topic", key=null, value=JsonNode)          │    │
│     └─────────────────────────────────────────────────────────────────────────┘    │
│                                      │                                             │
│                                      ▼                                             │
│  3. CdcWatermarkStrategy (如果启用了TAG_AUTOMATIC_CREATION)                    │
│     ┌─────────────────────────────────────────────────────────────────────────┐    │
│     │ MysqlCdcTimestampExtractor.extractTimestamp()                           │    │
│     │   -> 1735039200000                                                       │    │
│     │                                                                          │    │
│     │ onEvent()                                                                │    │
│     │   -> currentMaxTimestamp = max(0, 1735039200000)                        │    │
│     │   -> emitWatermark(new Watermark(1735039199999))                         │    │
│     └─────────────────────────────────────────────────────────────────────────┘    │
│                                      │                                             │
│                                      ▼                                             │
│  4. Flink DataStream (带有Watermark)                                            │
│     DataStream<CdcSourceRecord> watermark = 1735039199999                         │
│                                      │                                             │
│                                      ▼                                             │
│  5. Record Parsing                                                               │
│     RichCdcMultiplexRecord (db=mydb, table=orders, record={...})                │
│                                      │                                             │
│                                      ▼                                             │
│  6. CdcDynamicTableParsingProcessFunction                                       │
│     - 解析Schema变更                                                             │
│     - 输出数据记录                                                               │
│                                      │                                             │
│                                      ▼                                             │
│  7. CdcRecordStoreMultiWriteOperator                                              │
│     ┌─────────────────────────────────────────────────────────────────────────┐    │
│     │ write.write(genericRow)                                                 │    │
│     │                                                                          │    │
│     │ prepareCommit():                                                         │    │
│     │   MultiTableCommittable committable = new MultiTableCommittable(        │    │
│     │       tableId,                                                           │    │
│     │       new Committable(                                                   │    │
│     │           watermark,  // 1735039200000                                  │    │
│     │           files,                                                         │    │
│     │           ...                                                            │    │
│     │       )                                                                   │    │
│     │   )                                                                       │    │
│     └─────────────────────────────────────────────────────────────────────────┘    │
│                                      │                                             │
│                                      ▼                                             │
│  8. StoreMultiCommitter                                                           │
│     ┌─────────────────────────────────────────────────────────────────────────┐    │
│     │ commit(committables)                                                    │    │
│     │   -> 写入文件                                                           │    │
│     │   -> 提交Snapshot                                                       │    │
│     │   -> Snapshot.watermark() = 1735039200000                              │    │
│     │   -> 触发TagAutoManager.run()                                           │    │
│     └─────────────────────────────────────────────────────────────────────────┘    │
│                                      │                                             │
│                                      ▼                                             │
│  9. TagAutoManager                                                                │
│     ┌─────────────────────────────────────────────────────────────────────────┐    │
│     │ TagAutoCreation.run()                                                   │    │
│     │   -> watermarkTime = snapshot.watermark()  // 1735039200000            │    │
│     │   -> LocalDateTime = toLocalDateTime(watermarkTime)                     │    │
│     │   -> if (isNewPeriod(LocalDateTime)) {                                  │    │
│     │         tagName = timeToTag(LocalDateTime)  // "2025-12-24"             │    │
│     │         tagManager.createTag(tagName, snapshot)                         │    │
│     │     }                                                                      │    │
│     └─────────────────────────────────────────────────────────────────────────┘    │
│                                      │                                             │
│                                      ▼                                             │
│  10. Paimon Tag创建                                                                │
│      Tag: "2025-12-24"                                                             │
│      - Snapshot: 15                                                                │
│      - Time: 2025-12-24 12:00:00                                                    │
│                                                                                     │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

### 10.2 时间旅行查询

```sql
-- 查询最新数据
SELECT * FROM orders;

-- 查询特定Tag的数据（时间旅行）
SELECT * FROM orders /*+ OPTIONS('scan.tag-name'='2025-12-24') */;

-- 查询两个Tag之间的差异
SELECT * FROM orders /*+ OPTIONS('scan.tag-name'='2025-12-24') */
MINUS
SELECT * FROM orders /*+ OPTIONS('scan.tag-name'='2025-12-23') */;
```

---

## 11. 最佳实践

### 11.1 配置建议

```java
// 场景1: 按天创建Tag，用于每日数据归档
tableConfig.put("tag.automatic-creation", "watermark");
tableConfig.put("tag.creation-period", "daily");
tableConfig.put("tag.num-retained.max", "30");  // 保留30天

// 场景2: 按小时创建Tag，用于近实时分析
tableConfig.put("tag.automatic-creation", "watermark");
tableConfig.put("tag.creation-period", "hourly");
tableConfig.put("tag.num-retained.max", "168");  // 保留7天 (168小时)

// 场景3: 自定义周期（如15分钟）
tableConfig.put("tag.automatic-creation", "watermark");
tableConfig.put("tag.creation-period-duration", "15 min");
tableConfig.put("tag.num-retained.max", "96");  // 保留24小时 (96个15分钟)

// 场景4: 多源对齐
tableConfig.put("tag.automatic-creation", "watermark");
tableConfig.put("scan.watermark.alignment.group", "my-group");
tableConfig.put("scan.watermark.alignment.max-drift", "60s");  // 最大漂移60秒
tableConfig.put("scan.watermark.alignment.update-interval", "1s");

// 场景5: 空闲分区处理
tableConfig.put("tag.automatic-creation", "watermark");
tableConfig.put("scan.watermark.idle-timeout", "5min");  // 5分钟无数据则推进Watermark
```

### 11.2 注意事项

1. **时间戳准确性**: 确保CDC事件的时间戳准确反映业务时间
2. **时区处理**: 注意CDC源时区与Paimon Sink时区的一致性
3. **乱序处理**: 对于存在乱序的数据源，配置合理的Watermark对齐参数
4. **Tag数量**: 避免创建过多Tag，影响性能
5. **过期策略**: 合理配置Tag保留数量，及时清理旧数据

---

## 12. 总结

Watermark机制在Paimon CDC订阅中的核心作用：

1. **事件时间提取**: 从不同格式的CDC消息中提取事件时间戳
2. **Watermark生成**: 基于事件时间生成Flink Watermark
3. **多源对齐**: 支持多分区/多Source的Watermark对齐
4. **空闲处理**: 处理空闲分区，避免阻塞Watermark推进
5. **Tag创建**: 根据Watermark自动创建时间标签
6. **生命周期管理**: 基于Tag实现数据过期和清理

**完整流程**:
```
CDC Event -> CdcTimestampExtractor -> CdcWatermarkStrategy
    -> Flink Watermark -> Paimon Sink -> TagAutoManager
    -> Tag Creation -> Data Lifecycle Management
```
