# Paimon Flink CDC Action核心模块分析

> **文档版本**: v1.0
> **分析日期**: 2025-12-24
> **模块路径**: `/paimon-flink/paimon-flink-cdc/src/main/java/org/apache/paimon/flink/action/cdc`

---

## 1. 概述

CDC Action核心模块是Paimon Flink CDC的数据同步入口层，负责：
- 定义CDC数据记录结构
- 提供同步任务的基础框架
- 处理Schema构建和验证
- 支持计算列和元数据列
- 类型映射和表名转换

---

## 2. 模块架构

### 2.1 整体架构图

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                        CDC Action核心模块架构                                       │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                     │
│  ┌──────────────────────────────────────────────────────────────────────────────┐  │
│  │                        数据定义层 (Data Definitions)                          │  │
│  │  ┌────────────────────────────────────────────────────────────────────────┐  │  │
│  │  │  CdcSourceRecord                CDC源记录 (topic/key/value)              │  │  │
│  │  │  ComputedColumn                 计算列定义                                  │  │  │
│  │  │  Expression                     表达式接口                                 │  │  │
│  │  └────────────────────────────────────────────────────────────────────────┘  │  │
│  └──────────────────────────────────────────────────────────────────────────────┘  │
│                                      │                                             │
│                                      ▼                                             │
│  ┌──────────────────────────────────────────────────────────────────────────────┐  │
│  │                        配置和工具层 (Configuration & Utils)                   │  │
│  │  ┌────────────────────────────────────────────────────────────────────────┐  │  │
│  │  │  TypeMapping                    类型映射配置                               │  │  │
│  │  │  CdcMetadataConverter           元数据转换器                               │  │  │
│  │  │  TableNameConverter            表名转换器                                 │  │  │
│  │  │  CdcActionCommonUtils           通用工具类                                 │  │  │
│  │  │  MessageQueueSchemaUtils       Schema获取工具                            │  │  │
│  │  │  ComputedColumnUtils           计算列工具类                               │  │  │
│  │  └────────────────────────────────────────────────────────────────────────┘  │  │
│  └──────────────────────────────────────────────────────────────────────────────┘  │
│                                      │                                             │
│                                      ▼                                             │
│  ┌──────────────────────────────────────────────────────────────────────────────┐  │
│  │                        同步任务处理层 (Sync Job Handler)                     │  │
│  │  ┌────────────────────────────────────────────────────────────────────────┐  │  │
│  │  │  SyncJobHandler                 同步任务处理器                             │  │  │
│  │  │    - provideSource()             创建CDC Source                          │  │
│  │  │    - provideRecordParser()       提供Record解析器                         │  │
│  │  │    - provideDataFormat()         提供数据格式                             │  │
│  │  │    - checkRequiredOption()       检查必填配置                             │  │
│  │  └────────────────────────────────────────────────────────────────────────┘  │  │
│  └──────────────────────────────────────────────────────────────────────────────┘  │
│                                      │                                             │
│                                      ▼                                             │
│  ┌──────────────────────────────────────────────────────────────────────────────┐  │
│  │                        Action基类层 (Action Base Classes)                    │  │
│  │  ┌────────────────────────────────────────────────────────────────────────┐  │  │
│  │  │  SynchronizationActionBase      同步Action基类                           │  │  │
│  │  │    - build()                      构建Flink任务                           │  │
│  │  │    - buildSource()                创建Source                              │  │
│  │  │    - buildSink()                  创建Sink                                │  │
│  │  │    - recordParse()                记录解析                                │  │
│  │  │    - buildEventParserFactory()    构建Event解析器工厂                    │  │
│  │  └────────────────────────────────────────────────────────────────────────┘  │  │
│  │                              ▲                                               │  │
│  │                              │ 继承                                          │  │
│  │  ┌────────────────────────────────────────────────────────────────────────┐  │  │
│  │  │  SyncTableActionBase           表同步Action基类                          │  │
│  │  │    - retrieveSchema()             获取Schema                              │  │
│  │  │    - buildPaimonSchema()          构建Paimon Schema                       │  │
│  │  │    - withPartitionKeys()          设置分区键                              │  │
│  │  │    - withPrimaryKeys()            设置主键                                │  │
│  │  │    - withComputedColumnArgs()     设置计算列                              │  │
│  │  └────────────────────────────────────────────────────────────────────────┘  │  │
│  │  ┌────────────────────────────────────────────────────────────────────────┐  │  │
│  │  │  SyncDatabaseActionBase         数据库同步Action基类                      │  │
│  │  │    - buildEventParserFactory()    构建多表Event解析器                     │  │
│  │  │    - withTablePrefix()            表名前缀                                │  │
│  │  │    - withTableSuffix()            表名后缀                                │  │
│  │  │    - withTableMapping()           表名映射                                │  │
│  │  │    - includingTables()            包含表过滤                              │  │
│  │  │    - excludingTables()            排除表过滤                              │  │
│  │  └────────────────────────────────────────────────────────────────────────┘  │  │
│  └──────────────────────────────────────────────────────────────────────────────┘  │
│                                                                                     │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

### 2.2 核心类职责说明

| 类名 | 职责 |
|------|------|
| `CdcSourceRecord` | CDC源记录，封装从Kafka等消息队列消费的原始数据 |
| `ComputedColumn` | 计算列定义，支持表达式计算 |
| `Expression` | 表达式接口，支持多种时间、字符串、数值函数 |
| `TypeMapping` | 类型映射配置，控制源类型到Paimon类型的转换规则 |
| `CdcMetadataConverter` | 元数据转换器，提取database_name/table_name等元数据 |
| `TableNameConverter` | 表名转换器，支持前缀/后缀/映射规则 |
| `CdcActionCommonUtils` | 通用工具类，Schema构建、兼容性检查等 |
| `SyncJobHandler` | 同步任务处理器，根据SourceType分发处理逻辑 |
| `SynchronizationActionBase` | 同步Action基类，定义同步流程模板 |
| `SyncTableActionBase` | 表同步基类，处理单表同步逻辑 |
| `SyncDatabaseActionBase` | 数据库同步基类，处理多表同步逻辑 |

---

## 3. 数据定义层

### 3.1 CdcSourceRecord - CDC源记录

**位置**: `CdcSourceRecord.java`

```java
public class CdcSourceRecord implements Serializable {
    @Nullable private final String topic;   // Kafka Topic
    @Nullable private final Object key;     // 消息Key（用于分区）
    private final Object value;             // 消息Value（实际数据）

    public CdcSourceRecord(@Nullable String topic,
                           @Nullable Object key,
                           Object value) {
        this.topic = topic;
        this.key = key;
        this.value = value;
    }

    public String getTopic() { return topic; }
    public Object getKey() { return key; }
    public Object getValue() { return value; }
}
```

**关键点**:
- 封装从消息队列消费的原始消息
- `value`字段可以是JsonNode（JSON格式）、GenericRecord（Avro格式）等
- 作为Kafka Source的输出类型

### 3.2 ComputedColumn - 计算列

**位置**: `ComputedColumn.java`

```java
public class ComputedColumn implements Serializable {
    private final String columnName;      // 列名
    private final Expression expression;  // 表达式

    public ComputedColumn(String columnName, Expression expression) {
        this.columnName = columnName;
        this.expression = expression;
    }

    public String columnName() { return columnName; }
    public DataType columnType() { return expression.outputType(); }

    @Nullable
    public String fieldReference() {
        return expression.fieldReference();
    }

    @Nullable
    public String eval(@Nullable String input) {
        if (fieldReference() != null && input == null) {
            return null;
        }
        return expression.eval(input);
    }
}
```

**使用示例**:
```java
// 创建计算列：pt = date_format(event_time, yyyyMMdd)
ComputedColumn computedColumn = new ComputedColumn(
    "pt",
    Expression.create(typeMapping, caseSensitive, "date_format", "event_time", "yyyyMMdd")
);

// 计算值
String result = computedColumn.eval("2025-12-24 10:30:00"); // "20251224"
```

### 3.3 Expression - 表达式

**位置**: `Expression.java`

**支持的表达式类型**:

| 表达式 | 功能 | 示例 | 输出类型 |
|--------|------|------|----------|
| `year` | 提取年份 | `year(ts)` | INT |
| `month` | 提取月份 | `month(ts)` | INT |
| `day` | 提取日期 | `day(ts)` | INT |
| `hour` | 提取小时 | `hour(ts)` | INT |
| `minute` | 提取分钟 | `minute(ts)` | INT |
| `second` | 提取秒 | `second(ts)` | INT |
| `date_format` | 日期格式化 | `date_format(ts, yyyy-MM-dd)` | STRING |
| `substring` | 子串截取 | `substring(str, 0, 10)` | STRING |
| `truncate` | 数值截断 | `truncate(value, 100)` | 原类型 |
| `cast` | 类型转换 | `cast(value, INT)` | 目标类型 |
| `now` | 当前时间戳 | `now()` | TIMESTAMP(3) |
| `upper` | 转大写 | `upper(str)` | STRING |
| `lower` | 转小写 | `lower(str)` | STRING |
| `trim` | 去空格 | `trim(str)` | STRING |

**表达式创建流程**:
```java
// 1. 定义类型映射
Map<String, DataType> typeMapping = new HashMap<>();
typeMapping.put("event_time", DataTypes.TIMESTAMP(3));

// 2. 创建表达式
Expression expr = Expression.create(
    typeMapping,           // 字段类型映射
    caseSensitive,         // 是否大小写敏感
    "date_format",         // 表达式名称
    "event_time",          // 引用字段
    "yyyyMMdd"             // 格式参数
);

// 3. 计算值
String result = expr.eval("2025-12-24 10:30:00.123");
```

**核心方法**:
```java
public interface Expression extends Serializable {
    // 返回引用的字段名
    String fieldReference();

    // 返回计算结果的类型
    DataType outputType();

    // 计算表达式的值
    String eval(String input);
}
```

---

## 4. 配置和工具层

### 4.1 TypeMapping - 类型映射

**位置**: `TypeMapping.java`

**支持的模式**:

| 模式 | 说明 |
|------|------|
| `TINYINT1_NOT_BOOL` | MySQL TINYINT(1)映射为TINYINT而非BOOLEAN |
| `TO_NULLABLE` | 忽略所有NOT NULL约束（主键除外） |
| `TO_STRING` | 所有类型映射为STRING |
| `CHAR_TO_STRING` | CHAR/VARCHAR映射为STRING |
| `LONGTEXT_TO_BYTES` | LONGTEXT映射为BYTES |
| `DECIMAL_NO_CHANGE` | 禁止Decimal类型变更 |
| `BIGINT_UNSIGNED_TO_BIGINT` | BIGINT UNSIGNED映射为BIGINT |
| `ALLOW_NON_STRING_TO_STRING` | 允许任意类型转STRING |

**使用示例**:
```java
// 创建类型映射
TypeMapping typeMapping = TypeMapping.parse(new String[] {
    "tinyint1-not-bool",
    "to-string"
});

// 检查是否包含特定模式
if (typeMapping.containsMode(TypeMapping.TypeMappingMode.TO_STRING)) {
    // 使用STRING类型
}
```

### 4.2 CdcMetadataConverter - 元数据转换器

**位置**: `CdcMetadataConverter.java`

**内置转换器**:

| 转换器 | 列名 | 数据类型 | 来源 |
|--------|------|----------|------|
| `DatabaseNameConverter` | `database_name` | STRING NOT NULL | `source.db` |
| `TableNameConverter` | `table_name` | STRING NOT NULL | `source.table` |
| `SchemaNameConverter` | `schema_name` | STRING NOT NULL | `source.schema` |
| `OpTsConverter` | `op_ts` | TIMESTAMP_WITH_LOCAL_TIME_ZONE(3) NOT NULL | `source.ts_ms` |

**使用示例**:
```java
// 创建元数据转换器
CdcMetadataConverter[] metadataConverters = new CdcMetadataConverter[] {
    new CdcMetadataConverter.DatabaseNameConverter(),
    new CdcMetadataConverter.TableNameConverter(),
    new CdcMetadataConverter.OpTsConverter()
};

// 在Record解析时提取元数据
for (CdcMetadataConverter converter : metadataConverters) {
    String value = converter.read(sourceNode);
    String columnName = converter.columnName();
    DataType columnType = converter.dataType();
    // 添加到Schema和数据中
}
```

### 4.3 TableNameConverter - 表名转换器

**位置**: `TableNameConverter.java`

**转换优先级**:
1. `tableMapping` - 精确映射表名
2. `dbPrefix/dbSuffix` - 数据库级别前缀/后缀
3. `prefix/suffix` - 全局前缀/后缀

**核心方法**:
```java
public class TableNameConverter implements Serializable {
    private final boolean caseSensitive;
    private final boolean mergeShards;     // 是否合并分片
    private final Map<String, String> dbPrefix;   // 数据库前缀
    private final Map<String, String> dbSuffix;   // 数据库后缀
    private final String prefix;          // 全局前缀
    private final String suffix;          // 全局后缀
    private final Map<String, String> tableMapping; // 表名映射

    public String convert(String originDbName, String originTblName) {
        // 1. 最高优先级：tableMapping精确映射
        if (tableMapping.containsKey(originTblName.toLowerCase())) {
            String mappedName = tableMapping.get(originTblName.toLowerCase());
            return toLowerCaseIfNeed(mappedName, caseSensitive);
        }

        // 2. 第二优先级：数据库级别前缀/后缀
        String tblPrefix = prefix;
        String tblSuffix = suffix;
        if (dbPrefix.containsKey(originDbName.toLowerCase())) {
            tblPrefix = dbPrefix.get(originDbName.toLowerCase());
        }
        if (dbSuffix.containsKey(originDbName.toLowerCase())) {
            tblSuffix = dbSuffix.get(originDbName.toLowerCase());
        }

        // 3. 第三优先级：普通前缀/后缀
        String tableName = toLowerCaseIfNeed(originTblName, caseSensitive);
        return tblPrefix + tableName + tblSuffix;
    }

    public String convert(Identifier originIdentifier) {
        String rawName = mergeShards
            ? originIdentifier.getObjectName()
            : originIdentifier.getDatabaseName() + "_" + originIdentifier.getObjectName();
        return convert(originIdentifier.getDatabaseName(), rawName);
    }
}
```

**使用示例**:
```java
TableNameConverter converter = new TableNameConverter(
    caseSensitive,
    mergeShards,
    dbPrefix,    // {"db1": "prod_", "db2": "test_"}
    dbSuffix,    // {"db1": "_legacy"}
    "ods_",       // 全局前缀
    "",          // 全局后缀
    tableMapping // {"user": "user_info", "order": "order_detail"}
);

// 转换结果示例
// db1.orders -> ods_prod_orders_legacy (db前缀 + db后缀)
// db2.user -> ods_test_user (db前缀)
// db3.product -> ods_product (全局前缀)
// db4.user -> ods_user_info (tableMapping精确映射)
```

### 4.4 CdcActionCommonUtils - 通用工具类

**位置**: `CdcActionCommonUtils.java`

**核心功能**:

| 方法 | 功能 |
|------|------|
| `buildPaimonSchema()` | 构建Paimon表Schema |
| `schemaCompatible()` | 检查Schema兼容性 |
| `assertSchemaCompatible()` | 断言Schema兼容性 |
| `listCaseConvert()` | 大小写转换列表 |
| `checkDuplicateFields()` | 检查重复字段 |
| `tableList()` | 生成表列表正则表达式 |
| `checkRequiredOptions()` | 检查必填配置 |
| `checkOneRequiredOption()` | 检查互斥配置 |

**Schema构建流程**:
```java
public static Schema buildPaimonSchema(
        String tableName,
        List<String> specifiedPartitionKeys,
        List<String> specifiedPrimaryKeys,
        List<ComputedColumn> computedColumns,
        Map<String, String> tableConfig,
        Schema sourceSchema,
        CdcMetadataConverter[] metadataConverters,
        boolean caseSensitive,
        boolean strictlyCheckSpecified,
        boolean requirePrimaryKeys,
        boolean syncPKeysFromSourceSchema) {

    Schema.Builder builder = Schema.newBuilder();

    // 1. 合并配置选项
    builder.options(tableConfig);
    builder.options(sourceSchema.options());

    // 2. 添加源字段
    List<String> allFieldNames = new ArrayList<>();
    for (DataField field : sourceSchema.fields()) {
        String fieldName = toLowerCaseIfNeed(field.name(), caseSensitive);
        allFieldNames.add(fieldName);
        builder.column(fieldName, field.type(), field.description());
    }

    // 3. 添加计算列
    for (ComputedColumn computedColumn : computedColumns) {
        String computedColumnName = toLowerCaseIfNeed(computedColumn.columnName(), caseSensitive);
        allFieldNames.add(computedColumnName);
        builder.column(computedColumnName, computedColumn.columnType());
    }

    // 4. 添加元数据列
    for (CdcMetadataConverter metadataConverter : metadataConverters) {
        String metadataColumnName = toLowerCaseIfNeed(metadataConverter.columnName(), caseSensitive);
        allFieldNames.add(metadataColumnName);
        builder.column(metadataColumnName, metadataConverter.dataType());
    }

    // 5. 检查重复字段
    checkDuplicateFields(tableName, allFieldNames);

    // 6. 设置主键
    setPrimaryKeys(...);

    // 7. 设置分区键
    setPartitionKeys(...);

    // 8. 设置注释
    builder.comment(sourceSchema.comment());

    return builder.build();
}
```

**兼容性检查**:
```java
public static boolean schemaCompatible(
        TableSchema paimonSchema, List<DataField> sourceTableFields) {
    for (DataField field : sourceTableFields) {
        int idx = paimonSchema.fieldNames().indexOf(field.name());
        if (idx < 0) {
            LOG.info("Cannot find field '{}' in Paimon table.", field.name());
            return false;
        }
        DataType type = paimonSchema.fields().get(idx).type();
        if (UpdatedDataFieldsProcessFunction.canConvert(
                field.type(), type, TypeMapping.defaultMapping())
                != UpdatedDataFieldsProcessFunction.ConvertAction.CONVERT) {
            LOG.info("Cannot convert field '{}' from source type '{}' to Paimon type '{}'.",
                field.name(), field.type(), type);
            return false;
        }
    }
    return true;
}
```

---

## 5. 同步任务处理层

### 5.1 SyncJobHandler - 同步任务处理器

**位置**: `SyncJobHandler.java`

**SourceType枚举**:
```java
public enum SourceType {
    MYSQL("MySQL Source", "MySQL-Paimon %s Sync: %s"),
    KAFKA("Kafka Source", "Kafka-Paimon %s Sync: %s"),
    MONGODB("MongoDB Source", "MongoDB-Paimon %s Sync: %s"),
    PULSAR("Pulsar Source", "Pulsar-Paimon %s Sync: %s"),
    POSTGRES("Postgres Source", "Postgres-Paimon %s Sync: %s");

    private final String sourceName;
    private final String defaultJobNameFormat;
}
```

**核心方法**:
```java
public class SyncJobHandler {
    private final SourceType sourceType;
    private final Configuration cdcSourceConfig;
    private final boolean isTableSync;
    private final String sinkLocation;

    // 提供CDC Source
    public Source<CdcSourceRecord, ?, ?> provideSource() {
        switch (sourceType) {
            case KAFKA:
                return KafkaActionUtils.buildKafkaSource(
                    cdcSourceConfig,
                    provideDataFormat().createKafkaDeserializer(cdcSourceConfig)
                );
            case PULSAR:
                return PulsarActionUtils.buildPulsarSource(
                    cdcSourceConfig,
                    provideDataFormat().createPulsarDeserializer(cdcSourceConfig)
                );
            default:
                throw new UnsupportedOperationException(
                    "Cannot get source from source type" + sourceType);
        }
    }

    // 提供Record解析器
    public FlatMapFunction<CdcSourceRecord, RichCdcMultiplexRecord> provideRecordParser(
            List<ComputedColumn> computedColumns,
            TypeMapping typeMapping,
            CdcMetadataConverter[] metadataConverters) {
        switch (sourceType) {
            case MYSQL:
                return new MySqlRecordParser(
                    cdcSourceConfig, computedColumns, typeMapping, metadataConverters);
            case POSTGRES:
                return new PostgresRecordParser(
                    cdcSourceConfig, computedColumns, typeMapping, metadataConverters);
            case KAFKA:
            case PULSAR:
                DataFormat dataFormat = provideDataFormat();
                return dataFormat.createParser(typeMapping, computedColumns);
            case MONGODB:
                return new MongoDBRecordParser(computedColumns, cdcSourceConfig);
            default:
                throw new UnsupportedOperationException("Unknown source type " + sourceType);
        }
    }

    // 提供数据格式
    public DataFormat provideDataFormat() {
        switch (sourceType) {
            case KAFKA:
                return KafkaActionUtils.getDataFormat(cdcSourceConfig);
            case PULSAR:
                return PulsarActionUtils.getDataFormat(cdcSourceConfig);
            default:
                throw new UnsupportedOperationException(
                    "Cannot get DataFormat from source type" + sourceType);
        }
    }

    // 检查必填配置
    public void checkRequiredOption() {
        switch (sourceType) {
            case MYSQL:
                checkRequiredOptions(cdcSourceConfig, MYSQL_CONF,
                    MySqlSourceOptions.HOSTNAME,
                    MySqlSourceOptions.USERNAME,
                    MySqlSourceOptions.PASSWORD,
                    MySqlSourceOptions.DATABASE_NAME);
                if (isTableSync) {
                    checkRequiredOptions(cdcSourceConfig, MYSQL_CONF,
                        MySqlSourceOptions.TABLE_NAME);
                }
                break;
            case KAFKA:
                checkRequiredOptions(cdcSourceConfig, KAFKA_CONF,
                    KafkaConnectorOptions.VALUE_FORMAT,
                    KafkaConnectorOptions.PROPS_BOOTSTRAP_SERVERS);
                checkOneRequiredOption(cdcSourceConfig, KAFKA_CONF,
                    KafkaConnectorOptions.TOPIC,
                    KafkaConnectorOptions.TOPIC_PATTERN);
                break;
            // ... 其他数据源类型
        }
    }

    // 提供元数据转换器
    public CdcMetadataConverter provideMetadataConverter(String column) {
        return CdcMetadataProcessor.converter(sourceType, column);
    }
}
```

---

## 6. Action基类层

### 6.1 SynchronizationActionBase - 同步Action基类

**位置**: `SynchronizationActionBase.java`

**核心构建流程**:
```java
public abstract class SynchronizationActionBase extends ActionBase {

    protected final String database;
    protected final Configuration cdcSourceConfig;
    protected final SyncJobHandler syncJobHandler;
    protected final boolean caseSensitive;
    protected Map<String, String> tableConfig = new HashMap<>();
    protected TypeMapping typeMapping = TypeMapping.defaultMapping();
    protected boolean syncPKeysFromSourceSchema = true;
    protected CdcMetadataConverter[] metadataConverters = new CdcMetadataConverter[] {};

    @Override
    public void build() throws Exception {
        // 1. 检查必填配置
        syncJobHandler.checkRequiredOption();

        // 2. 创建数据库
        catalog.createDatabase(database, true);

        // 3. 子类扩展点
        beforeBuildingSourceSink();

        // 4. 构建Source -> 解析
        DataStream<RichCdcMultiplexRecord> input =
            buildDataStreamSource(buildSource())
                .flatMap(recordParse())
                .name("Parse");

        // 5. 构建Event解析器工厂
        EventParser.Factory<RichCdcMultiplexRecord> parserFactory = buildEventParserFactory();

        // 6. 构建Sink
        buildSink(input, parserFactory);
    }

    // 构建Source
    protected Source<CdcSourceRecord, ?, ?> buildSource() {
        return syncJobHandler.provideSource();
    }

    // 抽象方法：记录解析
    protected abstract FlatMapFunction<CdcSourceRecord, RichCdcMultiplexRecord> recordParse();

    // 抽象方法：构建Event解析器工厂
    protected abstract EventParser.Factory<RichCdcMultiplexRecord> buildEventParserFactory();

    // 抽象方法：构建Sink
    protected abstract void buildSink(
        DataStream<RichCdcMultiplexRecord> input,
        EventParser.Factory<RichCdcMultiplexRecord> parserFactory);

    // 构建DataStreamSource
    private DataStreamSource<CdcSourceRecord> buildDataStreamSource(
            Source<CdcSourceRecord, ?, ?> source) {
        // 判断是否启用自动水位线
        boolean isAutomaticWatermarkCreationEnabled =
            tableConfig.containsKey(CoreOptions.TAG_AUTOMATIC_CREATION.key())
                && Objects.equals(
                    tableConfig.get(CoreOptions.TAG_AUTOMATIC_CREATION.key()),
                    WATERMARK.toString());

        Options options = Options.fromMap(tableConfig);
        Duration idleTimeout = options.get(SCAN_WATERMARK_IDLE_TIMEOUT);
        String watermarkAlignGroup = options.get(SCAN_WATERMARK_ALIGNMENT_GROUP);

        WatermarkStrategy<CdcSourceRecord> watermarkStrategy =
            isAutomaticWatermarkCreationEnabled
                ? watermarkAlignGroup != null
                    ? new CdcWatermarkStrategy(createCdcTimestampExtractor())
                        .withWatermarkAlignment(
                            watermarkAlignGroup,
                            options.get(SCAN_WATERMARK_ALIGNMENT_MAX_DRIFT),
                            options.get(SCAN_WATERMARK_ALIGNMENT_UPDATE_INTERVAL))
                    : new CdcWatermarkStrategy(createCdcTimestampExtractor())
                : WatermarkStrategy.noWatermarks();

        if (idleTimeout != null) {
            watermarkStrategy = watermarkStrategy.withIdleness(idleTimeout);
        }

        return env.fromSource(source, watermarkStrategy, syncJobHandler.provideSourceName());
    }
}
```

### 6.2 SyncTableActionBase - 表同步基类

**位置**: `SyncTableActionBase.java`

**核心流程**:
```java
public abstract class SyncTableActionBase extends SynchronizationActionBase {

    protected final String table;
    protected FileStoreTable fileStoreTable;
    protected List<String> partitionKeys = new ArrayList<>();
    protected List<String> primaryKeys = new ArrayList<>();
    protected List<String> computedColumnArgs = new ArrayList<>();
    protected List<ComputedColumn> computedColumns = new ArrayList<>();

    // 抽象方法：获取Schema
    protected abstract Schema retrieveSchema() throws Exception;

    // 构建Paimon Schema
    protected Schema buildPaimonSchema(Schema retrievedSchema) {
        return CdcActionCommonUtils.buildPaimonSchema(
            table,
            partitionKeys,
            primaryKeys,
            computedColumns,
            tableConfig,
            retrievedSchema,
            metadataConverters,
            caseSensitive,
            true,   // strictlyCheckSpecified
            true,   // requirePrimaryKeys
            this.syncPKeysFromSourceSchema
        );
    }

    @Override
    protected void beforeBuildingSourceSink() throws Exception {
        Identifier identifier = new Identifier(database, table);

        try {
            // 表存在的情况
            fileStoreTable = (FileStoreTable) catalog.getTable(identifier);
            fileStoreTable = alterTableOptions(identifier, fileStoreTable);

            try {
                // 尝试获取Schema并检查兼容性
                Schema retrievedSchema = retrieveSchema();
                computedColumns = buildComputedColumns(
                    computedColumnArgs, retrievedSchema.fields()
                );
                Schema paimonSchema = buildPaimonSchema(retrievedSchema);
                assertSchemaCompatible(fileStoreTable.schema(), paimonSchema.fields());
            } catch (SchemaRetrievalException e) {
                // Schema获取失败，使用现有表Schema构建计算列
                computedColumns = buildComputedColumns(
                    computedColumnArgs,
                    fileStoreTable.schema().fields(),
                    caseSensitive
                );
                checkConstraints();
            }
        } catch (Catalog.TableNotExistException e) {
            // 表不存在，创建新表
            Schema retrievedSchema = retrieveSchema();
            computedColumns = buildComputedColumns(computedColumnArgs, retrievedSchema.fields());
            Schema paimonSchema = buildPaimonSchema(retrievedSchema);
            catalog.createTable(identifier, paimonSchema, false);
            fileStoreTable = (FileStoreTable) catalog.getTable(identifier);
        }
    }

    @Override
    protected FlatMapFunction<CdcSourceRecord, RichCdcMultiplexRecord> recordParse() {
        return syncJobHandler.provideRecordParser(
            computedColumns, typeMapping, metadataConverters
        );
    }

    @Override
    protected EventParser.Factory<RichCdcMultiplexRecord> buildEventParserFactory() {
        boolean caseSensitive = this.caseSensitive;
        return () -> new RichCdcMultiplexRecordEventParser(caseSensitive);
    }

    @Override
    protected void buildSink(
            DataStream<RichCdcMultiplexRecord> input,
            EventParser.Factory<RichCdcMultiplexRecord> parserFactory) {
        CdcSinkBuilder<RichCdcMultiplexRecord> sinkBuilder =
            new CdcSinkBuilder<RichCdcMultiplexRecord>()
                .withInput(input)
                .withParserFactory(parserFactory)
                .withTable(fileStoreTable)
                .withIdentifier(new Identifier(database, table))
                .withTypeMapping(typeMapping)
                .withCatalogLoader(catalogLoader());

        String sinkParallelism = tableConfig.get(
            FlinkConnectorOptions.SINK_PARALLELISM.key()
        );
        if (sinkParallelism != null) {
            sinkBuilder.withParallelism(Integer.parseInt(sinkParallelism));
        }
        sinkBuilder.build();
    }

    private void checkConstraints() {
        // 检查分区键
        if (!partitionKeys.isEmpty()) {
            List<String> actualPartitionKeys = fileStoreTable.partitionKeys();
            checkState(
                actualPartitionKeys.size() == partitionKeys.size()
                    && actualPartitionKeys.containsAll(partitionKeys),
                "Specified partition keys [%s] are not equal to the existed table partition keys [%s].",
                String.join(",", partitionKeys),
                String.join(",", actualPartitionKeys)
            );
        }

        // 检查主键
        if (!primaryKeys.isEmpty()) {
            List<String> actualPrimaryKeys = fileStoreTable.primaryKeys();
            checkState(
                actualPrimaryKeys.size() == primaryKeys.size()
                    && actualPrimaryKeys.containsAll(primaryKeys),
                "Specified primary keys [%s] are not equal to the existed table primary keys [%s].",
                String.join(",", primaryKeys),
                String.join(",", actualPrimaryKeys)
            );
        }
    }
}
```

---

## 7. MessageQueueSchemaUtils - Schema获取工具

**位置**: `MessageQueueSchemaUtils.java`

**核心功能**:
```java
public class MessageQueueSchemaUtils {

    private static final int MAX_RETRY = 5;
    private static final int POLL_TIMEOUT_MILLIS = 1000;

    /**
     * 从消息队列获取Schema
     * 通过消费消息并解析来推断Schema
     */
    public static Schema getSchema(
            ConsumerWrapper consumer,
            DataFormat dataFormat,
            TypeMapping typeMapping) throws SchemaRetrievalException {

        int retry = 0;
        int retryInterval = 1000;

        AbstractRecordParser recordParser =
            dataFormat.createParser(typeMapping, Collections.emptyList());

        while (true) {
            // 1. 消费消息
            Optional<Schema> schema =
                consumer.getRecords(POLL_TIMEOUT_MILLIS).stream()
                    .map(recordParser::buildSchema)
                    .filter(Objects::nonNull)
                    .findFirst();

            // 2. 成功获取Schema
            if (schema.isPresent()) {
                return schema.get();
            }

            // 3. 达到最大重试次数
            if (retry >= MAX_RETRY) {
                throw new SchemaRetrievalException(
                    String.format(
                        "Could not get metadata from server, topic: %s. " +
                        "If this topic is not empty, please check the configuration. " +
                        "Otherwise, you should create the Paimon table first.",
                        consumer.topic()
                    )
                );
            }

            // 4. 等待并重试
            sleepSafely(retryInterval);
            retryInterval *= 2;  // 指数退避
            retry++;
        }
    }

    /** Consumer包装接口 */
    public interface ConsumerWrapper extends AutoCloseable {
        List<CdcSourceRecord> getRecords(int pollTimeOutMills);
        String topic();
    }
}
```

**使用场景**:
- 表同步时，如果Paimon表不存在，需要从Kafka Topic推断Schema
- 通过消费最早的几条消息来解析Schema
- 支持指数退避重试机制

---

## 8. ComputedColumnUtils - 计算列工具类

**位置**: `ComputedColumnUtils.java`

**核心功能**:
```java
public class ComputedColumnUtils {

    /**
     * 构建计算列列表
     * 支持计算列之间的依赖关系
     */
    public static List<ComputedColumn> buildComputedColumns(
            List<String> computedColumnArgs,
            List<DataField> physicFields,
            boolean caseSensitive) {

        // 1. 构建类型映射
        Map<String, DataType> typeMapping = physicFields.stream()
            .collect(Collectors.toMap(DataField::name, DataField::type));

        // 2. 按依赖关系排序计算列
        LinkedHashMap<String, Tuple2<String, String[]>> sortedArgs =
            sortComputedColumnArgs(computedColumnArgs, caseSensitive);

        // 3. 创建计算列
        List<ComputedColumn> computedColumns = new ArrayList<>();
        for (Map.Entry<String, Tuple2<String, String[]>> columnArg : sortedArgs.entrySet()) {
            String columnName = columnArg.getKey().trim();
            String exprName = columnArg.getValue().f0.trim();
            String[] args = columnArg.getValue().f1;

            Expression expr = Expression.create(typeMapping, caseSensitive, exprName, args);
            ComputedColumn cmpColumn = new ComputedColumn(columnName, expr);
            computedColumns.add(cmpColumn);

            // 4. 更新类型映射（支持计算列引用其他计算列）
            typeMapping.put(columnName, cmpColumn.columnType());
        }

        return computedColumns;
    }

    /**
     * 按依赖关系排序计算列
     * 使用拓扑排序（DfsSort）
     */
    private static LinkedHashMap<String, Tuple2<String, String[]>> sortComputedColumnArgs(
            List<String> computedColumnArgs,
            boolean caseSensitive) {

        // 解析计算列参数
        LinkedHashMap<String, Tuple2<String, String[]>> eqMap = new LinkedHashMap<>();
        LinkedHashMap<String, String> refMap = new LinkedHashMap<>();

        for (String arg : computedColumnArgs) {
            String[] kv = arg.split("=");
            checkArgument(kv.length == 2,
                "Invalid computed column argument: %s. Please use format 'column-name=expr-name(args, ...)'",
                arg);

            String expression = kv[1].trim();
            int left = expression.indexOf('(');
            int right = expression.indexOf(')');

            checkArgument(left > 0 && right > left,
                "Invalid expression: %s. Please use format 'expr-name(args, ...)'", expression);

            String exprName = expression.substring(0, left);
            String[] args = expression.substring(left + 1, right).split(",");

            eqMap.put(kv[0].trim(), Tuple2.of(exprName, args));
            refMap.put(kv[0].trim(), args[0].trim());  // 第一个参数是依赖字段
        }

        // 拓扑排序
        List<String> sortedKeys = DfsSort.sortKeys(refMap);

        LinkedHashMap<String, Tuple2<String, String[]>> sortedMap =
            new LinkedHashMap<>(refMap.size());
        for (String key : sortedKeys) {
            sortedMap.put(key, eqMap.get(key));
        }
        return sortedMap;
    }
}
```

**依赖解析示例**:
```java
// 计算列参数
List<String> computedColumnArgs = Arrays.asList(
    "year=year(event_time)",
    "month=month(event_time)",
    "pt=date_format(event_time, yyyyMMdd)",
    "pt_full=concat(pt, '_', year)"
);

// 排序后（按依赖关系）
// 1. year (依赖 event_time)
// 2. month (依赖 event_time)
// 3. pt (依赖 event_time)
// 4. pt_full (依赖 pt, year)
```

---

## 9. 核心数据流

### 9.1 表同步数据流

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                        SyncTableActionBase数据流                                    │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                     │
│  用户调用 build()                                                                   │
│      │                                                                             │
│      ▼                                                                             │
│  ┌──────────────────────────────────────────────────────────────────────────────┐  │
│  │  beforeBuildingSourceSink()                                                   │  │
│  │    ┌────────────────────────────────────────────────────────────────────────┐  │  │
│  │    │  表存在?                                                                │  │  │
│  │    │    是:                                                                  │  │  │
│  │    │      1. alterTableOptions()                                            │  │  │
│  │    │      2. retrieveSchema() -> 获取源Schema                              │  │  │
│  │    │      3. buildComputedColumns() -> 构建计算列                          │  │  │
│  │    │      4. buildPaimonSchema() -> 构建Paimon Schema                      │  │  │
│  │    │      5. assertSchemaCompatible() -> 检查兼容性                        │  │  │
│  │    │    否:                                                                  │  │  │
│  │    │      1. retrieveSchema()                                               │  │  │
│  │    │      2. buildPaimonSchema()                                           │  │  │
│  │    │      3. catalog.createTable() -> 创建表                                │  │  │
│  │    └────────────────────────────────────────────────────────────────────────┘  │  │
│  └──────────────────────────────────────────────────────────────────────────────┘  │
│      │                                                                             │
│      ▼                                                                             │
│  ┌──────────────────────────────────────────────────────────────────────────────┐  │
│  │  buildSource() -> SyncJobHandler.provideSource()                            │  │
│  │    -> Kafka/Pulsar Source                                                    │  │
│  └──────────────────────────────────────────────────────────────────────────────┘  │
│      │                                                                             │
│      ▼                                                                             │
│  ┌──────────────────────────────────────────────────────────────────────────────┐  │
│  │  buildDataStreamSource()                                                     │  │
│  │    -> 配置WatermarkStrategy                                                  │  │
│  └──────────────────────────────────────────────────────────────────────────────┘  │
│      │                                                                             │
│      ▼                                                                             │
│  ┌──────────────────────────────────────────────────────────────────────────────┐  │
│  │  recordParse() -> SyncJobHandler.provideRecordParser()                       │  │
│  │    -> DebeziumJsonRecordParser                                              │  │
│  │    CdcSourceRecord -> RichCdcMultiplexRecord                                 │  │
│  └──────────────────────────────────────────────────────────────────────────────┘  │
│      │                                                                             │
│      ▼                                                                             │
│  ┌──────────────────────────────────────────────────────────────────────────────┐  │
│  │  buildEventParserFactory()                                                   │  │
│  │    -> RichCdcMultiplexRecordEventParser                                     │  │
│  └──────────────────────────────────────────────────────────────────────────────┘  │
│      │                                                                             │
│      ▼                                                                             │
│  ┌──────────────────────────────────────────────────────────────────────────────┐  │
│  │  buildSink()                                                                  │  │
│  │    -> CdcSinkBuilder                                                         │  │
│  │    -> CdcDynamicTableParsingProcessFunction                                 │  │
│  │    -> CdcRecordStoreMultiWriteOperator                                      │  │
│  │    -> StoreMultiCommitter                                                  │  │
│  └──────────────────────────────────────────────────────────────────────────────┘  │
│      │                                                                             │
│      ▼                                                                             │
│   Paimon Storage                                                                  │
│                                                                                     │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

### 9.2 数据记录转换流

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                        数据记录转换流                                               │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                     │
│  Kafka/Pulsar Message                                                              │
│  ┌──────────────────────────────────────────────────────────────────────────────┐  │
│  │  JSON/Avro/BSON 格式的Debezium消息                                            │  │
│  └──────────────────────────────────────────────────────────────────────────────┘  │
│                          │                                                         │
│                          ▼                                                         │
│  CdcSourceRecord                                                                   │
│  ┌──────────────────────────────────────────────────────────────────────────────┐  │
│  │  topic: "order_topic"                                                        │  │
│  │  key: null                                                                    │  │
│  │  value: JsonNode (Debezium JSON)                                            │  │
│  └──────────────────────────────────────────────────────────────────────────────┘  │
│                          │                                                         │
│                          ▼                                                         │
│  DebeziumJsonRecordParser.extractRecords()                                       │
│  ┌──────────────────────────────────────────────────────────────────────────────┐  │
│  │  - 解析op类型 (c/r/u/d)                                                        │  │
│  │  - 提取before/after数据                                                        │  │
│  │  - 生成CdcRecord (RowKind + Map<String, String>)                             │  │
│  └──────────────────────────────────────────────────────────────────────────────┘  │
│                          │                                                         │
│                          ▼                                                         │
│  RichCdcMultiplexRecord                                                             │
│  ┌──────────────────────────────────────────────────────────────────────────────┐  │
│  │  - databaseName: "mydb"                                                      │  │
│  │  - tableName: "orders"                                                       │  │
│  │  - record: CdcRecord                                                          │  │
│  │  - cdcSchema: CdcSchema                                                      │  │
│  └──────────────────────────────────────────────────────────────────────────────┘  │
│                          │                                                         │
│                          ▼                                                         │
│  计算列计算                                                                        │
│  ┌──────────────────────────────────────────────────────────────────────────────┐  │
│  │  for (ComputedColumn cc : computedColumns):                                 │  │
│  │    String value = cc.eval(record.data().get(cc.fieldReference()));         │  │
│  │    record.data().put(cc.columnName(), value);                              │  │
│  └──────────────────────────────────────────────────────────────────────────────┘  │
│                          │                                                         │
│                          ▼                                                         │
│  元数据列提取                                                                      │
│  ┌──────────────────────────────────────────────────────────────────────────────┐  │
│  │  for (CdcMetadataConverter converter : metadataConverters):               │  │
│  │    String value = converter.read(sourceNode);                               │  │
│  │    record.data().put(converter.columnName(), value);                        │  │
│  └──────────────────────────────────────────────────────────────────────────────┘  │
│                          │                                                         │
│                          ▼                                                         │
│  CdcRecord (包含计算列和元数据列)                                                  │
│                          │                                                         │
│                          ▼                                                         │
│   CdcDynamicTableParsingProcessFunction                                         │
│                          │                                                         │
│                          ▼                                                         │
│   CdcRecordStoreMultiWriteOperator                                              │
│                          │                                                         │
│                          ▼                                                         │
│   toGenericRow() + StoreSinkWrite.write()                                      │
│                          │                                                         │
│                          ▼                                                         │
│   Paimon Storage                                                                  │
│                                                                                     │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 10. 总结

CDC Action核心模块的关键特性：

1. **分层架构**: 数据定义 -> 配置工具 -> 任务处理 -> Action基类
2. **类型安全**: 通过TypeMapping控制类型转换规则
3. **Schema推断**: 从消息队列消息自动推断表结构
4. **计算列支持**: 支持表达式计算和依赖解析
5. **元数据提取**: 支持database_name/table_name等元数据列
6. **表名转换**: 灵活的前缀/后缀/映射配置
7. **兼容性检查**: 自动检查Schema兼容性

**关键数据流**:
```
CdcSourceRecord -> RecordParser -> RichCdcMultiplexRecord
    -> (计算列 + 元数据列) -> CdcRecord -> toGenericRow()
    -> StoreSinkWrite -> Paimon Storage
```
