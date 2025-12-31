# Paimon Flink CDC Schema演变处理机制分析

> **文档版本**: v1.0
> **分析日期**: 2025-12-24
> **模块路径**: `/paimon-flink/paimon-flink-cdc/src/main/java/org/apache/paimon/flink/sink/cdc`

---

## 1. 概述

Schema演变（Schema Evolution）是指当源数据库（如MySQL）发生DDL变更时，Paimon表结构能够自动跟随变更的能力。Paimon CDC通过监听Debezium数据流中的Schema变化事件，自动执行相应的DDL操作，保持与源数据库的表结构同步。

---

## 2. Schema演变架构

### 2.1 整体架构图

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                        Schema演变处理架构                                            │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                     │
│  MySQL DDL操作                                                                     │
│  ┌──────────────┐      ┌──────────────┐      ┌──────────────┐                      │
│  │ ALTER TABLE  │──────▶│ Debezium     │──────▶│  Kafka Topic │                      │
│  │  ADD COLUMN  │      │  Capture     │      │              │                      │
│  └──────────────┘      └──────────────┘      └──────────────┘                      │
│                                                          │                          │
│                                                          ▼                          │
│  ┌──────────────────────────────────────────────────────────────────────────────┐  │
│  │  1. RichEventParser.parseSchemaChange()                                     │  │
│  │     - 维护previousDataFields ( LinkedHashMap )                               │  │
│  │     - 比较新Schema与previousDataFields                                        │  │
│  │     - 生成CdcSchema变更                                                      │  │
│  └──────────────────────────────────────────────────────────────────────────────┘  │
│                                                          │                          │
│                                                          ▼                          │
│  ┌──────────────────────────────────────────────────────────────────────────────┐  │
│  │  2. RichCdcMultiplexRecordEventParser.parseSchemaChange()                    │  │
│  │     - 表过滤检查 (shouldSynchronizeCurrentTable)                              │  │
│  │     - 调用RichEventParser解析                                                 │  │
│  └──────────────────────────────────────────────────────────────────────────────┘  │
│                                                          │                          │
│                                                          ▼                          │
│  ┌──────────────────────────────────────────────────────────────────────────────┐  │
│  │  3. CdcDynamicTableParsingProcessFunction                                    │  │
│  │     - 输出到DYNAMIC_SCHEMA_CHANGE_OUTPUT_TAG                                 │  │
│  │     - Tuple2<Identifier, CdcSchema>                                          │  │
│  └──────────────────────────────────────────────────────────────────────────────┘  │
│                                                          │                          │
│                                                          ▼                          │
│  ┌──────────────────────────────────────────────────────────────────────────────┐  │
│  │  4. MultiTableUpdatedDataFieldsProcessFunction (并行度=1)                     │  │
│  │     ┌────────────────────────────────────────────────────────────────────┐   │  │
│  │     │ a. actualUpdatedDataFields()                                        │   │  │
│  │     │    - 过滤已处理的字段，避免重复变更                                   │   │  │
│  │     │    - 使用latestFields缓存追踪已处理字段                              │   │  │
│  │     └────────────────────────────────────────────────────────────────────┘   │  │
│  │     ┌────────────────────────────────────────────────────────────────────┐   │  │
│  │     │ b. extractSchemaChanges()                                          │   │  │
│  │     │    - 比较新旧Schema，生成SchemaChange列表                           │   │  │
│  │     │    - 支持嵌套类型变更 (NestedSchemaUtils)                            │   │  │
│  │     └────────────────────────────────────────────────────────────────────┘   │  │
│  │     ┌────────────────────────────────────────────────────────────────────┐   │  │
│  │     │ c. canConvert()                                                    │   │  │
│  │     │    - 检查类型兼容性                                                 │   │  │
│  │     │    - 返回CONVERT/IGNORE/EXCEPTION                                   │   │  │
│  │     └────────────────────────────────────────────────────────────────────┘   │  │
│  │     ┌────────────────────────────────────────────────────────────────────┐   │  │
│  │     │ d. applySchemaChange()                                             │   │  │
│  │     │    - 调用catalog.alterTable()执行DDL                               │   │  │
│  │     └────────────────────────────────────────────────────────────────────┘   │  │
│  └──────────────────────────────────────────────────────────────────────────────┘  │
│                                                          │                          │
│                                                          ▼                          │
│  ┌──────────────────────────────────────────────────────────────────────────────┐  │
│  │  5. Paimon Catalog                                                          │  │
│  │     - SchemaManager更新Schema                                                │  │
│  │     - 更新元数据文件                                                          │  │
│  └──────────────────────────────────────────────────────────────────────────────┘  │
│                                                          │                          │
│                                                          ▼                          │
│  ┌──────────────────────────────────────────────────────────────────────────────┐  │
│  │  6. CdcRecordStoreMultiWriteOperator                                        │  │
│  │     - replace(latestTable)刷新Table引用                                      │  │
│  │     - 使用新Schema继续处理数据                                                │  │
│  └──────────────────────────────────────────────────────────────────────────────┘  │
│                                                                                     │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

### 2.2 核心类说明

| 类名 | 职责 |
|------|------|
| `RichEventParser` | 单表Schema变化检测，维护previousDataFields |
| `RichCdcMultiplexRecordEventParser` | 多表Schema变化检测，应用表过滤规则 |
| `CdcDynamicTableParsingProcessFunction` | 解析并分发Schema变更事件 |
| `MultiTableUpdatedDataFieldsProcessFunction` | Schema变更处理，执行DDL |
| `UpdatedDataFieldsProcessFunctionBase` | Schema变更处理基类，类型兼容性检查 |
| `NewTableSchemaBuilder` | 新表Schema构建 |

---

## 3. Schema变化检测机制

### 3.1 RichEventParser变化检测

**位置**: `RichEventParser.java:42-64`

```java
@Override
public CdcSchema parseSchemaChange() {
    CdcSchema.Builder change = CdcSchema.newBuilder();
    CdcSchema recordedSchema = record.cdcSchema();

    // 遍历新Schema的所有字段
    recordedSchema.fields().forEach(dataField -> {
        DataField previous = previousDataFields.get(dataField.name());

        // 比较字段是否发生变化（忽略Field ID）
        if (!DataField.dataFieldEqualsIgnoreId(previous, dataField)) {
            previousDataFields.put(dataField.name(), dataField);
            change.column(dataField);  // 添加到变更中
        }
    });

    // 检查注释变更
    if (recordedSchema.comment() != null &&
        !recordedSchema.comment().equals(previousComment)) {
        previousComment = recordedSchema.comment();
        change.comment(recordedSchema.comment());
    }

    return change.build();
}
```

**关键点**:
1. 使用`LinkedHashMap`维护字段历史记录
2. 通过`dataFieldEqualsIgnoreId()`比较字段（忽略自动生成的Field ID）
3. 只有真正发生变化的字段才会被加入到变更中

### 3.2 变化检测流程图

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                      Schema变化检测流程                                              │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                     │
│  RichCdcMultiplexRecord                                                             │
│  ┌──────────────────────────────────────────────────────────────────────────────┐  │
│  │  - databaseName: String                                                     │  │
│  │  - tableName: String                                                        │  │
│  │  - cdcSchema: CdcSchema (新Schema)                                          │  │
│  │  - rowData: Map<String, String>                                             │  │
│  └──────────────────────────────────────────────────────────────────────────────┘  │
│                                      │                                             │
│                                      ▼                                             │
│  RichCdcMultiplexRecordEventParser.parseSchemaChange()                            │
│  ┌──────────────────────────────────────────────────────────────────────────────┐  │
│  │  1. 检查表是否需要同步                                                        │  │
│  │     shouldSynchronizeCurrentTable()                                          │  │
│  │       - 匹配including/excluding模式                                          │  │
│  │       - 检查db级别过滤                                                        │  │
│  │                                                                               │  │
│  │  2. 获取或创建表级Parser                                                       │  │
│  │     currentParser = parsers.computeIfAbsent(tableName, t -> new RichEventParser())│
│  │                                                                               │  │
│  │  3. 调用表级Parser检测变化                                                     │  │
│  │     currentParser.parseSchemaChange()                                         │  │
│  └──────────────────────────────────────────────────────────────────────────────┘  │
│                                      │                                             │
│                                      ▼                                             │
│  RichEventParser.parseSchemaChange()                                               │
│  ┌──────────────────────────────────────────────────────────────────────────────┐  │
│  │  for each field in recordedSchema.fields():                                  │  │
│  │    previous = previousDataFields.get(fieldName)                               │  │
│  │    if (!dataFieldEqualsIgnoreId(previous, newField)):                         │  │
│  │        change.column(newField)  // 字段变更                                   │  │
│  │                                                                               │  │
│  │  if (comment changed):                                                        │  │
│  │    change.comment(newComment)  // 注释变更                                    │  │
│  └──────────────────────────────────────────────────────────────────────────────┘  │
│                                      │                                             │
│                                      ▼                                             │
│  CdcSchema (变更描述)                                                              │
│  ┌──────────────────────────────────────────────────────────────────────────────┐  │
│  │  - fields: List<DataField> (变更的字段列表)                                  │  │
│  │  - primaryKeys: List<String>                                                 │  │
│  │  - comment: String                                                           │  │
│  └──────────────────────────────────────────────────────────────────────────────┘  │
│                                                                                     │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 4. Schema变更提取

### 4.1 extractSchemaChanges流程

**位置**: `UpdatedDataFieldsProcessFunctionBase.java:348-410`

```java
protected List<SchemaChange> extractSchemaChanges(
        SchemaManager schemaManager, CdcSchema updatedSchema) {

    // 1. 获取当前表的Schema
    TableSchema oldTableSchema = schemaManager.latest().get();
    RowType oldRowType = oldTableSchema.logicalRowType();
    Map<String, DataField> oldFields = new HashMap<>();
    for (DataField oldField : oldRowType.getFields()) {
        oldFields.put(oldField.name(), oldField);
    }

    List<SchemaChange> result = new ArrayList<>();

    // 2. 遍历新Schema的所有字段
    for (DataField newField : updatedSchema.fields()) {
        String newFieldName = toLowerCaseIfNeed(newField.name(), caseSensitive);

        if (oldFields.containsKey(newFieldName)) {
            // 字段已存在，检查是否需要修改
            DataField oldField = oldFields.get(newFieldName);

            if (oldField.type().copy(true).equalsIgnoreFieldId(newField.type().copy(true))) {
                // 类型相同，检查注释
                if (newField.description() != null &&
                    !newField.description().equals(oldField.description())) {
                    result.add(SchemaChange.updateColumnComment(
                        new String[] {newFieldName}, newField.description()));
                }
            } else {
                // 类型不同，生成类型更新
                if (allowDecimalTypeChange || !oldField.type().is(DataTypeRoot.DECIMAL)) {
                    NestedSchemaUtils.generateNestedColumnUpdates(
                        Collections.singletonList(newFieldName),
                        oldField.type(),
                        newField.type(),
                        result
                    );
                }
            }
        } else {
            // 字段不存在，添加新列
            result.add(SchemaChange.addColumn(
                newFieldName, newField.type(), newField.description(), null));
        }
    }

    // 3. 检查表注释变更
    if (updatedSchema.comment() != null &&
        !updatedSchema.comment().equals(oldTableSchema.comment())) {
        result.add(SchemaChange.updateComment(updatedSchema.comment()));
    }

    return result;
}
```

### 4.2 SchemaChange类型

| SchemaChange类型 | 说明 | 生成条件 |
|------------------|------|----------|
| `SchemaChange.AddColumn` | 添加列 | 新字段在旧Schema中不存在 |
| `SchemaChange.UpdateColumnType` | 修改列类型 | 字段存在但类型不同（兼容类型） |
| `SchemaChange.UpdateColumnComment` | 修改列注释 | 字段描述发生变化 |
| `SchemaChange.UpdateComment` | 修改表注释 | 表注释发生变化 |

---

## 5. 类型兼容性检查

### 5.1 ConvertAction枚举

**位置**: `UpdatedDataFieldsProcessFunctionBase.java:438-454`

```java
public enum ConvertAction {
    /** oldType可以转换为newType */
    CONVERT,

    /** 类型相同族但旧类型精度更高，忽略此转换请求 */
    IGNORE,

    /** 不同类型族，抛出异常 */
    EXCEPTION
}
```

### 5.2 兼容性规则矩阵

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                        类型兼容性检查规则                                             │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                     │
│  基础类型兼容性:                                                                     │
│  ┌─────────────────────────────────────────────────────────────────────────────┐    │
│  │  字符串类型族: CHAR -> VARCHAR -> TEXT                                      │    │
│  │    规则: 长度可以增加，不能减少                                              │    │
│  │    例: VARCHAR(10) -> VARCHAR(20)  ✓  CONVERT                              │    │
│  │        VARCHAR(20) -> VARCHAR(10)  ✗  IGNORE                               │    │
│  └─────────────────────────────────────────────────────────────────────────────┘    │
│  ┌─────────────────────────────────────────────────────────────────────────────┐    │
│  │  二进制类型族: BINARY -> VARBINARY -> BLOB                                   │    │
│  │    规则: 长度可以增加，不能减少                                              │    │
│  └─────────────────────────────────────────────────────────────────────────────┘    │
│  ┌─────────────────────────────────────────────────────────────────────────────┐    │
│  │  整数类型族: TINYINT -> SMALLINT -> INT -> BIGINT                           │    │
│  │    规则: 范围可以扩大，不能缩小                                              │    │
│  │    例: INT -> BIGINT  ✓  CONVERT                                           │    │
│  │        BIGINT -> INT  ✗  IGNORE                                            │    │
│  └─────────────────────────────────────────────────────────────────────────────┘    │
│  ┌─────────────────────────────────────────────────────────────────────────────┐    │
│  │  浮点类型族: FLOAT -> DOUBLE                                               │    │
│  │    规则: 精度可以提升，不能降低                                              │    │
│  └─────────────────────────────────────────────────────────────────────────────┘    │
│  ┌─────────────────────────────────────────────────────────────────────────────┐    │
│  │  Decimal类型:                                                               │    │
│  │    规则:                                                                    │    │
│  │      - precision(总位数) 和 scale(小数位数) 只能增加                         │    │
│  │      - 如果配置了DECIMAL_NO_CHANGE模式，则忽略Decimal变更                    │    │
│  └─────────────────────────────────────────────────────────────────────────────┘    │
│  ┌─────────────────────────────────────────────────────────────────────────────┐    │
│  │  Timestamp类型:                                                             │    │
│  │    规则: 精度可以增加，不能降低                                              │    │
│  │    例: TIMESTAMP(3) -> TIMESTAMP(6)  ✓  CONVERT                             │    │
│  └─────────────────────────────────────────────────────────────────────────────┘    │
│  ┌─────────────────────────────────────────────────────────────────────────────┐    │
│  │  复杂类型 (ARRAY, MAP, MULTISET, ROW):                                      │    │
│  │    ARRAY: 检查元素类型兼容性                                                 │    │
│  │    MAP:   Key类型不能变化，Value类型检查兼容性                               │    │
│  │    ROW:   - 旧的非空字段必须在新Schema中存在                                │    │
│  │          - 公共字段类型必须兼容                                              │    │
│  │          - 新字段必须可空                                                    │    │
│  └─────────────────────────────────────────────────────────────────────────────┘    │
│                                                                                     │
│  特殊规则:                                                                         │
│  ┌─────────────────────────────────────────────────────────────────────────────┐    │
│  │  1. Nullable转换:                                                            │    │
│  │     NULL -> NOT NULL  ✗  EXCEPTION (不能把可空改为非空)                      │    │
│  │     NOT NULL -> NULL   ✓  CONVERT                                          │    │
│  └─────────────────────────────────────────────────────────────────────────────┘    │
│  ┌─────────────────────────────────────────────────────────────────────────────┐    │
│  │  2. 跨类型族转换:                                                             │    │
│  │     如果配置了ALLOW_NON_STRING_TO_STRING:                                    │    │
│  │       任何类型 -> STRING  ✓  CONVERT                                        │    │
│  └─────────────────────────────────────────────────────────────────────────────┘    │
│                                                                                     │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

### 5.3 canConvert核心代码

**位置**: `UpdatedDataFieldsProcessFunctionBase.java:173-264`

```java
public static ConvertAction canConvert(DataType oldType, DataType newType, TypeMapping typeMapping) {
    // 1. 完全相同（忽略nullable）
    if (oldType.equalsIgnoreNullable(newType)) {
        if (oldType.isNullable() && !newType.isNullable()) {
            return ConvertAction.EXCEPTION;  // 不能把可空改为非空
        }
        return ConvertAction.CONVERT;
    }

    // 2. ARRAY类型
    if (oldType.getTypeRoot() == DataTypeRoot.ARRAY && newType.getTypeRoot() == DataTypeRoot.ARRAY) {
        return canConvertArray((ArrayType) oldType, (ArrayType) newType, typeMapping);
    }

    // 3. MAP类型
    if (oldType.getTypeRoot() == DataTypeRoot.MAP && newType.getTypeRoot() == DataTypeRoot.MAP) {
        return canConvertMap((MapType) oldType, (MapType) newType, typeMapping);
    }

    // 4. MULTISET类型
    if (oldType.getTypeRoot() == DataTypeRoot.MULTISET && newType.getTypeRoot() == DataTypeRoot.MULTISET) {
        return canConvertMultisetType((MultisetType) oldType, (MultisetType) newType, typeMapping);
    }

    // 5. ROW类型
    if (oldType.getTypeRoot() == DataTypeRoot.ROW && newType.getTypeRoot() == DataTypeRoot.ROW) {
        return canConvertRowType((RowType) oldType, (RowType) newType, typeMapping);
    }

    // 6. 字符串类型族
    int oldIdx = STRING_TYPES.indexOf(oldType.getTypeRoot());
    int newIdx = STRING_TYPES.indexOf(newType.getTypeRoot());
    if (oldIdx >= 0 && newIdx >= 0) {
        return getLength(oldType) <= getLength(newType)
            ? ConvertAction.CONVERT : ConvertAction.IGNORE;
    }

    // 7. 二进制类型族
    oldIdx = BINARY_TYPES.indexOf(oldType.getTypeRoot());
    newIdx = BINARY_TYPES.indexOf(newType.getTypeRoot());
    if (oldIdx >= 0 && newIdx >= 0) {
        return getLength(oldType) <= getLength(newType)
            ? ConvertAction.CONVERT : ConvertAction.IGNORE;
    }

    // 8. 整数类型族
    oldIdx = INTEGER_TYPES.indexOf(oldType.getTypeRoot());
    newIdx = INTEGER_TYPES.indexOf(newType.getTypeRoot());
    if (oldIdx >= 0 && newIdx >= 0) {
        return oldIdx <= newIdx ? ConvertAction.CONVERT : ConvertAction.IGNORE;
    }

    // 9. 浮点类型族
    oldIdx = FLOATING_POINT_TYPES.indexOf(oldType.getTypeRoot());
    newIdx = FLOATING_POINT_TYPES.indexOf(newType.getTypeRoot());
    if (oldIdx >= 0 && newIdx >= 0) {
        return oldIdx <= newIdx ? ConvertAction.CONVERT : ConvertAction.IGNORE;
    }

    // 10. Decimal类型
    oldIdx = DECIMAL_TYPES.indexOf(oldType.getTypeRoot());
    newIdx = DECIMAL_TYPES.indexOf(newType.getTypeRoot());
    if (oldIdx >= 0 && newIdx >= 0) {
        return (getPrecision(newType) <= getPrecision(oldType)
                && getScale(newType) <= getScale(oldType))
            ? ConvertAction.IGNORE : ConvertAction.CONVERT;
    }

    // 11. Timestamp类型
    oldIdx = TIMESTAMP_TYPES.indexOf(oldType.getTypeRoot());
    newIdx = TIMESTAMP_TYPES.indexOf(newType.getTypeRoot());
    if (oldIdx >= 0 && newIdx >= 0) {
        return getPrecision(oldType) <= getPrecision(newType)
            ? ConvertAction.CONVERT : ConvertAction.IGNORE;
    }

    return ConvertAction.EXCEPTION;
}
```

---

## 6. Schema变更应用

### 6.1 applySchemaChange流程

**位置**: `UpdatedDataFieldsProcessFunctionBase.java:106-162`

```java
protected void applySchemaChange(
        SchemaManager schemaManager,
        SchemaChange schemaChange,
        Identifier identifier,
        CdcSchema newSchema) throws Exception {

    if (schemaChange instanceof SchemaChange.AddColumn) {
        // 添加列
        try {
            catalog.alterTable(identifier, schemaChange, false);
        } catch (Catalog.ColumnAlreadyExistException e) {
            // 忽略重复列异常（多表合并场景可能出现）
            LOG.debug("Failed to perform SchemaChange.AddColumn, possibly due to duplicated column name");
        }
    }
    else if (schemaChange instanceof SchemaChange.UpdateColumnType) {
        // 修改列类型
        SchemaChange.UpdateColumnType updateColumnType = (SchemaChange.UpdateColumnType) schemaChange;
        String fieldName = updateColumnType.fieldNames()[0];

        // 检查类型兼容性
        ConvertAction action = canConvert(oldFieldType, newFieldType, typeMapping);
        switch (action) {
            case CONVERT:
                catalog.alterTable(identifier, schemaChange, false);
                break;
            case EXCEPTION:
                throw new UnsupportedOperationException(
                    String.format("Cannot convert field %s from %s to %s",
                        fieldName, oldFieldType, newFieldType));
        }
    }
    else if (schemaChange instanceof SchemaChange.UpdateColumnComment) {
        // 修改列注释
        catalog.alterTable(identifier, schemaChange, false);
    }
    else if (schemaChange instanceof SchemaChange.UpdateComment) {
        // 修改表注释
        catalog.alterTable(identifier, schemaChange, false);
    }
    else {
        throw new UnsupportedOperationException("Unsupported schema change: " + schemaChange);
    }
}
```

### 6.2 新表创建流程

**位置**: `NewTableSchemaBuilder.java:64-88`

```java
public Optional<Schema> build(RichCdcMultiplexRecord record) {
    Schema sourceSchema = record.buildSchema();
    List<String> specifiedPartitionKeys = new ArrayList<>();

    // 支持表级别的分区配置
    List<String> partitionKeyMultipleList = partitionKeyMultiple.get(record.tableName());
    if (partitionKeyMultipleList != null && !partitionKeyMultipleList.isEmpty()) {
        specifiedPartitionKeys = partitionKeyMultipleList;
    } else if (partitionKeys != null && !partitionKeys.isEmpty()) {
        specifiedPartitionKeys = partitionKeys;
    }

    // 构建Paimon表Schema
    return Optional.of(buildPaimonSchema(
        record.tableName(),
        specifiedPartitionKeys,
        primaryKeys,
        Collections.emptyList(),  // computedColumns
        tableConfig,
        sourceSchema,
        metadataConverters,
        caseSensitive,
        false,  // isInPipeline
        requirePrimaryKeys,
        syncPKeysFromSourceSchema
    ));
}
```

---

## 7. 字段变更去重机制

### 7.1 去重流程

**位置**: `MultiTableUpdatedDataFieldsProcessFunction.java:88-110`

```java
@Override
public void processElement(
        Tuple2<Identifier, CdcSchema> updatedSchema,
        Context context,
        Collector<Void> collector) throws Exception {

    Identifier tableId = updatedSchema.f0;
    SchemaManager schemaManager = schemaManagers.computeIfAbsent(tableId, id -> {
        FileStoreTable table;
        try {
            table = (FileStoreTable) catalog.getTable(tableId);
        } catch (Catalog.TableNotExistException e) {
            return null;
        }
        return new SchemaManager(table.fileIO(), table.location());
    });

    // 获取该表的已处理字段缓存
    Set<FieldIdentifier> latestFields = latestFieldsMap.computeIfAbsent(tableId, id -> new HashSet<>());

    // 计算实际需要变更的字段（过滤已处理的）
    List<DataField> actualUpdatedDataFields = actualUpdatedDataFields(
        updatedSchema.f1.fields(),
        latestFields
    );

    if (actualUpdatedDataFields.isEmpty() && updatedSchema.f1.comment() == null) {
        return;  // 没有新变更，直接返回
    }

    // 构建实际变更的CdcSchema
    CdcSchema actualUpdatedSchema = new CdcSchema(
        actualUpdatedDataFields,
        updatedSchema.f1.primaryKeys(),
        updatedSchema.f1.comment()
    );

    // 应用变更
    for (SchemaChange schemaChange : extractSchemaChanges(schemaManager, actualUpdatedSchema)) {
        applySchemaChange(schemaManager, schemaChange, tableId, actualUpdatedSchema);
    }

    // 更新已处理字段缓存
    latestFieldsMap.put(tableId, updateLatestFields(schemaManager));
}
```

### 7.2 actualUpdatedDataFields实现

**位置**: `UpdatedDataFieldsProcessFunctionBase.java:412-417`

```java
protected List<DataField> actualUpdatedDataFields(
        List<DataField> newFields, Set<FieldIdentifier> latestFields) {
    return newFields.stream()
        .filter(dataField -> !latestFields.contains(new FieldIdentifier(dataField)))
        .collect(Collectors.toList());
}
```

### 7.3 updateLatestFields实现

**位置**: `UpdatedDataFieldsProcessFunctionBase.java:419-424`

```java
protected Set<FieldIdentifier> updateLatestFields(SchemaManager schemaManager) {
    RowType oldRowType = schemaManager.latest().get().logicalRowType();
    return oldRowType.getFields().stream()
        .map(FieldIdentifier::new)
        .collect(Collectors.toSet());
}
```

**去重机制说明**:
1. 使用`latestFieldsMap`为每个表维护已处理的字段集合
2. 在处理变更前，过滤掉已在`latestFields`中的字段
3. 变更应用后，更新`latestFields`为当前表的所有字段
4. 避免同一字段被重复变更（例如多条CDC消息包含相同的Schema变更）

---

## 8. 嵌套类型Schema演变

### 8.1 嵌套类型变更处理

Paimon支持ROW、ARRAY、MAP等嵌套类型的Schema演变。当嵌套类型发生变更时，`NestedSchemaUtils.generateNestedColumnUpdates`会递归生成变更路径。

```java
// 示例: ROW类型字段变更
// 旧类型: ROW<id INT, name STRING, age INT>
// 新类型: ROW<id INT, name STRING, age INT, address STRING>

// 生成的SchemaChange:
SchemaChange.addColumn(new String[] {"user", "address"}, DataTypes.STRING(), null, null)
```

### 8.2 ROW类型演变规则

```java
private static ConvertAction canConvertRowType(
        RowType oldRowType, RowType newRowType, TypeMapping typeMapping) {

    Map<String, DataField> oldFieldMap = new HashMap<>();
    for (DataField field : oldRowType.getFields()) {
        oldFieldMap.put(field.name(), field);
    }

    Map<String, DataField> newFieldMap = new HashMap<>();
    for (DataField field : newRowType.getFields()) {
        newFieldMap.put(field.name(), field);
    }

    // 规则1: 旧的非空字段必须在新Schema中存在
    for (DataField oldField : oldRowType.getFields()) {
        if (!oldField.type().isNullable()) {
            if (!newFieldMap.containsKey(oldField.name())) {
                return ConvertAction.EXCEPTION;
            }
        }
    }

    // 规则2: 公共字段类型必须兼容
    boolean needsConversion = false;
    for (DataField newField : newRowType.getFields()) {
        DataField oldField = oldFieldMap.get(newField.name());
        if (oldField != null) {
            ConvertAction fieldAction = canConvert(oldField.type(), newField.type(), typeMapping);
            if (fieldAction == ConvertAction.EXCEPTION) {
                return ConvertAction.EXCEPTION;
            }
            if (fieldAction == ConvertAction.CONVERT) {
                needsConversion = true;
            }
        } else {
            // 规则3: 新字段必须可空
            if (!newField.type().isNullable()) {
                return ConvertAction.EXCEPTION;
            }
            needsConversion = true;
        }
    }

    return needsConversion ? ConvertAction.CONVERT : ConvertAction.IGNORE;
}
```

---

## 9. 并发控制

### 9.1 并行度限制

Schema演变操作必须保证串行执行，因此相关ProcessFunction的并行度强制设置为1：

```java
// FlinkCdcSyncDatabaseSinkBuilder.java:181-183
SingleOutputStreamOperator<?> schemaChangeProcessFunction = ...
    .process(new MultiTableUpdatedDataFieldsProcessFunction(catalogLoader, typeMapping))
    .name("Schema Evolution");

schemaChangeProcessFunction.getTransformation().setParallelism(1);
schemaChangeProcessFunction.getTransformation().setMaxParallelism(1);
```

### 9.2 为什么需要串行执行

1. **避免并发修改冲突**: 多个线程同时修改同一个表Schema可能导致元数据损坏
2. **保证变更顺序**: CDC消息可能乱序，串行执行保证Schema按正确顺序变更
3. **避免死锁**: 多个表的Schema变更可能形成循环依赖

---

## 10. 完整处理流程示例

### 10.1 MySQL添加列场景

```sql
-- MySQL执行DDL
ALTER TABLE users ADD COLUMN phone VARCHAR(20);
```

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                          MySQL添加列处理流程                                         │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                     │
│  1. MySQL执行DDL                                                                    │
│     ALTER TABLE users ADD COLUMN phone VARCHAR(20);                                │
│                                                                                     │
│  2. Debezium捕获变更                                                                │
│     ┌─────────────────────────────────────────────────────────────────────────┐    │
│     │ {                                                                       │    │
│     │   "payload": {                                                          │    │
│     │     "after": {                                                          │    │
│     │       "id": 1,                                                          │    │
│     │       "name": "Alice",                                                  │    │
│     │       "phone": "1234567890"  // 新字段                                  │    │
│     │     },                                                                  │    │
│     │     "source": {                                                          │    │
│     │       "db": "mydb",                                                     │    │
│     │       "table": "users"                                                  │    │
│     │     }                                                                    │    │
│     │   }                                                                      │    │
│     │ }                                                                       │    │
│     └─────────────────────────────────────────────────────────────────────────┘    │
│                                      │                                             │
│                                      ▼                                             │
│  3. DebeziumJsonRecordParser解析                                                   │
│     - 解析schema字段，发现新字段phone                                             │
│     - 生成RichCdcMultiplexRecord包含新Schema                                      │
│                                                                                     │
│  4. RichEventParser检测变化                                                         │
│     - previousDataFields中不存在phone字段                                           │
│     - 生成CdcSchema包含phone字段                                                   │
│                                                                                     │
│  5. CdcDynamicTableParsingProcessFunction                                           │
│     - 输出到DYNAMIC_SCHEMA_CHANGE_OUTPUT_TAG                                       │
│     - Tuple2<Identifier("mydb", "users"), CdcSchema>                               │
│                                                                                     │
│  6. MultiTableUpdatedDataFieldsProcessFunction                                     │
│     a. actualUpdatedDataFields()                                                   │
│        - latestFields不包含phone，需要变更                                         │
│     b. extractSchemaChanges()                                                      │
│        - 生成SchemaChange.addColumn("phone", VARCHAR)                             │
│     c. canConvert(VARCHAR不存在, 新VARCHAR)                                        │
│        - 返回CONVERT                                                               │
│     d. applySchemaChange()                                                         │
│        - catalog.alterTable(identifier, addColumn, false)                         │
│                                                                                     │
│  7. Paimon Catalog更新Schema                                                       │
│     - SchemaManager.commitChanges()                                               │
│     - 写入新的Schema文件到文件系统                                                  │
│                                                                                     │
│  8. CdcRecordStoreMultiWriteOperator刷新Table                                      │
│     - replace(latestTable)加载新Schema                                             │
│     - 后续数据使用新Schema处理                                                      │
│                                                                                     │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

### 10.2 类型升级场景

```sql
-- MySQL执行DDL
ALTER TABLE users MODIFY COLUMN name VARCHAR(100);  -- 原来是VARCHAR(50)
```

```
处理流程:

1. Debezium捕获变更，新Schema中name字段为VARCHAR(100)

2. extractSchemaChanges()
   - 识别到name字段存在但类型不同
   - 生成SchemaChange.updateColumnType("name", VARCHAR(100))

3. canConvert(VARCHAR(50), VARCHAR(100))
   - 检查: 50 <= 100
   - 返回CONVERT

4. applySchemaChange()
   - catalog.alterTable(..., updateColumnType, false)
   - Paimon Schema更新为VARCHAR(100)

5. 后续数据使用新Schema处理
```

---

## 11. 限制和注意事项

### 11.1 不支持的Schema变更

| 变更类型 | 状态 | 说明 |
|----------|------|------|
| 删除列 | 部分支持 | 可以物理删除，但可能导致查询失败 |
| 修改主键 | 不支持 | 需要手动重建表 |
| 修改分区键 | 不支持 | 需要手动重建表 |
| 缩小类型精度 | 不支持 | 返回IGNORE，不执行变更 |
| 非空转可空 | 不支持 | 返回EXCEPTION |
| MAP Key类型变更 | 不支持 | 返回EXCEPTION |

### 11.2 配置选项

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `type_mapping` | `default` | 类型映射模式 |
| `DECIMAL_NO_CHANGE` | `false` | 是否禁止Decimal类型变更 |
| `ALLOW_NON_STRING_TO_STRING` | `false` | 是否允许任意类型转STRING |

### 11.3 最佳实践

1. **源端控制变更**: 尽量在源端控制DDL变更的类型兼容性
2. **测试验证**: Schema变更前在测试环境验证
3. **监控告警**: 监控Schema变更失败事件
4. **回滚预案**: 准备好回滚方案
5. **渐进式变更**: 避免大量Schema变更同时发生

---

## 12. 总结

Paimon CDC的Schema演变机制通过以下核心组件实现：

1. **RichEventParser**: 检测Schema变化
2. **MultiTableUpdatedDataFieldsProcessFunction**: 处理Schema变更
3. **类型兼容性检查**: canConvert方法确保变更安全
4. **去重机制**: latestFieldsMap避免重复变更
5. **并发控制**: 并行度=1保证串行执行

**完整数据流**:
```
MySQL DDL -> Debezium -> Kafka -> DebeziumJsonRecordParser
    -> RichEventParser -> CdcDynamicTableParsingProcessFunction
    -> MultiTableUpdatedDataFieldsProcessFunction
    -> Paimon Catalog -> SchemaManager -> 文件系统
```
