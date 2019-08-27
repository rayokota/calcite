/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.table.kafka;

import io.kcache.Cache;
import io.kcache.KafkaCache;
import io.kcache.KafkaCacheConfig;
import io.kcache.utils.InMemoryCache;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.SchemaBuilder;
import org.apache.calcite.adapter.table.AbstractTable;
import org.apache.calcite.adapter.table.SortedTable;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Base class for table that reads CSV files.
 */
public class KafkaTable extends AbstractTable {
  private SortedTable sortedTable;
  private Schema schema;
  private String bootstrapServers;
  private Map<Comparable[], Comparable[]> rows;

  /** Creates a CsvTable. */
  public KafkaTable(SortedTable sortedTable) {
    this.sortedTable = sortedTable;
  }

  @Override
  public RelDataType getRowType() {
    return sortedTable.getRowType();
  }

  @Override
  protected Map<Comparable[], Comparable[]> delegate() {
    return rows;
  }

  @Override
  public void configure(Map<String, ?> operand) {
    if (sortedTable.getRowType() == null) {
      // TODO support custom tables
      throw new IllegalStateException("Custom tables not yet supported for Kafka");
    }
    Schema schema = (Schema) operand.get("schema");
    if (schema == null) {
      schema = toSchema(sortedTable.getRowType(), sortedTable.getKeyFields());
    }
    final String bootstrapServers = (String) operand.get("bootstrapServers");
    this.bootstrapServers = bootstrapServers;
    Pair<Schema, Schema> schemas = getKeyValueSchemas(schema);
    KafkaTableRowSerde keySerde = new KafkaTableRowSerde();
    KafkaTableRowSerde valueSerde = new KafkaTableRowSerde();
    keySerde.configure(Collections.singletonMap("schema", schemas.left), true);
    valueSerde.configure(Collections.singletonMap("schema", schemas.right), false);
    Properties props = new Properties();
    props.put(KafkaCacheConfig.KAFKACACHE_BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(KafkaCacheConfig.KAFKACACHE_TOPIC_CONFIG, sortedTable.getName());
    Cache<Comparable[], Comparable[]> cache = new KafkaCache<>(
            new KafkaCacheConfig(props), keySerde, valueSerde, null,
            new InMemoryCache<>(new SortedTable.MapComparator()));
    // TODO call close
    cache.init();
    this.schema = schema;
    this.rows = cache;
  }

  public Schema getSchema() {
    return schema;
  }

  private Schema toSchema(RelDataType rowType, List<String> keyFields) {
    // TODO fix unknown
    SchemaBuilder.FieldAssembler<Schema> schemaBuilder =
            SchemaBuilder.record("unknown").fields();
    Map<String, Integer> keyIndices = Ord.zip(keyFields).stream().collect(Collectors.toMap(o -> o.e, o -> o.i));
    for (RelDataTypeField field : rowType.getFieldList()) {
      SqlTypeName type = field.getType().getSqlTypeName();
      SchemaBuilder.FieldBuilder<Schema> fieldBuilder = schemaBuilder.name(field.getName());
      Integer keyIndex = keyIndices.get(field.getName());
      if (keyIndex != null) {
        fieldBuilder = fieldBuilder.prop("sql.key.index", keyIndex);
      }
      // TODO add more types, fix optional, default value
      switch (type) {
        case VARCHAR:
          // TODO cleanup
          //schemaBuilder = fieldBuilder.type().optional().stringType();
          schemaBuilder = fieldBuilder.type().stringType().noDefault();
          break;
        case BIGINT:
          //schemaBuilder = fieldBuilder.type().optional().longType();
          schemaBuilder = fieldBuilder.type().longType().noDefault();
          break;
        case INTEGER:
          //schemaBuilder = fieldBuilder.type().optional().intType();
          schemaBuilder = fieldBuilder.type().intType().noDefault();
          break;
        default:
          throw new IllegalArgumentException("Unsupported type " + type);
      }
    }
    Schema schema = schemaBuilder.endRecord();
    return schema;
  }

  private Pair<Schema, Schema> getKeyValueSchemas(Schema schema) {
    SchemaBuilder.FieldAssembler<Schema> keySchemaBuilder =
            SchemaBuilder.record(schema.getName() + "_key").fields();
    SchemaBuilder.FieldAssembler<Schema> valueSchemaBuilder =
            SchemaBuilder.record(schema.getName() + "_value").fields();
    int size = schema.getFields().size();
    Field[] keyFields = new Field[size];
    Field[] valueFields = new Field[size];
    int valueIndex = 0;
    for (Field field : schema.getFields()) {
      Integer keyIndex = (Integer) field.getObjectProp("sql.key.index");
      if (keyIndex != null) {
        keyFields[keyIndex] = field;
      } else {
        valueFields[valueIndex++] = field;
      }
    }
    int keyCount = 0;
    for (int i = 0; i < keyFields.length; i++) {
      Field field = keyFields[i];
      if (field == null) {
        break;
      }
      // TODO fix default
      keySchemaBuilder = keySchemaBuilder.name(field.name()).type(field.schema()).noDefault();
      keyCount++;
    }
    valueIndex = 0;
    if (keyCount == 0) {
      // Use first value field as key
      Field field = valueFields[valueIndex++];
      keySchemaBuilder = keySchemaBuilder.name(field.name()).type(field.schema()).noDefault();
    }
    for (; valueIndex < valueFields.length; valueIndex++) {
      Field field = valueFields[valueIndex];
      if (field == null) {
        break;
      }
      valueSchemaBuilder = valueSchemaBuilder.name(field.name()).type(field.schema()).noDefault();
    }
    Schema keySchema = keySchemaBuilder.endRecord();
    Schema valueSchema = valueSchemaBuilder.endRecord();
    return Pair.of(keySchema, valueSchema);
  }
}

// End CsvSortedTable.java
