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

import org.apache.avro.Schema;
import org.apache.calcite.adapter.table.SortedTable;
import org.apache.calcite.adapter.table.SortedTableSchema;
import org.apache.calcite.adapter.table.avro.AvroTableSchema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.Table;
import org.apache.calcite.util.Pair;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KafkaTableDeserializer implements Deserializer<Table> {
  private String bootstrapServers;

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    bootstrapServers = (String) configs.get("bootstrapServers");
  }

  @Override
  public Table deserialize(String topic, byte[] payload) throws SerializationException {
    if (payload == null) {
      return null;
    }
    try {
      Schema.Parser parser1 = new Schema.Parser();
      Schema avroSchema = parser1.parse(new String(payload, StandardCharsets.UTF_8));
      Pair<RelDataType, List<String>> rowType = AvroTableSchema.getRowType(avroSchema);
      Map<String, Object> configs = new HashMap<>();
      configs.put("bootstrapServers", bootstrapServers);
      configs.put("schema", avroSchema);
      configs.put("kind", SortedTable.Kind.KAFKA.name());
      return SortedTableSchema.createTable(avroSchema.getName(), configs, rowType.left, rowType.right);
    } catch (RuntimeException e) {
      // avro deserialization may throw AvroRuntimeException, NullPointerException, etc
      throw new SerializationException("Error deserializing table", e);
    }
  }

  @Override
  public void close() {
  }
}

// End CsvSortedTable.java
