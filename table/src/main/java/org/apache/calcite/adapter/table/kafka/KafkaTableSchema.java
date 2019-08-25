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
import io.kcache.KeyValue;
import io.kcache.KeyValueIterator;
import org.apache.avro.Schema;
import org.apache.calcite.adapter.table.AbstractTableSchema;
import org.apache.calcite.adapter.table.SortedTableColumnType;
import org.apache.calcite.adapter.table.SortedTableSchema;
import org.apache.calcite.adapter.table.avro.AvroTableSchema;
import org.apache.calcite.model.ModelHandler;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.Table;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Source;
import org.apache.calcite.util.Sources;
import org.apache.kafka.common.serialization.Serdes;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Schema mapped onto a directory of CSV files. Each table in the schema
 * is a CSV file in that directory.
 */
public class KafkaTableSchema extends AbstractTableSchema {
  private final Map<String, Table> tableMap;
  private String bootstrapServers;
  private Cache<String, String> schemas;

  /**
   * Creates a CSV schema.
   */
  public KafkaTableSchema() {
    this.tableMap = new HashMap<>();
  }

  @Override
  protected Map<String, Table> delegate() {
    return tableMap;
  }

  @Override
  public void configure(Map<String, ?> operand) {
    final String bootstrapServers = (String) operand.get("bootstrapServers");
    this.bootstrapServers = bootstrapServers;
    this.schemas = new KafkaCache<String, String>(bootstrapServers, Serdes.String(), Serdes.String());
    this.schemas.init();
    init(operand);
  }

  private void init(Map<String, ?> operand) {
    Map<String, Object> configs = new HashMap<>(operand);
    for (KeyValueIterator<String, String> iter = schemas.all(); iter.hasNext(); ) {
      KeyValue<String, String> kv = iter.next();
      Schema.Parser parser = new Schema.Parser();
      Schema avroSchema = parser.parse(kv.value);
      configs.put("schema", avroSchema);
      // TODO use primary key annotation
      final Table table = SortedTableSchema.createTable(configs, AvroTableSchema.getRowType(avroSchema));
      tableMap.put(avroSchema.getName(), table);
    }
  }
}
