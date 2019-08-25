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

import com.google.common.collect.Maps;
import io.kcache.Cache;
import io.kcache.KafkaCache;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.calcite.adapter.table.AbstractTable;
import org.apache.calcite.adapter.table.SortedTable;
import org.apache.calcite.model.ModelHandler;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Source;
import org.apache.calcite.util.Sources;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Base class for table that reads CSV files.
 */
public class KafkaTable extends AbstractTable {
  private SortedTable sortedTable;
  private Map<Comparable[], Comparable[]> rows;
  private String bootstrapServers;
  private Schema keySchema;
  private Schema valueSchema;

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
    final String bootstrapServers = (String) operand.get("bootstrapServers");
    this.bootstrapServers = bootstrapServers;
    final Schema schema = (Schema) operand.get("schema");
    Pair<Schema, Schema> schemas = getKeyValueSchemas(schema);
    KafkaTableSerde keySerde = new KafkaTableSerde();
    KafkaTableSerde valueSerde = new KafkaTableSerde();
    keySerde.configure(Collections.singletonMap("schema", schemas.left), true);
    valueSerde.configure(Collections.singletonMap("schema", schemas.right), false);
    Cache<Comparable[], Comparable[]> cache = new KafkaCache<>(bootstrapServers, keySerde, valueSerde);
    // TODO call close
    cache.init();
    this.rows = cache;
  }

  private Pair<Schema, Schema> getKeyValueSchemas(Schema schema) {
    return null;
  }
}

// End CsvSortedTable.java
