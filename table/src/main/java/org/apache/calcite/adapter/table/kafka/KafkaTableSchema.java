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
import org.apache.calcite.adapter.table.AbstractTableSchema;
import org.apache.calcite.schema.Table;
import org.apache.kafka.common.serialization.Serdes;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

/**
 * Schema mapped onto a directory of CSV files. Each table in the schema
 * is a CSV file in that directory.
 */
public class KafkaTableSchema extends AbstractTableSchema {
  private Cache<String, Table> tableMap;

  /**
   * Creates a CSV schema.
   */
  public KafkaTableSchema() {
  }

  @Override
  protected Map<String, Table> delegate() {
    return tableMap;
  }

  @Override
  public void configure(Map<String, ?> operand) {
    final String bootstrapServers = (String) operand.get("bootstrapServers");
    Properties props = new Properties();
    props.put(KafkaCacheConfig.KAFKACACHE_BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    // TODO fix dummy
    props.put(KafkaCacheConfig.KAFKACACHE_TOPIC_CONFIG, "_meta_dummy_123");
    KafkaTableSerde tableSerde = new KafkaTableSerde();
    tableSerde.configure(Collections.singletonMap("bootstrapServers", bootstrapServers), false);
    this.tableMap = new KafkaCache<>(new KafkaCacheConfig(props), Serdes.String(), tableSerde);
    this.tableMap.init();
  }
}
