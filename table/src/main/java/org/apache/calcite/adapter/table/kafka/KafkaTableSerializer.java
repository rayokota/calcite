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
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.calcite.adapter.table.SortedTable;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.schema.Table;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class KafkaTableSerializer implements Serializer<Table> {

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
  }

  @Override
  public byte[] serialize(String topic, Table table) {
    if (table == null) {
      return null;
    }
    try {
      Schema schema = ((KafkaTable) ((SortedTable) table).getRawTable()).getSchema();
      return schema.toString().getBytes(StandardCharsets.UTF_8);
    } catch (RuntimeException e) {
      // avro serialization can throw AvroRuntimeException, NullPointerException,
      // ClassCastException, etc
      throw new SerializationException("Error serializing table", e);
    }
  }

  @Override
  public void close() {
  }
}

// End CsvSortedTable.java
