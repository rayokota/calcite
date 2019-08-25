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
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

public class KafkaTableDeserializer implements Deserializer<Comparable[]> {
  private final DecoderFactory decoderFactory = DecoderFactory.get();
  private Schema schema;

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    schema = (Schema) configs.get("schema");
  }

  @Override
  public Comparable[] deserialize(String topic, byte[] payload) throws SerializationException {
    if (payload == null) {
      return null;
    }
    try {
      DatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
      GenericRecord record = reader.read(null, decoderFactory.binaryDecoder(payload, null));
      return toRow(record);
    } catch (IOException | RuntimeException e) {
      // avro deserialization may throw AvroRuntimeException, NullPointerException, etc
      throw new SerializationException("Error deserializing Avro message", e);
    }
  }

  private Comparable[] toRow(GenericRecord record) {
    int size = schema.getFields().size();
    Comparable[] row = new Comparable[size];
    for (int i = 0; i < size; i++) {
      row[i] = (Comparable) record.get(i);
    }
    return row;
  }

  @Override
  public void close() {
  }
}

// End CsvSortedTable.java
