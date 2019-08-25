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
import org.apache.avro.Schema.Field;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.calcite.adapter.table.AbstractTable;
import org.apache.calcite.adapter.table.SortedTable;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.model.ModelHandler;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Source;
import org.apache.calcite.util.Sources;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Map;

public class KafkaTableSerializer implements Serializer<Comparable[]> {
  private final EncoderFactory encoderFactory = EncoderFactory.get();
  private Schema schema;

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    schema = (Schema) configs.get("schema");
  }

  @Override
  public byte[] serialize(String topic, Comparable[] object) {
    if (object == null) {
      return null;
    }
    try {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      BinaryEncoder encoder = encoderFactory.directBinaryEncoder(out, null);
      DatumWriter<Object> writer = new GenericDatumWriter<>(schema);
      writer.write(toRecord(object), encoder);
      encoder.flush();
      byte[] bytes = out.toByteArray();
      out.close();
      return bytes;
    } catch (IOException | RuntimeException e) {
      // avro serialization can throw AvroRuntimeException, NullPointerException,
      // ClassCastException, etc
      throw new SerializationException("Error serializing Avro message", e);
    }
  }

  private GenericRecord toRecord(Comparable[] object) {
    GenericRecordBuilder builder = new GenericRecordBuilder(schema);
    for (Ord<Field> field : Ord.zip(schema.getFields())) {
      builder.set(field.e, object[field.i]);
    }
    return builder.build();
  }

  @Override
  public void close() {
  }
}

// End CsvSortedTable.java
