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
package org.apache.calcite.adapter.table.avro;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.calcite.adapter.table.AbstractTable;
import org.apache.calcite.adapter.table.SortedTable;
import org.apache.calcite.model.ModelHandler;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.Source;
import org.apache.calcite.util.Sources;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;

/**
 * Base class for table that reads CSV files.
 */
public class AvroTable extends AbstractTable {
  private final SortedTable sortedTable;
  private final Map<Comparable[], Comparable[]> rows;

  private final DecoderFactory decoderFactory = DecoderFactory.get();

  /** Creates a CsvTable. */
  public AvroTable(SortedTable sortedTable) {
    this.sortedTable = sortedTable;
    this.rows = new TreeMap<>(new SortedTable.MapComparator());
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
      throw new IllegalStateException("Custom tables not yet supported for Avro");
    }
    try {
      Schema schema = (Schema) operand.get("schema");
      if (schema == null) {
        return;
      }
      Collection modifiableCollection = sortedTable.getModifiableCollection();
      DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
      Source json = getSource(operand, schema.getName() + ".json");
      if (json != null) {
        BufferedReader reader = Files.newBufferedReader(Paths.get(json.path()));
        String line;
        while ((line = reader.readLine()) != null) {
          Object row = convertRow(datumReader.read(null, decoderFactory.jsonDecoder(schema, line)));
          //noinspection unchecked
          modifiableCollection.add(row);
        }
      }
      Source avro = getSource(operand, schema.getName() + ".avro");
      if (avro != null) {
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(avro.file(), datumReader);
        for (GenericRecord record : dataFileReader) {
          Object row = convertRow(record);
          //noinspection unchecked
          modifiableCollection.add(row);
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private Source getSource(Map<String, ?> operand, String fileName) {
    if (fileName == null) {
      return null;
    }
    Path path = Paths.get(fileName);
    final String directory = (String) operand.get("directory");
    if (directory != null) {
      path = Paths.get(directory, path.toString());
    }
    final File base = (File) operand.get(ModelHandler.ExtraOperand.BASE_DIRECTORY.camelName);
    if (base != null) {
      path = Paths.get(base.getPath(), path.toString());
    }
    File file = path.toFile();
    return file.exists() ? Sources.of(path.toFile()) : null;
  }

  private Object convertRow(GenericRecord record) {
    int size = record.getSchema().getFields().size();
    Object[] result = new Object[size];
    for (int i = 0; i < size; i++) {
      Object field = record.get(i);
      result[i] = field;
    }
    return size == 1 ? result[0] : result;
  }

  public static void main(String[] args) throws Exception {
    Schema schema = new Schema.Parser().parse(new File("/tmp/user.avsc"));
    GenericRecord user1 = new GenericData.Record(schema);
    user1.put("name", "Alyssa");
    user1.put("age", 25);
    GenericRecord user2 = new GenericData.Record(schema);
    user2.put("name", "Ben");
    user2.put("age", 7);
    GenericRecord user3 = new GenericData.Record(schema);
    user3.put("name", "Chris");
    user3.put("age", 35);
    GenericRecord user4 = new GenericData.Record(schema);
    user4.put("name", "Dan");
    user4.put("age", 73);
    File file = new File("/tmp/users.avro");
    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
    DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
    dataFileWriter.create(schema, file);
    dataFileWriter.append(user1);
    dataFileWriter.append(user2);
    dataFileWriter.append(user3);
    dataFileWriter.append(user4);
    dataFileWriter.close();

    /*
    Schema emptyRecordSchema = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"EmptyRecord\",\"fields\":[]}");
    GenericRecord emptyRecord = (GenericRecord) GenericData.get().newRecord(null, emptyRecordSchema);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(out, null);
    DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(emptyRecordSchema);
    writer.write(emptyRecord, encoder);
    encoder.flush();
    out.close();
    byte[] bytes = out.toByteArray();
    DatumReader<GenericRecord> reader = new GenericDatumReader<>(emptyRecordSchema);
    Object o = reader.read(null, DecoderFactory.get().binaryDecoder(bytes, null));
    System.out.println("*** o " + o);
    */
  }
}

// End CsvSortedTable.java
