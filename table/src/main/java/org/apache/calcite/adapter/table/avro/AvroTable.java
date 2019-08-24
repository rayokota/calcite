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
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.calcite.adapter.table.AbstractTable;
import org.apache.calcite.adapter.table.SortedTable;
import org.apache.calcite.adapter.table.SortedTableColumnType;
import org.apache.calcite.model.ModelHandler;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.Source;
import org.apache.calcite.util.Sources;
import org.apache.commons.lang3.time.FastDateFormat;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.TreeMap;

/**
 * Base class for table that reads CSV files.
 *
 * @param <E> Row type
 */
public class AvroTable<E> extends AbstractTable<E> {
  private Map<E, E> rows;
  private RelDataType rowType;

  private final DecoderFactory decoderFactory = DecoderFactory.get();

  /** Creates a CsvTable. */
  public AvroTable(RelDataType rowType) {
    this.rows = new TreeMap<E, E>(new SortedTable.MapComparator());
    this.rowType = rowType;
  }

  @Override
  public RelDataType getRowType() {
    return rowType;
  }

  @Override
  protected Map<E, E> delegate() {
    return rows;
  }

  @Override
  public void configure(Map<String, ?> operand) {
    if (rowType == null) {
      // TODO support custom tables
      throw new IllegalStateException("Custom tables not yet supported for Avro");
    }
    try {
      Schema schema = (Schema) operand.get("schema");
      DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
      Source json = getSource(operand, schema.getName() + ".json");
      if (json != null) {
        BufferedReader reader = Files.newBufferedReader(Paths.get(json.path()));
        String line;
        while ((line = reader.readLine()) != null) {
          addRow(datumReader.read(null, decoderFactory.jsonDecoder(schema, line)));
        }
      }
      Source avro = getSource(operand, schema.getName() + ".avro");
      if (avro != null) {
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(avro.file(), datumReader);
        for (GenericRecord record : dataFileReader) {
          addRow(record);
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

  @SuppressWarnings("unchecked")
  private void addRow(GenericRecord record) {
    E row = convertRow(record);
    E keyArray = (E) SortedTable.getKey(row);
    E valueArray = (E) SortedTable.getValue(row);
    rows.put(keyArray, valueArray);
  }

  @SuppressWarnings("unchecked")
  private E convertRow(GenericRecord record) {
    int size = record.getSchema().getFields().size();
    Object[] result = new Object[size];
    for (int i = 0; i < size; i++) {
      Object field = record.get(i);
      result[i] = field;
    }
    return size == 1 ? (E) result[0] : (E) result;
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
    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
    DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
    dataFileWriter.create(schema, file);
    dataFileWriter.append(user1);
    dataFileWriter.append(user2);
    dataFileWriter.append(user3);
    dataFileWriter.append(user4);
    dataFileWriter.close();
  }
}

// End CsvSortedTable.java
