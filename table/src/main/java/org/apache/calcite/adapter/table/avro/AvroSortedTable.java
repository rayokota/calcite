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

import au.com.bytecode.opencsv.CSVReader;
import com.google.common.collect.ForwardingMap;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.reflect.AvroSchema;
import org.apache.calcite.adapter.table.SortedTable;
import org.apache.calcite.adapter.table.SortedTableColumnType;
import org.apache.calcite.adapter.table.csv.CsvSortedTableSchema;
import org.apache.calcite.avatica.util.Base64;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.model.ModelHandler;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Source;
import org.apache.calcite.util.Sources;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.utils.Bytes;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Base class for table that reads CSV files.
 *
 * @param <E> Row type
 */
public class AvroSortedTable<E> extends ForwardingMap<E, E> implements Configurable {
  private List<String> names;
  private List<SortedTableColumnType> fieldTypes;
  private Map<E, E> rows;
  private RelDataType rowType;

  private final DecoderFactory decoderFactory = DecoderFactory.get();

  private static final FastDateFormat TIME_FORMAT_DATE;
  private static final FastDateFormat TIME_FORMAT_TIME;
  private static final FastDateFormat TIME_FORMAT_TIMESTAMP;

  static {
    final TimeZone gmt = TimeZone.getTimeZone("GMT");
    TIME_FORMAT_DATE = FastDateFormat.getInstance("yyyy-MM-dd", gmt);
    TIME_FORMAT_TIME = FastDateFormat.getInstance("HH:mm:ss", gmt);
    TIME_FORMAT_TIMESTAMP =
            FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss", gmt);
  }

  /** Creates a CsvTable. */
  public AvroSortedTable(RelDataType rowType) {
    this.rows = new TreeMap<E, E>(new SortedTable.MapComparator());
    this.rowType = rowType;
  }

  @Override
  protected Map<E, E> delegate() {
    return rows;
  }

  public RelDataType getRowType() {
    return rowType;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void configure(Map<String, ?> operand) {
    Schema schema = (Schema) operand.get("schema");
    if (schema != null) {
      // TODO
      String fileName = schema.getName() + ".json";
      Path file = Paths.get(fileName);
      final String directory = (String) operand.get("directory");
      if (directory != null) {
        file = Paths.get(directory, file.toString());
      }
      final File base = (File) operand.get(ModelHandler.ExtraOperand.BASE_DIRECTORY.camelName);
      if (base != null) {
        file = Paths.get(base.getPath(), file.toString());
      }
      final Source source = Sources.of(file.toFile());
      if (rowType == null) {
        // TODO support custom tables
        throw new IllegalStateException("Custom tables not yet supported for Avro");
      }
      try {
        BufferedReader reader = Files.newBufferedReader(Paths.get(source.path()));
        String line;
        while ((line = reader.readLine()) != null) {
          DatumReader<Record> avroReader = new GenericDatumReader<>(schema);
          Record record = avroReader.read(null, decoderFactory.jsonDecoder(schema, line));
          E row = convertRow(record);
          E keyArray = (E) SortedTable.getKey(row);
          E valueArray = (E) SortedTable.getValue(row);
          rows.put(keyArray, valueArray);
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private E convertRow(Record record) {
    int size = record.getSchema().getFields().size();
    Object[] result = new Object[size];
    for (int i = 0; i < size; i++) {
      Object field = record.get(i);
      result[i] = field;
    }
    return size == 1 ? (E) result[0] : (E) result;
  }
}

// End CsvSortedTable.java
