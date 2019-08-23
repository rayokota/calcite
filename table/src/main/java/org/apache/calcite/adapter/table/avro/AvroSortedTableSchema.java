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
import avro.shaded.com.google.common.collect.ForwardingMap;
import org.apache.avro.Schema;
import org.apache.calcite.adapter.table.SortedTableColumnType;
import org.apache.calcite.adapter.table.SortedTableSchema;
import org.apache.calcite.model.ModelHandler;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.Table;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Source;
import org.apache.calcite.util.Sources;
import org.apache.kafka.common.Configurable;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Schema mapped onto a directory of CSV files. Each table in the schema
 * is a CSV file in that directory.
 */
public class AvroSortedTableSchema extends ForwardingMap<String, Table> implements Configurable {
  private final SortedTableSchema schema;
  private final Map<String, Table> tableMap;
  private File directoryFile;

  /**
   * Creates a CSV schema.
   */
  public AvroSortedTableSchema(SortedTableSchema schema) {
    this.schema = schema;
    this.tableMap = new HashMap<>();
  }

  @Override
  protected Map<String, Table> delegate() {
    return tableMap;
  }

  @Override
  public void configure(Map<String, ?> operand) {
    final String directory = (String) operand.get("directory");
    final File base =
            (File) operand.get(ModelHandler.ExtraOperand.BASE_DIRECTORY.camelName);
    File directoryFile = new File(directory);
    if (base != null && !directoryFile.isAbsolute()) {
      directoryFile = new File(base, directory);
    }
    this.directoryFile = directoryFile;
    init(operand);
  }

  /** Looks for a suffix on a string and returns
   * either the string with the suffix removed
   * or the original string. */
  private static String trim(String s, String suffix) {
    String trimmed = trimOrNull(s, suffix);
    return trimmed != null ? trimmed : s;
  }

  /** Looks for a suffix on a string and returns
   * either the string with the suffix removed
   * or null. */
  private static String trimOrNull(String s, String suffix) {
    return s.endsWith(suffix)
        ? s.substring(0, s.length() - suffix.length())
        : null;
  }

  private void init(Map<String, ?> operand) {
    try {
      // Look for files in the directory ending in ".avsc"
      final Source baseSource = Sources.of(directoryFile);
      File[] files = directoryFile.listFiles((dir, name) -> {
        return name.endsWith(".avsc");
      });
      if (files == null) {
        System.out.println("directory " + directoryFile + " not found");
        files = new File[0];
      }
      Map<String, Object> configs = new HashMap<>(operand);
      // Build a map from table name to table; each file becomes a table.
      for (File file : files) {
        Source source = Sources.of(file);
        if (source != null) {
          Schema.Parser parser = new Schema.Parser();
          Schema avroSchema = parser.parse(source.file());
          configs.put("schema", avroSchema);
          final Table table = schema.createTable(configs, getRowType(avroSchema));
          tableMap.put(avroSchema.getName(), table);
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static RelDataType getRowType(Schema schema) {
    Pair<List<String>, List<SortedTableColumnType>> types = getFieldTypes(schema);
    List<String> names = types.left;
    List<SortedTableColumnType> fieldTypes = types.right;
    return SortedTableSchema.deduceRowType(names, fieldTypes);
  }

  private static Pair<List<String>, List<SortedTableColumnType>> getFieldTypes(Schema schema) {
    final List<String> names = new ArrayList<>();
    final List<SortedTableColumnType> fieldTypes = new ArrayList<>();
    for (Schema.Field field : schema.getFields()) {
      Schema fieldSchema = field.schema();
      SortedTableColumnType fieldType;
      switch (fieldSchema.getType()) {
        case BOOLEAN:
          fieldType = SortedTableColumnType.BOOLEAN;
          break;
        case INT:
          fieldType = SortedTableColumnType.INT;
          break;
        case LONG:
          fieldType = SortedTableColumnType.LONG;
          break;
        case FLOAT:
          fieldType = SortedTableColumnType.FLOAT;
          break;
        case DOUBLE:
          fieldType = SortedTableColumnType.DOUBLE;
          break;
        case BYTES:
          fieldType = SortedTableColumnType.BYTES;
          break;
        case STRING:
          fieldType = SortedTableColumnType.STRING;
          break;
          // TODO logical
        default:
          throw new IllegalArgumentException("Unsupported type");
      }
      names.add(field.name());
      fieldTypes.add(fieldType);
    }
    return Pair.of(names, fieldTypes);
  }
}

// End CsvSchema.java
