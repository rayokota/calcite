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
import org.apache.calcite.adapter.table.AbstractTableSchema;
import org.apache.calcite.adapter.table.SortedTableColumnType;
import org.apache.calcite.adapter.table.SortedTableSchema;
import org.apache.calcite.model.ModelHandler;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.Table;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Source;
import org.apache.calcite.util.Sources;

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
public class AvroTableSchema extends AbstractTableSchema {
  private final Map<String, Table> tableMap;
  private File directoryFile;

  /**
   * Creates a CSV schema.
   */
  public AvroTableSchema() {
    this.tableMap = new HashMap<>();
  }

  @Override
  protected Map<String, Table> delegate() {
    return tableMap;
  }

  @Override
  public void configure(Map<String, ?> operand) {
    final String directory = (String) operand.get("directory");
    final File base = (File) operand.get(ModelHandler.ExtraOperand.BASE_DIRECTORY.camelName);
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
      File[] files = directoryFile.listFiles((dir, name) -> name.endsWith(".avsc"));
      if (files == null) {
        System.out.println("directory " + directoryFile + " not found");
        files = new File[0];
      }
      Map<String, Object> configs = new HashMap<>(operand);
      // Build a map from table name to table; each file becomes a table.
      for (File file : files) {
        Source source = Sources.of(file);
        Schema.Parser parser = new Schema.Parser();
        Schema avroSchema = parser.parse(source.file());
        configs.put("schema", avroSchema);
        // TODO use primary key annotation
        String name = avroSchema.getName();
        Pair<RelDataType, List<String>> rowType = getRowType(avroSchema);
        final Table table = SortedTableSchema.createTable(name, configs, rowType.left, rowType.right);
        tableMap.put(name, table);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static Pair<RelDataType, List<String>> getRowType(Schema schema) {
    Pair<Pair<List<String>, List<SortedTableColumnType>>, List<String>> types = toColumnTypes(schema);
    List<String> names = types.left.left;
    List<SortedTableColumnType> fieldTypes = types.left.right;
    List<String> keyFields = types.right;
    return Pair.of(SortedTableSchema.toRowType(names, fieldTypes), keyFields);
  }

  private static Pair<Pair<List<String>, List<SortedTableColumnType>>, List<String>> toColumnTypes(Schema schema) {
    int size = schema.getFields().size();
    final List<String> names = new ArrayList<>(size);
    final List<SortedTableColumnType> fieldTypes = new ArrayList<>(size);
    String[] keyNames = new String[size];
    for (Schema.Field field : schema.getFields()) {
      Schema fieldSchema = field.schema();
      Integer keyIndex = (Integer) field.getObjectProp("sql.key.index");
      if (keyIndex != null) {
        keyNames[keyIndex] = field.name();
      }
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
    List<String> keyFields = new ArrayList<>(size);
    for (int i = 0; i < keyNames.length; i++) {
      String keyName = keyNames[i];
      if (keyName == null) {
        break;
      }
      keyFields.add(keyName);
    }
    return Pair.of(Pair.of(names, fieldTypes), keyFields);
  }
}

// End CsvSchema.java
