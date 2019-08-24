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
package org.apache.calcite.adapter.table.csv;

import au.com.bytecode.opencsv.CSVReader;
import com.google.common.collect.ForwardingMap;
import org.apache.calcite.adapter.table.AbstractSortedTable;
import org.apache.calcite.adapter.table.AbstractSortedTableSchema;
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
import java.nio.file.Paths;
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
public class CsvSortedTableSchema extends AbstractSortedTableSchema {
  private final Map<String, Table> tableMap;
  private File directoryFile;

  /**
   * Creates a CSV schema.
   */
  public CsvSortedTableSchema() {
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
    // Look for files in the directory ending in ".csv", ".csv.gz"
    final Source baseSource = Sources.of(directoryFile);
    File[] files = directoryFile.listFiles((dir, name) -> {
      final String nameSansGz = trim(name, ".gz");
      return nameSansGz.endsWith(".csv");
    });
    if (files == null) {
      System.out.println("directory " + directoryFile + " not found");
      files = new File[0];
    }
    Map<String, Object> configs = new HashMap<>(operand);
    // Build a map from table name to table; each file becomes a table.
    for (File file : files) {
      Source source = Sources.of(file);
      Source sourceSansGz = source.trim(".gz");
      final Source sourceSansCsv = sourceSansGz.trimOrNull(".csv");
      if (sourceSansCsv != null) {
        configs.put("file", source.file().getName());
        final Table table = SortedTableSchema.createTable(configs, getRowType(source));
        tableMap.put(sourceSansCsv.relative(baseSource).path(), table);
      }
    }
  }

  public static RelDataType getRowType(Source source) {
    try (CSVReader reader = openCsv(source)) {
      String[] strings = reader.readNext(); // get header row
      Pair<List<String>, List<SortedTableColumnType>> types = getFieldTypes(strings);
      List<String> names = types.left;
      List<SortedTableColumnType> fieldTypes = types.right;
      return SortedTableSchema.deduceRowType(names, fieldTypes);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static CSVReader openCsv(Source source) throws IOException {
    final Reader fileReader = source.reader();
    return new CSVReader(fileReader);
  }

  private static Pair<List<String>, List<SortedTableColumnType>> getFieldTypes(String[] strings) {
    final List<String> names = new ArrayList<>();
    final List<SortedTableColumnType> fieldTypes = new ArrayList<>();
    if (strings == null) {
      strings = new String[]{"EmptyFileHasNoColumns:boolean"};
    }
    for (String string : strings) {
      final String name;
      final SortedTableColumnType fieldType;
      final int colon = string.indexOf(':');
      if (colon >= 0) {
        name = string.substring(0, colon);
        String typeString = string.substring(colon + 1);
        fieldType = SortedTableColumnType.of(typeString);
        if (fieldType == null) {
          System.out.println("WARNING: Found unknown type: "
                  + typeString + " in file: "
                  + " for column: " + name
                  + ". Will assume the type of column is string");
        }
      } else {
        name = string;
        fieldType = null;
      }
      names.add(name);
      fieldTypes.add(fieldType);
    }
    if (names.isEmpty()) {
      names.add("line");
      fieldTypes.add(null);
    }
    return Pair.of(names, fieldTypes);
  }
}

// End CsvSchema.java
