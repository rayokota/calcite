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
public class CsvSortedTableSchema implements Map<String, Table>, Configurable {
  private final SortedTableSchema schema;
  private final Map<String, Table> tableMap;
  private File directoryFile;

  /**
   * Creates a CSV schema.
   */
  public CsvSortedTableSchema(SortedTableSchema schema) {
    this.schema = schema;
    this.tableMap = new HashMap<>();
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
    init();
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

  private void init() {
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
    // Build a map from table name to table; each file becomes a table.
    for (File file : files) {
      Source source = Sources.of(file);
      Source sourceSansGz = source.trim(".gz");
      final Source sourceSansCsv = sourceSansGz.trimOrNull(".csv");
      if (sourceSansCsv != null) {
        Map<String, Object> operand = new HashMap<>();
        operand.put("file", source.path());
        final Table table = schema.createTable(operand, getRowType(source));
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

  @Override
  public int size() {
    return tableMap.size();
  }

  @Override
  public boolean isEmpty() {
    return tableMap.isEmpty();
  }

  @Override
  public boolean containsKey(Object key) {
    return tableMap.containsKey(key);
  }

  @Override
  public boolean containsValue(Object value) {
    return tableMap.containsValue(value);
  }

  @Override
  public Table get(Object key) {
    return tableMap.get(key);
  }

  @Override
  public Table put(String key, Table value) {
    return tableMap.put(key, value);
  }

  @Override
  public Table remove(Object key) {
    return tableMap.remove(key);
  }

  @Override
  public void putAll(Map<? extends String, ? extends Table> m) {
    tableMap.putAll(m);
  }

  @Override
  public void clear() {
    tableMap.clear();
  }

  @Override
  public Set<String> keySet() {
    return tableMap.keySet();
  }

  @Override
  public Collection<Table> values() {
    return tableMap.values();
  }

  @Override
  public Set<Entry<String, Table>> entrySet() {
    return tableMap.entrySet();
  }

  @Override
  public boolean equals(Object o) {
    return tableMap.equals(o);
  }

  @Override
  public int hashCode() {
    return tableMap.hashCode();
  }

  @Override
  public Table getOrDefault(Object key, Table defaultValue) {
    return tableMap.getOrDefault(key, defaultValue);
  }

  @Override
  public void forEach(BiConsumer<? super String, ? super Table> action) {
    tableMap.forEach(action);
  }

  @Override
  public void replaceAll(BiFunction<? super String, ? super Table, ? extends Table> function) {
    tableMap.replaceAll(function);
  }

  @Override
  public Table putIfAbsent(String key, Table value) {
    return tableMap.putIfAbsent(key, value);
  }

  @Override
  public boolean remove(Object key, Object value) {
    return tableMap.remove(key, value);
  }

  @Override
  public boolean replace(String key, Table oldValue, Table newValue) {
    return tableMap.replace(key, oldValue, newValue);
  }

  @Override
  public Table replace(String key, Table value) {
    return tableMap.replace(key, value);
  }

  @Override
  public Table computeIfAbsent(String key, Function<? super String, ? extends Table> mappingFunction) {
    return tableMap.computeIfAbsent(key, mappingFunction);
  }

  @Override
  public Table computeIfPresent(String key, BiFunction<? super String, ? super Table, ? extends Table> remappingFunction) {
    return tableMap.computeIfPresent(key, remappingFunction);
  }

  @Override
  public Table compute(String key, BiFunction<? super String, ? super Table, ? extends Table> remappingFunction) {
    return tableMap.compute(key, remappingFunction);
  }

  @Override
  public Table merge(String key, Table value, BiFunction<? super Table, ? super Table, ? extends Table> remappingFunction) {
    return tableMap.merge(key, value, remappingFunction);
  }
}

// End CsvSchema.java
