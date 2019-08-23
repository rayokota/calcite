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
import org.apache.calcite.adapter.table.SortedTable;
import org.apache.calcite.adapter.table.SortedTableColumnType;
import org.apache.calcite.avatica.util.Base64;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.model.ModelHandler;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.util.Source;
import org.apache.calcite.util.Sources;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.utils.Bytes;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.math.BigDecimal;
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
public class CsvSortedTable<E> implements Map<E, E>, Configurable {
  private List<String> names;
  private List<SortedTableColumnType> fieldTypes;
  private Map<E, E> rows;
  private RelDataType rowType;

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
  public CsvSortedTable(RelDataType rowType) {
    this.rows = new TreeMap<E, E>(new SortedTable.MapComparator());
    this.rowType = rowType;
  }

  public RelDataType getRowType() {
    return rowType;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void configure(Map<String, ?> operand) {
    String fileName = (String) operand.get("file");
    if (fileName != null) {
      final File base = (File) operand.get(ModelHandler.ExtraOperand.BASE_DIRECTORY.camelName);
      final Source source = base != null ? Sources.file(base, fileName) : Sources.of(new File(fileName));
      if (rowType == null) {
        this.rowType = CsvSortedTableSchema.getRowType(source);
      }
      try (CSVReader reader = openCsv(source)) {
        String[] strings = reader.readNext(); // skip header row
        List<SortedTableColumnType> fieldTypes = getFieldTypes(rowType);
        //noinspection unchecked
        RowConverter<E> rowConverter = (RowConverter<E>) converter(fieldTypes);
        String[] row = reader.readNext();
        while (row != null) {
          E elem = rowConverter.convertRow(row);
          E keyArray = (E) SortedTable.getKey(elem);
          E valueArray = (E) SortedTable.getValue(elem);
          rows.put(keyArray, valueArray);
          row = reader.readNext();
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private static CSVReader openCsv(Source source) throws IOException {
    final Reader fileReader = source.reader();
    return new CSVReader(fileReader);
  }

  private static List<SortedTableColumnType> getFieldTypes(RelDataType rowType) {
    final List<SortedTableColumnType> fieldTypes = new ArrayList<>();
    for (RelDataTypeField field : rowType.getFieldList()) {
      fieldTypes.add(SortedTableColumnType.of(field.getType().getSqlTypeName()));
    }
    return fieldTypes;
  }

  private static RowConverter<?> converter(List<SortedTableColumnType> fieldTypes) {
    final int[] fields = SortedTable.identityList(fieldTypes.size());
    if (fields.length == 1) {
      final int field = fields[0];
      return new SingleColumnRowConverter(fieldTypes.get(field), field);
    } else {
      return new ArrayRowConverter(fieldTypes, fields);
    }
  }

  /** Row converter.
   *
   * @param <E> element type */
  abstract static class RowConverter<E> {
    abstract E convertRow(String[] rows);

    protected Object convert(SortedTableColumnType fieldType, String string) {
      if (fieldType == null) {
        return string;
      }
      switch (fieldType) {
        case BOOLEAN:
          if (string.length() == 0) {
            return null;
          }
          return Boolean.parseBoolean(string);
        case INT:
          if (string.length() == 0) {
            return null;
          }
          return Integer.parseInt(string);
        case LONG:
          if (string.length() == 0) {
            return null;
          }
          return Long.parseLong(string);
        case FLOAT:
          if (string.length() == 0) {
            return null;
          }
          return Float.parseFloat(string);
        case DOUBLE:
          if (string.length() == 0) {
            return null;
          }
          return Double.parseDouble(string);
        case BYTES:
          if (string.length() == 0) {
            return Bytes.wrap(new byte[0]);
          }
          try {
            return Bytes.wrap(Base64.decode(string));
          } catch (IOException e) {
            return null;
          }
        case DECIMAL:
          if (string.length() == 0) {
            return null;
          }
          return new BigDecimal(string);
        case DATE:
          if (string.length() == 0) {
            return null;
          }
          try {
            Date date = TIME_FORMAT_DATE.parse(string);
            return (int) (date.getTime() / DateTimeUtils.MILLIS_PER_DAY);
          } catch (ParseException e) {
            return null;
          }
        case TIME:
          if (string.length() == 0) {
            return null;
          }
          try {
            Date date = TIME_FORMAT_TIME.parse(string);
            return (int) date.getTime();
          } catch (ParseException e) {
            return null;
          }
        case TIMESTAMP:
          if (string.length() == 0) {
            return null;
          }
          try {
            Date date = TIME_FORMAT_TIMESTAMP.parse(string);
            return date.getTime();
          } catch (ParseException e) {
            return null;
          }
        case STRING:
        default:
          return string;
      }
    }
  }

  /** Array row converter. */
  private static class ArrayRowConverter extends RowConverter<Object[]> {
    private final SortedTableColumnType[] fieldTypes;
    private final int[] fields;

    ArrayRowConverter(List<SortedTableColumnType> fieldTypes, int[] fields) {
      this.fieldTypes = fieldTypes.toArray(new SortedTableColumnType[0]);
      this.fields = fields;
    }

    public Object[] convertRow(String[] strings) {
      final Object[] objects = new Object[fields.length];
      for (int i = 0; i < fields.length; i++) {
        int field = fields[i];
        objects[i] = convert(fieldTypes[field], strings[field]);
      }
      return objects;
    }
  }

  /** Single column row converter. */
  private static class SingleColumnRowConverter extends RowConverter<Object> {
    private final SortedTableColumnType fieldType;
    private final int fieldIndex;

    private SingleColumnRowConverter(SortedTableColumnType fieldType, int fieldIndex) {
      this.fieldType = fieldType;
      this.fieldIndex = fieldIndex;
    }

    public Object convertRow(String[] strings) {
      return convert(fieldType, strings[fieldIndex]);
    }
  }

  @Override
  public int size() {
    return rows.size();
  }

  @Override
  public boolean isEmpty() {
    return rows.isEmpty();
  }

  @Override
  public boolean containsKey(Object key) {
    return rows.containsKey(key);
  }

  @Override
  public boolean containsValue(Object value) {
    return rows.containsValue(value);
  }

  @Override
  public E get(Object key) {
    return rows.get(key);
  }

  @Override
  public E put(E key, E value) {
    return rows.put(key, value);
  }

  @Override
  public E remove(Object key) {
    return rows.remove(key);
  }

  @Override
  public void putAll(Map<? extends E, ? extends E> m) {
    rows.putAll(m);
  }

  @Override
  public void clear() {
    rows.clear();
  }

  @Override
  public Set<E> keySet() {
    return rows.keySet();
  }

  @Override
  public Collection<E> values() {
    return rows.values();
  }

  @Override
  public Set<Entry<E, E>> entrySet() {
    return rows.entrySet();
  }

  @Override
  public boolean equals(Object o) {
    return rows.equals(o);
  }

  @Override
  public int hashCode() {
    return rows.hashCode();
  }

  @Override
  public E getOrDefault(Object key, E defaultValue) {
    return rows.getOrDefault(key, defaultValue);
  }

  @Override
  public void forEach(BiConsumer<? super E, ? super E> action) {
    rows.forEach(action);
  }

  @Override
  public void replaceAll(BiFunction<? super E, ? super E, ? extends E> function) {
    rows.replaceAll(function);
  }

  @Override
  public E putIfAbsent(E key, E value) {
    return rows.putIfAbsent(key, value);
  }

  @Override
  public boolean remove(Object key, Object value) {
    return rows.remove(key, value);
  }

  @Override
  public boolean replace(E key, E oldValue, E newValue) {
    return rows.replace(key, oldValue, newValue);
  }

  @Override
  public E replace(E key, E value) {
    return rows.replace(key, value);
  }

  @Override
  public E computeIfAbsent(E key, Function<? super E, ? extends E> mappingFunction) {
    return rows.computeIfAbsent(key, mappingFunction);
  }

  @Override
  public E computeIfPresent(E key, BiFunction<? super E, ? super E, ? extends E> remappingFunction) {
    return rows.computeIfPresent(key, remappingFunction);
  }

  @Override
  public E compute(E key, BiFunction<? super E, ? super E, ? extends E> remappingFunction) {
    return rows.compute(key, remappingFunction);
  }

  @Override
  public E merge(E key, E value, BiFunction<? super E, ? super E, ? extends E> remappingFunction) {
    return rows.merge(key, value, remappingFunction);
  }
}

// End CsvSortedTable.java
