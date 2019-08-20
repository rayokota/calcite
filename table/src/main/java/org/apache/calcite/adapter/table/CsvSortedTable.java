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
package org.apache.calcite.adapter.table;

import au.com.bytecode.opencsv.CSVReader;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Source;
import org.apache.commons.lang3.time.FastDateFormat;

import java.io.IOException;
import java.io.Reader;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.TimeZone;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * Base class for table that reads CSV files.
 */
public class CsvSortedTable implements Collection<Object[]> {
  protected List<String> names;
  protected List<SortedTableColumnType> fieldTypes;
  protected Collection<Object[]> rows;

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
  CsvSortedTable(Source source) {
    try (CSVReader reader = openCsv(source)){
      String[] strings = reader.readNext(); // skip header row
      Pair<List<String>, List<SortedTableColumnType>> types = getFieldTypes(strings);
      this.names = types.left;
      this.fieldTypes = types.right;
      this.rows = new ArrayList<>();
      RowConverter rowConverter = converter(fieldTypes);
      String[] row = reader.readNext();
      while (row != null) {
        rows.add(rowConverter.convertRow(row));
        row = reader.readNext();
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
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
  public boolean contains(Object o) {
    return rows.contains(o);
  }

  @Override
  public Iterator<Object[]> iterator() {
    return rows.iterator();
  }

  @Override
  public Object[] toArray() {
    return rows.toArray();
  }

  @Override
  public <T> T[] toArray(T[] a) {
    return rows.toArray(a);
  }

  @Override
  public boolean add(Object[] objects) {
    return rows.add(objects);
  }

  @Override
  public boolean remove(Object o) {
    return rows.remove(o);
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    return rows.containsAll(c);
  }

  @Override
  public boolean addAll(Collection<? extends Object[]> c) {
    return rows.addAll(c);
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    return rows.removeAll(c);
  }

  @Override
  public boolean removeIf(Predicate<? super Object[]> filter) {
    return rows.removeIf(filter);
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    return rows.retainAll(c);
  }

  @Override
  public void clear() {
    rows.clear();
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
  public Spliterator<Object[]> spliterator() {
    return rows.spliterator();
  }

  @Override
  public Stream<Object[]> stream() {
    return rows.stream();
  }

  @Override
  public Stream<Object[]> parallelStream() {
    return rows.parallelStream();
  }

  private static RowConverter converter(List<SortedTableColumnType> fieldTypes) {
    final int[] fields = SortedTable.identityList(fieldTypes.size());
    return new ArrayRowConverter(fieldTypes, fields);
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

  private static CSVReader openCsv(Source source) throws IOException {
    final Reader fileReader = source.reader();
    return new CSVReader(fileReader);
  }

  /** Row converter.
   */
  abstract static class RowConverter {
    abstract Object[] convertRow(String[] rows);

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
        case BYTE:
          if (string.length() == 0) {
            return null;
          }
          return Byte.parseByte(string);
        case SHORT:
          if (string.length() == 0) {
            return null;
          }
          return Short.parseShort(string);
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
  static class ArrayRowConverter extends RowConverter {
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
}

// End CsvSortedTable.java
