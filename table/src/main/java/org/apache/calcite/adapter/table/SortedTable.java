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
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Source;
import org.apache.commons.lang3.time.FastDateFormat;

import java.io.IOException;
import java.io.Reader;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

/**
 * Base class for table that reads CSV files.
 */
public abstract class SortedTable extends AbstractTable {
  protected final RelProtoDataType protoRowType;
  protected List<String> names;
  protected List<SortedTableColumnType> fieldTypes;
  protected List<Object[]> rows;

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
  SortedTable(Source source, RelProtoDataType protoRowType) {
    try (CSVReader reader = openCsv(source)){
      this.protoRowType = protoRowType;
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

  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    if (protoRowType != null) {
      return protoRowType.apply(typeFactory);
    }
    return deduceRowType((JavaTypeFactory) typeFactory, names, fieldTypes);
  }

  /** Various degrees of table "intelligence". */
  public enum Flavor {
    SCANNABLE, FILTERABLE, TRANSLATABLE
  }

  private static RowConverter converter(List<SortedTableColumnType> fieldTypes) {
    final int[] fields = identityList(fieldTypes.size());
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

  private static RelDataType deduceRowType(JavaTypeFactory typeFactory, List<String> names,
                                   List<SortedTableColumnType> fieldTypes) {
    List<RelDataType> types = new ArrayList<>();
    for (SortedTableColumnType fieldType : fieldTypes) {
      final RelDataType type;
      if (fieldType == null) {
        type = typeFactory.createSqlType(SqlTypeName.VARCHAR);
      } else {
        type = fieldType.toType(typeFactory);
      }
      types.add(type);
    }
    return typeFactory.createStructType(Pair.zip(names, types));
  }

  private static CSVReader openCsv(Source source) throws IOException {
    final Reader fileReader = source.reader();
    return new CSVReader(fileReader);
  }

  /** Returns an array of integers {0, ..., n - 1}. */
  static int[] identityList(int n) {
    int[] integers = new int[n];
    for (int i = 0; i < n; i++) {
      integers[i] = i;
    }
    return integers;
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

// End CsvTable.java
