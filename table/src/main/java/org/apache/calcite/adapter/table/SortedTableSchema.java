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

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.adapter.table.SortedTable.Flavor;
import org.apache.calcite.adapter.table.SortedTable.Kind;
import org.apache.calcite.adapter.table.avro.AvroTableSchema;
import org.apache.calcite.adapter.table.csv.CsvTableSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Schema mapped onto a directory of CSV files. Each table in the schema
 * is a CSV file in that directory.
 */
public class SortedTableSchema extends AbstractSchema {
  private final Kind kind;
  private final Flavor flavor;
  private final AbstractTableSchema tableMap;

  /**
   * Creates a CSV schema.
   */
  public SortedTableSchema(Map<String, Object> operand) {
    super();
    String flavorName = (String) operand.getOrDefault("flavor", Flavor.SCANNABLE.name());
    this.flavor = Flavor.valueOf(flavorName.toUpperCase(Locale.ROOT));
    String kindName = (String) operand.get("kind");
    this.kind = Kind.valueOf(kindName.toUpperCase(Locale.ROOT));
    AbstractTableSchema tableMap = null;
    switch (kind) {
      case AVRO:
        tableMap = new AvroTableSchema();
        break;
      case CSV:
        tableMap = new CsvTableSchema();
        break;
      default:
        throw new IllegalArgumentException("Unsupported kind " + kind);
    }
    tableMap.configure(operand);
    this.tableMap = tableMap;
  }

  public Kind getKind() {
    return kind;
  }

  public Flavor getFlavor() {
    return flavor;
  }

  public void add(String name, Table table) {
    tableMap.put(name, table);
  }

  @Override protected Map<String, Table> getTableMap() {
    return tableMap;
  }

  public static RelDataType deduceRowType(List<String> names,
                                          List<SortedTableColumnType> fieldTypes) {
    JavaTypeFactory typeFactory = new JavaTypeFactoryImpl();
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

  public static SortedTable createTable(Map<String, Object> operand, RelDataType rowType) {
      return createTable(operand, rowType, Collections.emptyList());
  }

  /** Creates different sub-type of table based on the "flavor" attribute. */
  public static SortedTable createTable(Map<String, Object> operand, RelDataType rowType, List<String> keyFields) {
    String flavorName = (String) operand.getOrDefault("flavor", Flavor.SCANNABLE.name());
    Flavor flavor = Flavor.valueOf(flavorName.toUpperCase(Locale.ROOT));
    switch (flavor) {
      case TRANSLATABLE:
        return new SortedTranslatableTable(operand, rowType, keyFields);
      case SCANNABLE:
        return new SortedScannableTable(operand, rowType, keyFields);
      case FILTERABLE:
        return new SortedFilterableTable(operand, rowType, keyFields);
      default:
        throw new AssertionError("Unknown flavor " + flavor);
    }
  }
}

// End CsvSchema.java
