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
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Source;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Base class for table that reads CSV files.
 */
public abstract class SortedTable extends AbstractTable {
  protected final RelProtoDataType protoRowType;
  protected Collection<Object[]> rows;
  protected List<String> names;
  protected List<SortedTableColumnType> fieldTypes;

  /** Creates a CsvTable. */
  SortedTable(Source source, RelProtoDataType protoRowType) {
    this.protoRowType = protoRowType;
    CsvSortedTable csvTable = new CsvSortedTable(source);
    this.rows = csvTable;
    this.names = csvTable.names;
    this.fieldTypes = csvTable.fieldTypes;
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

  /** Returns an array of integers {0, ..., n - 1}. */
  static int[] identityList(int n) {
    int[] integers = new int[n];
    for (int i = 0; i < n; i++) {
      integers[i] = i;
    }
    return integers;
  }
}

// End SortedTable.java
