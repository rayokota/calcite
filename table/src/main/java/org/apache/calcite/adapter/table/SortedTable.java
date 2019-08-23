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

import com.google.common.collect.Iterators;
import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.adapter.table.csv.CsvSortedTable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ModifiableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTableQueryable;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * Base class for table that reads CSV files.
 */
public abstract class SortedTable extends AbstractQueryableTable implements ModifiableTable {
  protected final RelDataType rowType;
  protected final Collection<?> rows;

  /** Creates a CsvTable. */
  SortedTable(Map<String, Object> operand, RelDataType rowType) {
    super(Object[].class);
    CsvSortedTable<?> csvTable = new CsvSortedTable<>(rowType);
    csvTable.configure(operand);
    this.rows = csvTable;
    this.rowType = csvTable.getRowType();
  }

  public int size() {
    return rowType.getFieldList().size();
  }

  @Override public TableModify toModificationRel(
          RelOptCluster cluster,
          RelOptTable table,
          Prepare.CatalogReader catalogReader,
          RelNode child,
          TableModify.Operation operation,
          List<String> updateColumnList,
          List<RexNode> sourceExpressionList,
          boolean flattened) {
    return LogicalTableModify.create(table, catalogReader, child, operation,
            updateColumnList, sourceExpressionList, flattened);
  }

  @Override public Collection getModifiableCollection() {
    return rows;
  }

  @Override public <T> Queryable<T> asQueryable(QueryProvider queryProvider,
                                                SchemaPlus schema, String tableName) {
    return new AbstractTableQueryable<T>(queryProvider, schema, this, tableName) {
      public Enumerator<T> enumerator() {
        //noinspection unchecked
        return (Enumerator<T>) Linq4j.iterableEnumerator(
                () -> Iterators.transform(rows.iterator(), SortedTable::toRow));
      }
    };
  }

  public static Object[] toRow(Object o) {
    return o.getClass().isArray() ? (Object[]) o : new Object[]{o};
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    if (rowType == null) {
        return null;
    }
    RelProtoDataType protoRowType = RelDataTypeImpl.proto(rowType);
    return protoRowType.apply(typeFactory);
  }

  /** Various degrees of table "intelligence". */
  public enum Flavor {
    SCANNABLE, FILTERABLE, TRANSLATABLE
  }

  /** Returns an array of integers {0, ..., n - 1}. */
  public static int[] identityList(int n) {
    int[] integers = new int[n];
    for (int i = 0; i < n; i++) {
      integers[i] = i;
    }
    return integers;
  }

  static class ArrayComparator<T extends Comparable<T>> implements Comparator<T[]> {

    @Override
    public int compare(T[] o1, T[] o2) {
      for (int i = 0; i < Math.min(o1.length, o2.length); i++) {
        int c = o1[i].compareTo(o2[i]);
        if (c != 0) {
          return c;
        }
      }
      return Integer.compare(o1.length, o2.length);
    }

  }
}

// End SortedTable.java
