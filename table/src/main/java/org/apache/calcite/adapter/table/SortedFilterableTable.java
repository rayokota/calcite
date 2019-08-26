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
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.FilterableTable;
import org.apache.calcite.sql.SqlKind;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Table based on a CSV file that can implement simple filtering.
 *
 * <p>It implements the {@link FilterableTable} interface, so Calcite gets
 * data by calling the {@link #scan(DataContext, List)} method.
 */
public class SortedFilterableTable extends SortedTable
    implements FilterableTable {
  /** Creates a CsvFilterableTable. */
  public SortedFilterableTable(String name, Map<String, Object> operand, RelDataType rowType, List<String> keyFields) {
    super(name, operand, rowType, keyFields);
  }

  public String toString() {
    return "SortedFilterableTable";
  }

  public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters) {
    final String[] filterValues = new String[size()];
    filters.removeIf(filter -> addFilter(filter, filterValues));
    final int[] fields = identityList(size());
    final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get(root);
    return new AbstractEnumerable<Object[]>() {
      public Enumerator<Object[]> enumerator() {
        return new SortedTableEnumerator<>(Iterators.<Object, Object[]>transform(
                getModifiableCollection().iterator(), SortedTable::toArray),
                cancelFlag, filterValues, fields);
      }
    };
  }

  private boolean addFilter(RexNode filter, Object[] filterValues) {
    if (filter.isA(SqlKind.AND)) {
        // We cannot refine(remove) the operands of AND,
        // it will cause o.a.c.i.TableScanNode.createFilterable filters check failed.
      ((RexCall) filter).getOperands().forEach(subFilter -> addFilter(subFilter, filterValues));
    } else if (filter.isA(SqlKind.EQUALS)) {
      final RexCall call = (RexCall) filter;
      RexNode left = call.getOperands().get(0);
      if (left.isA(SqlKind.CAST)) {
        left = ((RexCall) left).operands.get(0);
      }
      final RexNode right = call.getOperands().get(1);
      if (left instanceof RexInputRef
          && right instanceof RexLiteral) {
        final int index = ((RexInputRef) left).getIndex();
        if (filterValues[index] == null) {
          filterValues[index] = ((RexLiteral) right).getValue2().toString();
          return true;
        }
      }
    }
    return false;
  }
}

// End CsvFilterableTable.java
