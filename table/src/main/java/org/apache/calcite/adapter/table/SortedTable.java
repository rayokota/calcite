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
import org.apache.calcite.adapter.table.avro.AvroTable;
import org.apache.calcite.adapter.table.csv.CsvTable;
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
import org.apache.calcite.util.Pair;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Base class for table that reads CSV files.
 */
public abstract class SortedTable extends AbstractQueryableTable implements ModifiableTable {

  public static final Comparable[] EMPTY_VALUE = new Comparable[0];

  private RelDataType rowType;
  private final AbstractTable rows;
  private final List<String> keyFields;
  private int[] permutationIndices;
  private int[] inverseIndices;

  /** Creates a CsvTable. */
  SortedTable(Map<String, Object> operand, RelDataType rowType, List<String> keyFields) {
    super(Object[].class);
    String kindName = (String) operand.get("kind");
    Kind kind = Kind.valueOf(kindName.toUpperCase(Locale.ROOT));
    AbstractTable rows = null;
    switch (kind) {
      case AVRO:
        rows = new AvroTable(this);
        break;
      case CSV:
        rows = new CsvTable(this);
        break;
      default:
        throw new IllegalArgumentException("Unsupported kind " + kind);
    }
    this.rowType = rowType;
    this.keyFields = keyFields;
    this.rows = rows;
    rows.configure(operand);
  }

  private int[] getPermutationIndices() {
    if (permutationIndices == null) {
      int size = size();
      if (keyFields.isEmpty()) {
        permutationIndices = identityList(size);
      } else {
        int[] result = new int[size];
        Set<Integer> keyIndices = new HashSet<>();
        int index = 0;
        for (String keyField : keyFields) {
          keyIndices.add(index);
          result[index++] = rowType.getField(keyField, true, false).getIndex();
        }
        for (int i = 0; i < size; i++) {
          if (!keyIndices.contains(i)) {
            result[index++] = i;
          }
        }
        permutationIndices = result;
      }
    }
    return permutationIndices;
  }

  private int[] getInverseIndices() {
    if (inverseIndices == null) {
      int[] permutation = getPermutationIndices();
      int size = size();
      int[] result = new int[size];
      for (int i = 0; i < size; i++) {
        result[permutation[i]] = i;
      }
      inverseIndices = result;
    }
    return inverseIndices;
  }

  public int size() {
    return rowType.getFieldCount();
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
    return new MapWrapper(rows);
  }

  @Override public <T> Queryable<T> asQueryable(QueryProvider queryProvider,
                                                SchemaPlus schema, String tableName) {
    return new AbstractTableQueryable<T>(queryProvider, schema, this, tableName) {
      public Enumerator<T> enumerator() {
        //noinspection unchecked
        return (Enumerator<T>) Linq4j.iterableEnumerator(
                () -> Iterators.transform(getModifiableCollection().iterator(), SortedTable::toArray));
      }
    };
  }

  public static Object[] toArray(Object o) {
    return o.getClass().isArray() ? (Object[]) o : new Object[]{o};
  }

  public RelDataType getRowType() {
    return rowType;
  }

  public void setRowType(RelDataType rowType) {
    this.rowType = rowType;
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    if (rowType == null) {
        return null;
    }
    RelProtoDataType protoRowType = RelDataTypeImpl.proto(rowType);
    return protoRowType.apply(typeFactory);
  }

  public enum Kind {
    AVRO, CSV, KAFKA
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

  private Pair<Comparable[], Comparable[]> toKeyValue(Object o) {
    if (!o.getClass().isArray()) {
      return new Pair<>(new Comparable[]{(Comparable) o}, EMPTY_VALUE);
    }
    Object[] objs = (Object[]) o;
    if (keyFields.isEmpty()) {
      // Use first field as key
      return new Pair<>(
              new Comparable[]{(Comparable) ((Object[]) o)[0]},
              Arrays.copyOfRange(((Object[]) o), 1, ((Object[]) o).length, Comparable[].class));
    }
    int keySize = keyFields.size();
    int valueSize = size() - keySize;
    Comparable[] keys = new Comparable[keySize];
    Comparable[] values = new Comparable[valueSize];
    int[] permutation = getPermutationIndices();
    int index = 0;
    for (int i = 0; i < keySize; i++) {
      keys[index++] = (Comparable) objs[permutation[i]];
    }
    index = 0;
    for (int i = keySize; i < valueSize; i++) {
      values[index++] = (Comparable) objs[permutation[i]];
    }
    return new Pair<>(keys, values);
  }

  private Object toRow(Map.Entry<Comparable[], Comparable[]> entry) {
    Comparable[] keys = entry.getKey();
    Comparable[] values = entry.getValue();
    if (keys.length == 1 && values.length == 0) {
      return keys[0];
    }
    int[] inverse = getInverseIndices();
    Object[] row = new Object[inverse.length];
    for (int i = 0; i < inverse.length; i++) {
      int index = inverse[i];
      row[i] = index < keys.length ? keys[index] : values[index - keys.length];
    }
    return row;
  }

  public static class MapComparator implements Comparator<Comparable[]> {
    private final Comparator<Comparable> defaultComparator =
            Comparator.<Comparable>nullsFirst(Comparator.<Comparable>naturalOrder());

    @Override
    public int compare(Comparable[] o1, Comparable[] o2) {
      for (int i = 0; i < Math.min(o1.length, o2.length); i++) {
        int c = defaultComparator.compare(o1[i], o2[i]);
        if (c != 0) {
          return c;
        }
      }
      return Integer.compare(o1.length, o2.length);
    }
  }

  class MapWrapper implements Collection {

    private final Map<Comparable[], Comparable[]> map;

    public MapWrapper(Map<Comparable[], Comparable[]> map) {
      this.map = map;
    }

    @Override
    public int size() {
      return map.size();
    }

    @Override
    public boolean isEmpty() {
      return map.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
      Pair<Comparable[], Comparable[]> keyValue = toKeyValue(o);
      return map.containsKey(keyValue.left);
    }

    @Override
    public Iterator iterator() {
      return toStream().iterator();
    }

    @Override
    public Object[] toArray() {
      return toStream().toArray();
    }

    @Override
    public Object[] toArray(Object[] a) {
      return toStream().toArray();
    }

    @Override
    public boolean add(Object o) {
      Pair<Comparable[], Comparable[]> keyValue = toKeyValue(o);
      map.put(keyValue.left, keyValue.right);
      return true;
    }

    @Override
    public boolean remove(Object o) {
      Pair<Comparable[], Comparable[]> keyValue = toKeyValue(o);
      return map.remove(keyValue.left) != null;
    }

    @Override
    public boolean containsAll(Collection c) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean addAll(Collection c) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(Collection c) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(Collection c) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
      map.clear();
    }

    private Stream toStream() {
      return map.entrySet().stream().map(SortedTable.this::toRow);
    }
  }
}

// End SortedTable.java
