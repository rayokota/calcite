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
import com.google.common.collect.ObjectArrays;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
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
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ModifiableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTableQueryable;
import org.apache.calcite.util.Pair;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Base class for table that reads CSV files.
 */
public abstract class SortedTable extends AbstractQueryableTable implements ModifiableTable {
  private final RelDataType rowType;
  private final AbstractTable<?> rows;
  private final List<String> keyFields;

  /** Creates a CsvTable. */
  SortedTable(Map<String, Object> operand, RelDataType rowType, List<String> keyFields) {
    super(Object[].class);
    String kindName = (String) operand.get("kind");
    Kind kind = Kind.valueOf(kindName.toUpperCase(Locale.ROOT));
    AbstractTable<?> rows = null;
    switch (kind) {
      case AVRO:
        rows = new AvroTable<>(rowType, keyFields);
        break;
      case CSV:
        rows = new CsvTable<>(rowType, keyFields);
        break;
      default:
        throw new IllegalArgumentException("Unsupported kind " + kind);
    }
    rows.configure(operand);
    this.rows = rows;
    this.rowType = rows.getRowType();
    this.keyFields = keyFields;
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

  @SuppressWarnings("unchecked")
  @Override public Collection getModifiableCollection() {
    return new MapWrapper((Map<Object, Object>) rows);
  }

  @Override public <T> Queryable<T> asQueryable(QueryProvider queryProvider,
                                                SchemaPlus schema, String tableName) {
    return new AbstractTableQueryable<T>(queryProvider, schema, this, tableName) {
      public Enumerator<T> enumerator() {
        //noinspection unchecked
        return (Enumerator<T>) Linq4j.iterableEnumerator(
                () -> Iterators.transform(getModifiableCollection().iterator(), SortedTable::toRow));
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

  public static Pair<Object, Object> getKeyValue(Object o, RelDataType rowType, List<String> keyFields) {
    if (!o.getClass().isArray()) {
      return new Pair<>(o, o);
    } else {
      Object[] objs = (Object[]) o;
      if (keyFields.isEmpty()) {
        // Use first field as key
        return new Pair<>(
                new Comparable[]{(Comparable) ((Object[]) o)[0]},
                Arrays.copyOfRange(((Object[]) o), 1, ((Object[]) o).length, Comparable[].class));
      }
      int size = rowType.getFieldCount();
      int keySize = Math.max(1, keyFields.size());
      int valueSize = size - keySize;
      Comparable[] keys = new Comparable[keySize];
      Comparable[] values = new Comparable[valueSize];
      int keyCount = 0;
      int valueCount = 0;
      Set<Integer> keyIndices = new HashSet<>();
      // Use keyFields to order the keys
      for (String keyField : keyFields) {
        int index = rowType.getField(keyField, true, false).getIndex();
        keyIndices.add(index);
        keys[keyCount++] = (Comparable) objs[index];
      }
      for (int i = 0; i < size; i++) {
        if (!keyIndices.contains(i)) {
          values[valueCount++] = (Comparable) objs[i];
        }
      }
      return new Pair<>(keys, values);
    }
  }

  @SuppressWarnings("unchecked")
  public static class MapComparator implements Comparator<Object> {
    private final Comparator defaultComparator = Comparator.nullsFirst(Comparator.naturalOrder());

    @Override
    public int compare(Object o1, Object o2) {
      if (o1.getClass().isArray()) {
        return new ArrayComparator().compare((Comparable[]) o1, (Comparable[]) o2);
      } else {
        return defaultComparator.compare(o1, o2);
      }
    }
  }

  static class ArrayComparator<T extends Comparable<T>> implements Comparator<T[]> {
    private final Comparator<T> defaultComparator = Comparator.<T>nullsFirst(Comparator.<T>naturalOrder());

    @Override
    public int compare(T[] o1, T[] o2) {
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

    private final Map<Object, Object> map;

    public MapWrapper(Map<Object, Object> map) {
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
      Pair<?, ?> keyValue = getKeyValue(o, rowType, keyFields);
      return map.containsKey(keyValue.left);
    }

    @Override
    public Iterator iterator() {
      return toList().iterator();
    }

    @Override
    public Object[] toArray() {
      return toList().toArray();
    }

    @Override
    public Object[] toArray(Object[] a) {
      return toList().toArray(a);
    }

    @Override
    public boolean add(Object o) {
      Pair<?, ?> keyValue = getKeyValue(o, rowType, keyFields);
      map.put(keyValue.left, keyValue.right);
      return true;
    }

    @Override
    public boolean remove(Object o) {
      Pair<?, ?> keyValue = getKeyValue(o, rowType, keyFields);
      return map.remove(keyValue.left, keyValue.right);
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

    private List toList() {
      return map.entrySet().stream()
              .map(entry -> {
                if (entry.getKey().getClass().isArray()) {
                  return ObjectArrays.concat((Object[]) entry.getKey(), (Object[]) entry.getValue(), Object.class);
                } else {
                  return entry.getKey();
                }
              })
              .collect(Collectors.toList());
    }
  }
}

// End SortedTable.java
