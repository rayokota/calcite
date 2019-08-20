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

import org.apache.calcite.linq4j.Enumerator;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

/** Enumerator that reads from a CSV file.
 *
 * @param <E> Row type
 */
class SortedTableEnumerator<E> implements Enumerator<E> {
  private final Iterator<Object[]> rows;
  private final String[] filterValues;
  private final AtomicBoolean cancelFlag;
  private final int[] fields;
  private E current;

  SortedTableEnumerator(Iterator<Object[]> rows, AtomicBoolean cancelFlag,
                        String[] filterValues, int[] fields) {
    this.rows = rows;
    this.cancelFlag = cancelFlag;
    this.filterValues = filterValues;
    this.fields = fields;
  }

  public E current() {
    return current;
  }

  public boolean moveNext() {
    outer:
      for (;;) {
        if (cancelFlag.get()) {
          return false;
        }
        final Object[] row = rows.hasNext() ? rows.next() : null;
        if (row == null) {
          current = null;
          return false;
        }
        if (filterValues != null) {
          for (int i = 0; i < row.length; i++) {
            String filterValue = filterValues[i];
            if (filterValue != null) {
              if (!filterValue.equals(row[i].toString())) {
                continue outer;
              }
            }
          }
        }
        current = project(row, fields);
        return true;
      }
  }

  @SuppressWarnings("unchecked")
  private E project(Object[] row, int[] fields) {
    if (fields.length == 1) {
      return (E) row[fields[0]];
    }
    final Object[] objects = new Object[fields.length];
    for (int i = 0; i < fields.length; i++) {
      int field = fields[i];
      objects[i] = row[field];
    }
    return (E) objects;
  }

  public void reset() {
    throw new UnsupportedOperationException();
  }

  public void close() {
  }
}

// End SortedTableEnumerator.java
