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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.util.Source;
import org.apache.calcite.util.Sources;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

/**
 * Schema mapped onto a directory of CSV files. Each table in the schema
 * is a CSV file in that directory.
 */
public class SortedTableSchema extends AbstractSchema {
  private SortedTable.Flavor flavor;
  private Map<String, Table> tableMap;

  /**
   * Creates a CSV schema.
   *
   * @param directoryFile Directory that holds {@code .csv} files
   * @param flavor     Whether to instantiate flavor tables that undergo
   *                   query optimization
   */
  public SortedTableSchema(File directoryFile, SortedTable.Flavor flavor) {
    super();
    this.flavor = flavor;
    this.tableMap = new CsvSortedTableSchema(this, directoryFile);
  }

  public void add(String name, Table table) {
    tableMap.put(name, table);
  }

  @Override protected Map<String, Table> getTableMap() {
    return tableMap;
  }

  /** Creates different sub-type of table based on the "flavor" attribute. */
  public Table createTable(Source source, RelDataType rowType) {
    switch (flavor) {
    case TRANSLATABLE:
      return new SortedTranslatableTable(source, rowType);
    case SCANNABLE:
      return new SortedScannableTable(source, rowType);
    case FILTERABLE:
      return new SortedFilterableTable(source, rowType);
    default:
      throw new AssertionError("Unknown flavor " + flavor);
    }
  }
}

// End CsvSchema.java
