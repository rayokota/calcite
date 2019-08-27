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
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TableFactory;

import java.util.Collections;
import java.util.Map;

/**
 * Factory that creates a {@link SortedTranslatableTable}.
 *
 * <p>Allows a CSV table to be included in a model.json file, even in a
 * schema that is not based upon {@link SortedTableSchema}.
 */
@SuppressWarnings("UnusedDeclaration")
public class SortedTableFactory implements TableFactory<SortedTable> {
  // public constructor, per factory contract
  public SortedTableFactory() {
  }

  public SortedTable create(SchemaPlus schema, String name,
                            Map<String, Object> operand, RelDataType rowType) {
    return SortedTableSchema.createTable(name, operand, rowType, Collections.emptyList());
  }
}

// End CsvTableFactory.java
