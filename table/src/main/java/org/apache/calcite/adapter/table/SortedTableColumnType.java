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
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

/**
 * Type of a field in a CSV file.
 *
 * <p>Usually, and unless specified explicitly in the header row, a field is
 * of type {@link #STRING}. But specifying the field type in the header row
 * makes it easier to write SQL.</p>
 */
public enum SortedTableColumnType {
  BOOLEAN(Primitive.BOOLEAN, SqlTypeName.BOOLEAN),
  INT(Primitive.INT, SqlTypeName.INTEGER),
  LONG(Primitive.LONG, SqlTypeName.BIGINT),
  FLOAT(Primitive.FLOAT, SqlTypeName.REAL),
  DOUBLE(Primitive.DOUBLE, SqlTypeName.DOUBLE),
  BYTES(byte[].class, "bytes", SqlTypeName.VARBINARY),
  STRING(String.class, "string", SqlTypeName.VARCHAR),
  DECIMAL(BigDecimal.class, "decimal", SqlTypeName.DECIMAL),
  DATE(java.sql.Date.class, "date", SqlTypeName.DATE),
  TIME(java.sql.Time.class, "time", SqlTypeName.TIME),
  TIMESTAMP(java.sql.Timestamp.class, "timestamp", SqlTypeName.TIMESTAMP);

  private final Class clazz;
  private final String simpleName;
  private final SqlTypeName sqlType;

  private static final Map<String, SortedTableColumnType> NAMES = new HashMap<>();
  private static final Map<SqlTypeName, SortedTableColumnType> TYPES = new HashMap<>();

  static {
    for (SortedTableColumnType value : values()) {
      NAMES.put(value.simpleName, value);
      TYPES.put(value.sqlType, value);
    }
  }

  SortedTableColumnType(Primitive primitive, SqlTypeName sqlType) {
    this(primitive.boxClass, primitive.primitiveName, sqlType);
  }

  SortedTableColumnType(Class clazz, String simpleName, SqlTypeName sqlType) {
    this.clazz = clazz;
    this.simpleName = simpleName;
    this.sqlType = sqlType;
  }

  public RelDataType toType(JavaTypeFactory typeFactory) {
    RelDataType javaType = typeFactory.createJavaType(clazz);
    RelDataType sqlType = typeFactory.createSqlType(javaType.getSqlTypeName());
    return typeFactory.createTypeWithNullability(sqlType, true);
  }

  public static SortedTableColumnType of(String typeString) {
    return NAMES.get(typeString);
  }

  public static SortedTableColumnType of(SqlTypeName sqlType) {
    return TYPES.get(sqlType);
  }
}

// End CsvFieldType.java
