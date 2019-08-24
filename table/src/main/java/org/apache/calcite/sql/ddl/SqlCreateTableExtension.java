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
package org.apache.calcite.sql.ddl;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.adapter.table.SortedTable;
import org.apache.calcite.adapter.table.SortedTableSchema;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Expression;
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
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.schema.ModifiableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.schema.impl.AbstractTableQueryable;
import org.apache.calcite.schema.impl.ViewTable;
import org.apache.calcite.schema.impl.ViewTableMacro;
import org.apache.calcite.sql.SqlCreate;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlExecutableStatement;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql2rel.InitializerContext;
import org.apache.calcite.sql2rel.InitializerExpressionFactory;
import org.apache.calcite.sql2rel.NullInitializerExpressionFactory;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * Simple test example of a CREATE TABLE statement.
 */
public class SqlCreateTableExtension extends SqlCreateTable {
  private final SqlIdentifier name;
  private final SqlNodeList columnList;
  private final SqlNode query;

  /** Creates a SqlCreateTable. */
  public SqlCreateTableExtension(SqlParserPos pos, boolean replace, boolean ifNotExists,
                                 SqlIdentifier name, SqlNodeList columnList, SqlNode query) {
    super(pos, replace, ifNotExists, name, columnList, query);
    this.name = Objects.requireNonNull(name);
    this.columnList = columnList; // may be null
    this.query = query; // for "CREATE TABLE ... AS query"; may be null
  }

  @Override
  public void execute(CalcitePrepare.Context context) {
    final List<String> path = context.getDefaultSchemaPath();
    CalciteSchema schema = context.getRootSchema();
    for (String p : path) {
      schema = schema.getSubSchema(p, true);
    }

    final Pair<CalciteSchema, String> pair =
            SqlDdlNodes.schema(context, true, name);
    final JavaTypeFactory typeFactory = new JavaTypeFactoryImpl();
    final RelDataType queryRowType;
    if (query != null) {
      // A bit of a hack: pretend it's a view, to get its row type
      final String sql = query.toSqlString(CalciteSqlDialect.DEFAULT).getSql();
      final ViewTableMacro viewTableMacro =
              ViewTable.viewMacro(pair.left.plus(), sql, pair.left.path(null),
                      context.getObjectPath(), false);
      final TranslatableTable x = viewTableMacro.apply(ImmutableList.of());
      queryRowType = x.getRowType(typeFactory);

      if (columnList != null
              && queryRowType.getFieldCount() != columnList.size()) {
        throw SqlUtil.newContextException(columnList.getParserPosition(),
                RESOURCE.columnCountMismatch());
      }
    } else {
      queryRowType = null;
    }
    final List<SqlNode> columnList;
    if (this.columnList != null) {
      columnList = this.columnList.getList();
    } else {
      if (queryRowType == null) {
        // "CREATE TABLE t" is invalid; because there is no "AS query" we need
        // a list of column names and types, "CREATE TABLE t (INT c)".
        throw SqlUtil.newContextException(name.getParserPosition(),
                RESOURCE.createTableRequiresColumnList());
      }
      columnList = new ArrayList<>();
      for (String name : queryRowType.getFieldNames()) {
        columnList.add(new SqlIdentifier(name, SqlParserPos.ZERO));
      }
    }
    final ImmutableList.Builder<ColumnDef> b = ImmutableList.builder();
    final RelDataTypeFactory.Builder builder = typeFactory.builder();
    final RelDataTypeFactory.Builder storedBuilder = typeFactory.builder();
    final List<String> keyFields = new ArrayList<>();
    for (Ord<SqlNode> c : Ord.zip(columnList)) {
      if (c.e instanceof SqlColumnDeclaration) {
        final SqlColumnDeclaration d = (SqlColumnDeclaration) c.e;
        RelDataType type = d.dataType.deriveType(typeFactory, true);
        final Pair<CalciteSchema, String> pairForType =
                SqlDdlNodes.schema(context, true, d.dataType.getTypeName());
        if (type == null) {
          CalciteSchema.TypeEntry typeEntry = pairForType.left.getType(pairForType.right, false);
          if (typeEntry != null) {
            type = typeEntry.getType().apply(typeFactory);
            if (d.dataType.getNullable() != null
                    && d.dataType.getNullable() != type.isNullable()) {
              type = typeFactory.createTypeWithNullability(type, d.dataType.getNullable());
            }
          }
        }
        builder.add(d.name.getSimple(), type);
        if (d.strategy != ColumnStrategy.VIRTUAL) {
          storedBuilder.add(d.name.getSimple(), type);
        }
        b.add(ColumnDef.of(d.expression, type, d.strategy));
      } else if (c.e instanceof SqlIdentifier) {
        final SqlIdentifier id = (SqlIdentifier) c.e;
        if (queryRowType == null) {
          throw SqlUtil.newContextException(id.getParserPosition(),
                  RESOURCE.createTableRequiresColumnTypes(id.getSimple()));
        }
        final RelDataTypeField f = queryRowType.getFieldList().get(c.i);
        final ColumnStrategy strategy = f.getType().isNullable()
                ? ColumnStrategy.NULLABLE
                : ColumnStrategy.NOT_NULLABLE;
        b.add(ColumnDef.of(c.e, f.getType(), strategy));
        builder.add(id.getSimple(), f.getType());
        storedBuilder.add(id.getSimple(), f.getType());
      } else if (c.e instanceof SqlKeyConstraint) {
        final SqlKeyConstraint keyConstraint = (SqlKeyConstraint) c.e;
        if (keyConstraint.getOperator() == SqlKeyConstraint.PRIMARY) {
          List<SqlNode> operands = keyConstraint.getOperandList();
          SqlNodeList sqlNodeList = (SqlNodeList) operands.get(1);
          List<SqlNode> keyNodes = sqlNodeList.getList();
          for (SqlNode keyNode : keyNodes) {
            keyFields.add(((SqlIdentifier) keyNode).getSimple());
          }
        }
      } else {
        throw new AssertionError(c.e.getClass());
      }
    }
    final RelDataType rowType = builder.build();
    final RelDataType storedRowType = storedBuilder.build();
    final List<ColumnDef> columns = b.build();
    final InitializerExpressionFactory ief =
            new NullInitializerExpressionFactory() {
              @Override public ColumnStrategy generationStrategy(RelOptTable table,
                                                                 int iColumn) {
                return columns.get(iColumn).strategy;
              }

              @Override public RexNode newColumnDefaultValue(RelOptTable table,
                                                             int iColumn, InitializerContext context) {
                final ColumnDef c = columns.get(iColumn);
                if (c.expr != null) {
                  return context.convertExpression(c.expr);
                }
                return super.newColumnDefaultValue(table, iColumn, context);
              }
            };
    if (pair.left.plus().getTable(pair.right) != null) {
      // Table exists.
      if (!ifNotExists) {
        // They did not specify IF NOT EXISTS, so give error.
        throw SqlUtil.newContextException(name.getParserPosition(),
                RESOURCE.tableExists(pair.right));
      }
      return;
    }

    // Table does not exist. Create it.
    SortedTableSchema schemaPlus = schema.plus().unwrap(SortedTableSchema.class);
    SortedTable.Kind kind = schemaPlus.getKind();
    SortedTable.Flavor flavor = schemaPlus.getFlavor();
    schemaPlus.add(name.getSimple(), SortedTableSchema.createTable(
            ImmutableMap.of("kind", kind.name(), "flavor", flavor.name()), rowType, keyFields));
    /*
    pair.left.add(pair.right,
            new MutableArrayTable(pair.right,
                    RelDataTypeImpl.proto(storedRowType),
                    RelDataTypeImpl.proto(rowType), ief));
    */
    if (query != null) {
      SqlDdlNodes.populate(name, query, context);
    }
  }

  /** Column definition. */
  private static class ColumnDef {
    final SqlNode expr;
    final RelDataType type;
    final ColumnStrategy strategy;

    private ColumnDef(SqlNode expr, RelDataType type,
                      ColumnStrategy strategy) {
      this.expr = expr;
      this.type = type;
      this.strategy = Objects.requireNonNull(strategy);
      Preconditions.checkArgument(
              strategy == ColumnStrategy.NULLABLE
                      || strategy == ColumnStrategy.NOT_NULLABLE
                      || expr != null);
    }

    static ColumnDef of(SqlNode expr, RelDataType type,
                        ColumnStrategy strategy) {
      return new ColumnDef(expr, type, strategy);
    }
  }
}

// End SqlCreateTable.java
