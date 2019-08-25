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
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.sql.SqlAlter;
import org.apache.calcite.sql.SqlExecutableStatement;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql2rel.InitializerContext;
import org.apache.calcite.sql2rel.InitializerExpressionFactory;
import org.apache.calcite.sql2rel.NullInitializerExpressionFactory;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * Simple test example of a CREATE TABLE statement.
 */
public class SqlAlterTable extends SqlAlter
  implements SqlExecutableStatement {
  public static final SqlOperator OPERATOR =
          new SqlSpecialOperator("ALTER TABLE", SqlKind.OTHER_DDL);

  private final boolean ifExists;
  private final SqlIdentifier name;
  private final List<Action> actions;
  private final SqlNodeList columnList;

  /** Creates a SqlCreateTable. */
  public SqlAlterTable(SqlParserPos pos, boolean ifExists,
                       SqlIdentifier name, List<Action> actions, SqlNodeList columnList) {
    super(pos, "TABLE");
    this.ifExists = ifExists;
    this.name = Objects.requireNonNull(name);
    this.actions = actions;
    this.columnList = columnList;
  }

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableList.of(name, columnList);
  }

  @Override protected void unparseAlterOperation(SqlWriter writer, int leftPrec, int rightPrec) {
    if (ifExists) {
      writer.keyword("IF NOT EXISTS");
    }
    name.unparse(writer, leftPrec, rightPrec);
    if (columnList != null) {
      SqlWriter.Frame frame = writer.startList("", "");
      for (int i = 0; i < columnList.size(); i++) {
        Action a = actions.get(i);
        SqlNode c = columnList.get(i);
        writer.sep(",");
        writer.keyword(a.name());
        c.unparse(writer, 0, 0);
      }
      writer.endList(frame);
    }
  }

  @Override public void execute(CalcitePrepare.Context context) {
    final List<String> path = context.getDefaultSchemaPath();
    CalciteSchema schema = context.getRootSchema();
    for (String p : path) {
      schema = schema.getSubSchema(p, true);
    }

    final Pair<CalciteSchema, String> pair =
            SqlDdlNodes.schema(context, true, name);
    final JavaTypeFactory typeFactory = new JavaTypeFactoryImpl();
    final List<SqlNode> columnList = this.columnList.getList();
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
    SortedTableSchema schemaPlus = schema.plus().unwrap(SortedTableSchema.class);
    SortedTable.Kind kind = schemaPlus.getKind();
    SortedTable.Flavor flavor = schemaPlus.getFlavor();
    if (schemaPlus.getTable(pair.right) == null) {
      // Table does not exists.
      if (!ifExists) {
        // They did not specify IF EXISTS, so give error.
        throw SqlUtil.newContextException(name.getParserPosition(),
                RESOURCE.tableNameNotFound(pair.right));
      }
      return;
    }
    // Table does not exist. Create it.
    schemaPlus.add(name.getSimple(), SortedTableSchema.createTable(
            ImmutableMap.of("kind", kind.name(), "flavor", flavor.name()), rowType, keyFields));
    /*
    pair.left.add(pair.right,
            new MutableArrayTable(pair.right,
                    RelDataTypeImpl.proto(storedRowType),
                    RelDataTypeImpl.proto(rowType), ief));
    */
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

  public enum Action {
    ADD, ALTER, DROP
  }
}

// End SqlCreateTable.java
