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
package org.apache.calcite.test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Ordering;
import org.apache.calcite.adapter.table.SortedTableSchemaFactory;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.test.utils.ClusterTestHarness;
import org.apache.calcite.util.Sources;
import org.apache.calcite.util.TestUtil;
import org.apache.calcite.util.Util;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.isA;
import static org.junit.Assert.assertThat;

public class KafkaTableTest extends ClusterTestHarness {

  private static final Logger log = LoggerFactory.getLogger(KafkaTableTest.class);

  @Before
  public void setup() {
    log.debug("bootstrapservers = " + bootstrapServers);
  }

  @After
  public void teardown() {
    log.debug("Shutting down");
  }

  private void close(Connection connection, Statement statement) {
    if (statement != null) {
      try {
        statement.close();
      } catch (SQLException e) {
        // ignore
      }
    }
    if (connection != null) {
      try {
        connection.close();
      } catch (SQLException e) {
        // ignore
      }
    }
  }

  @Test public void testKafka() throws SQLException {
    try (Connection connection =
                 DriverManager.getConnection("jdbc:calcite:",
                         CalciteAssert.propBuilder()
                                 .set(CalciteConnectionProperty.MODEL, jsonPath("kafka"))
                                 .set(CalciteConnectionProperty.PARSER_FACTORY,
                                         "org.apache.calcite.sql.parser.parserextension"
                                                 + ".ExtensionSqlParserImpl#FACTORY")
                                 .build())) {

      Statement s = connection.createStatement();
      boolean b = s.execute("create table t (i int not null, constraint pk primary key (i))");
      assertThat(b, is(false));
      int x = s.executeUpdate("insert into t values 1");
      //TODO fix ret
      assertThat(x, is(1));
      x = s.executeUpdate("insert into t values 3");
      assertThat(x, is(1));

      ResultSet resultSet = s.executeQuery("select * from t");
      output(resultSet);
      resultSet.close();

      s = connection.createStatement();
      b = s.execute("create table t2 (i int not null, j int not null, k varchar, constraint pk primary key (j, i))");
      assertThat(b, is(false));
      x = s.executeUpdate("insert into t2 values (1, 2, 'hi')");
      assertThat(x, is(1));
      x = s.executeUpdate("insert into t2 values (3, 4, 'world')");
      assertThat(x, is(1));

      resultSet = s.executeQuery("select * from t2");
      output(resultSet);
      resultSet.close();

      s.close();
    }
  }

  @Ignore
  @Test public void testKafka2() throws SQLException {
    sql("kafka", "select * from t2").ok();
  }

  private Fluent sql(String model, String sql) {
    return new Fluent(model, sql, this::output);
  }

  /** Returns a function that checks the contents of a result set against an
   * expected string. */
  private static Consumer<ResultSet> expect(final String... expected) {
    return resultSet -> {
      try {
        final List<String> lines = new ArrayList<>();
        KafkaTableTest.collect(lines, resultSet);
        Assert.assertEquals(Arrays.asList(expected), lines);
      } catch (SQLException e) {
        throw TestUtil.rethrow(e);
      }
    };
  }

  /** Returns a function that checks the contents of a result set against an
   * expected string. */
  private static Consumer<ResultSet> expectUnordered(String... expected) {
    final List<String> expectedLines =
        Ordering.natural().immutableSortedCopy(Arrays.asList(expected));
    return resultSet -> {
      try {
        final List<String> lines = new ArrayList<>();
        KafkaTableTest.collect(lines, resultSet);
        Collections.sort(lines);
        Assert.assertEquals(expectedLines, lines);
      } catch (SQLException e) {
        throw TestUtil.rethrow(e);
      }
    };
  }

  private void checkSql(String sql, String model, Consumer<ResultSet> fn)
      throws SQLException {
    Connection connection = null;
    Statement statement = null;
    try {
      Properties info = new Properties();
      info.put("model", jsonPath(model));
      connection = DriverManager.getConnection("jdbc:calcite:", info);
      statement = connection.createStatement();
      final ResultSet resultSet =
          statement.executeQuery(
              sql);
      fn.accept(resultSet);
    } finally {
      close(connection, statement);
    }
  }

  private String jsonPath(String model) {
    return resourcePath(model + ".json");
  }

  private String resourcePath(String path) {
    return Sources.of(KafkaTableTest.class.getResource("/" + path)).file().getAbsolutePath();
  }

  private static void collect(List<String> result, ResultSet resultSet)
      throws SQLException {
    final StringBuilder buf = new StringBuilder();
    while (resultSet.next()) {
      buf.setLength(0);
      int n = resultSet.getMetaData().getColumnCount();
      String sep = "";
      for (int i = 1; i <= n; i++) {
        buf.append(sep)
            .append(resultSet.getMetaData().getColumnLabel(i))
            .append("=")
            .append(resultSet.getString(i));
        sep = "; ";
      }
      result.add(Util.toLinux(buf.toString()));
    }
  }

  private void output(ResultSet resultSet, PrintStream out)
      throws SQLException {
    final ResultSetMetaData metaData = resultSet.getMetaData();
    final int columnCount = metaData.getColumnCount();
    while (resultSet.next()) {
      for (int i = 1;; i++) {
        out.print(resultSet.getString(i));
        if (i < columnCount) {
          out.print(", ");
        } else {
          out.println();
          break;
        }
      }
    }
  }

  private Void output(ResultSet resultSet) {
    try {
      output(resultSet, System.out);
    } catch (SQLException e) {
      throw TestUtil.rethrow(e);
    }
    return null;
  }

  /** Fluent API to perform test actions. */
  private class Fluent {
    private final String model;
    private final String sql;
    private final Consumer<ResultSet> expect;

    Fluent(String model, String sql, Consumer<ResultSet> expect) {
      this.model = model;
      this.sql = sql;
      this.expect = expect;
    }

    /** Runs the test. */
    Fluent ok() {
      try {
        checkSql(sql, model, expect);
        return this;
      } catch (SQLException e) {
        throw TestUtil.rethrow(e);
      }
    }

    /** Assigns a function to call to test whether output is correct. */
    Fluent checking(Consumer<ResultSet> expect) {
      return new Fluent(model, sql, expect);
    }

    /** Sets the rows that are expected to be returned from the SQL query. */
    Fluent returns(String... expectedLines) {
      return checking(expect(expectedLines));
    }

    /** Sets the rows that are expected to be returned from the SQL query,
     * in no particular order. */
    Fluent returnsUnordered(String... expectedLines) {
      return checking(expectUnordered(expectedLines));
    }
  }
}
