/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.tests.sqlserver;

import com.teradata.tempto.AfterTestWithContext;
import com.teradata.tempto.BeforeTestWithContext;
import com.teradata.tempto.ProductTest;
import com.teradata.tempto.Requirement;
import com.teradata.tempto.RequirementsProvider;
import com.teradata.tempto.configuration.Configuration;
import com.teradata.tempto.query.QueryResult;
import io.airlift.log.Logger;
import org.testng.annotations.Test;

import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;

import static com.facebook.presto.tests.TestGroups.PROFILE_SPECIFIC_TESTS;
import static com.facebook.presto.tests.TestGroups.SQL_SERVER;
import static com.facebook.presto.tests.TpchTableResults.PRESTO_NATION_RESULT;
import static com.facebook.presto.tests.sqlserver.SqlServerDataTypesTableDefinition.SQLSERVER_ALL_TYPES;
import static com.facebook.presto.tests.sqlserver.SqlServerTpchTableDefinitions.NATION;
import static com.facebook.presto.tests.sqlserver.TestConstants.CONNECTOR_NAME;
import static com.facebook.presto.tests.sqlserver.TestConstants.KEY_SPACE;
import static com.facebook.presto.tests.utils.QueryExecutors.onPresto;
import static com.facebook.presto.tests.utils.QueryExecutors.onSqlServer;
import static com.teradata.tempto.Requirements.compose;
import static com.teradata.tempto.assertions.QueryAssert.Row.row;
import static com.teradata.tempto.assertions.QueryAssert.assertThat;
import static com.teradata.tempto.fulfillment.table.TableRequirements.immutableTable;
import static java.lang.String.format;
import static java.sql.JDBCType.BIGINT;
import static java.sql.JDBCType.CHAR;
import static java.sql.JDBCType.DATE;
import static java.sql.JDBCType.DOUBLE;
import static java.sql.JDBCType.INTEGER;
import static java.sql.JDBCType.LONGNVARCHAR;
import static java.sql.JDBCType.REAL;
import static java.sql.JDBCType.SMALLINT;
import static java.sql.JDBCType.TIMESTAMP;
import static java.sql.JDBCType.TINYINT;

public class Select
        extends ProductTest
        implements RequirementsProvider
{
    @Override
    public Requirement getRequirements(Configuration configuration)
    {
        return compose(
                immutableTable(NATION),
                immutableTable(SQLSERVER_ALL_TYPES));
    }

    private static final String CTAS_TABLE_NAME = "create_table_as_select";
    private static final String NATION_TABLE_NAME = format("%s.%s.%s", CONNECTOR_NAME, KEY_SPACE, NATION.getName());
    private static final String CREATE_TABLE_AS_SELECT = format("%s.%s.%s", CONNECTOR_NAME, KEY_SPACE, CTAS_TABLE_NAME);
    private static final String ALL_TYPES_TABLE_NAME = format("%s.%s.%s", CONNECTOR_NAME, KEY_SPACE, SQLSERVER_ALL_TYPES.getName());

    @BeforeTestWithContext
    @AfterTestWithContext
    public void dropTestTables()
    {
        try {
            onPresto().executeQuery(format("DROP TABLE IF EXISTS %s", CREATE_TABLE_AS_SELECT));
        }
        catch (Exception e) {
            Logger.get(getClass()).warn(e, "failed to drop table");
        }
    }

    @Test(groups = {SQL_SERVER, PROFILE_SPECIFIC_TESTS})
    public void testSelectNation()
            throws SQLException
    {
        String sql = format(
                "SELECT n_nationkey, n_name, n_regionkey, n_comment FROM %s",
                NATION_TABLE_NAME);
        QueryResult queryResult = onPresto()
                .executeQuery(sql);

        assertThat(queryResult).matches(PRESTO_NATION_RESULT);
    }

    @Test(groups = {SQL_SERVER, PROFILE_SPECIFIC_TESTS})
    public void testNationSelfInnerJoin()
            throws SQLException
    {
        String sql = format(
                "SELECT n1.n_name, n2.n_regionkey FROM %s n1 JOIN " +
                        "%s n2 ON n1.n_nationkey = n2.n_regionkey " +
                        "WHERE n1.n_nationkey=3",
                NATION_TABLE_NAME,
                NATION_TABLE_NAME);
        QueryResult queryResult = onPresto()
                .executeQuery(sql);

        assertThat(queryResult).containsOnly(
                row("CANADA", 3),
                row("CANADA", 3),
                row("CANADA", 3),
                row("CANADA", 3),
                row("CANADA", 3));
    }

    @Test(groups = {SQL_SERVER, PROFILE_SPECIFIC_TESTS})
    public void testNationJoinRegion()
            throws SQLException
    {
        String sql = format(
                "SELECT c.n_name, t.name FROM %s c JOIN " +
                        "tpch.tiny.region t ON c.n_regionkey = t.regionkey " +
                        "WHERE c.n_nationkey=3",
                NATION_TABLE_NAME);
        QueryResult queryResult = onPresto()
                .executeQuery(sql);

        assertThat(queryResult).containsOnly(row("CANADA", "AMERICA"));
    }

    @Test(groups = {SQL_SERVER, PROFILE_SPECIFIC_TESTS})
    public void testAllDatatypes()
            throws SQLException
    {
        String sql = format(
                "SELECT bi, si, i, ti, f, r, c, vc, te, nc, nvc, nt, d, dt, dt2, sdt, pf30, pf22 " +
                        "FROM %s", ALL_TYPES_TABLE_NAME);
        QueryResult queryResult = onPresto()
                .executeQuery(sql);

        assertThat(queryResult)
                .hasColumns(BIGINT, SMALLINT, INTEGER, TINYINT, DOUBLE, REAL, CHAR, LONGNVARCHAR, LONGNVARCHAR,
                        CHAR, LONGNVARCHAR, LONGNVARCHAR, DATE, TIMESTAMP, TIMESTAMP, TIMESTAMP, DOUBLE, REAL)
                .containsOnly(
                        row(Long.MIN_VALUE, Short.MIN_VALUE, Integer.MIN_VALUE, Byte.MIN_VALUE, Double.MIN_VALUE,
                                Float.valueOf("-3.40E+38"), "\0   ", "\0", "\0", "\0    ", "\0", "\0",
                                Date.valueOf("0001-01-02"), Timestamp.valueOf("1753-01-01 00:00:00.000"),
                                Timestamp.valueOf("0001-01-01 00:00:00.000"), Timestamp.valueOf("1900-01-01 00:00:00"),
                                Double.MIN_VALUE, Float.valueOf("-3.40E+38")
                        ),
                        row(Long.MAX_VALUE, Short.MAX_VALUE, Integer.MAX_VALUE, Byte.MAX_VALUE, Double.MAX_VALUE,
                                Float.MAX_VALUE, "abcd", "abcdef", "abcd", "abcde", "abcdefg", "abcd",
                                Date.valueOf("9999-12-31"), Timestamp.valueOf("9999-12-31 23:59:59.997"),
                                Timestamp.valueOf("9999-12-31 23:59:59.999"), Timestamp.valueOf("2079-06-06 00:00:00"),
                                Double.valueOf("12345678912.3456756"), Float.valueOf("12345678.6557")
                        ),
                        row(null, null, null, null, null, null, null, null, null, null, null, null, null, null)
                );
    }

    @Test(groups = {SQL_SERVER, PROFILE_SPECIFIC_TESTS})
    public void testCreateTableAsSelect()
            throws SQLException
    {
        String sql = format(
                "CREATE TABLE %s AS SELECT * FROM %s", CREATE_TABLE_AS_SELECT, NATION_TABLE_NAME);
        onPresto().executeQuery(sql);

        sql = format(
                "SELECT n_nationkey, n_name, n_regionkey, n_comment FROM %s.%s.%s",
                "master", KEY_SPACE, CTAS_TABLE_NAME);
        QueryResult queryResult = onSqlServer()
                .executeQuery(sql);

        assertThat(queryResult).matches(PRESTO_NATION_RESULT);
    }
}
