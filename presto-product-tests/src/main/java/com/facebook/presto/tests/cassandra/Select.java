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
package com.facebook.presto.tests.cassandra;

import com.datastax.driver.core.utils.Bytes;
import com.teradata.tempto.ProductTest;
import com.teradata.tempto.Requirement;
import com.teradata.tempto.RequirementsProvider;
import com.teradata.tempto.configuration.Configuration;
import com.teradata.tempto.query.QueryResult;
import org.testng.annotations.Test;

import java.sql.SQLException;
import java.sql.Timestamp;

import static com.facebook.presto.tests.TestGroups.CASSANDRA;
import static com.facebook.presto.tests.TpchTableResults.PRESTO_NATION_RESULT;
import static com.facebook.presto.tests.cassandra.CassandraTpchTableDefinitions.CASSANDRA_NATION;
import static com.facebook.presto.tests.cassandra.CassandraTpchTableDefinitions.CASSANDRA_SUPPLIER;
import static com.facebook.presto.tests.cassandra.DataTypesTableDefinition.CASSANDRA_ALL_TYPES;
import static com.facebook.presto.tests.cassandra.TestConstants.CONNECTOR_NAME;
import static com.facebook.presto.tests.cassandra.TestConstants.KEY_SPACE;
import static com.facebook.presto.tests.utils.QueryExecutors.onPresto;
import static com.teradata.tempto.Requirements.compose;
import static com.teradata.tempto.assertions.QueryAssert.Row.row;
import static com.teradata.tempto.assertions.QueryAssert.assertThat;
import static com.teradata.tempto.fulfillment.table.TableRequirements.immutableTable;
import static com.teradata.tempto.query.QueryExecutor.query;
import static java.lang.String.format;
import static java.sql.JDBCType.BIGINT;
import static java.sql.JDBCType.BOOLEAN;
import static java.sql.JDBCType.DOUBLE;
import static java.sql.JDBCType.INTEGER;
import static java.sql.JDBCType.LONGNVARCHAR;
import static java.sql.JDBCType.LONGVARBINARY;
import static java.sql.JDBCType.REAL;
import static java.sql.JDBCType.TIMESTAMP;

public class Select
        extends ProductTest
        implements RequirementsProvider
{
    @Override
    public Requirement getRequirements(Configuration configuration)
    {
        return compose(
                immutableTable(CASSANDRA_NATION),
                immutableTable(CASSANDRA_SUPPLIER),
                immutableTable(CASSANDRA_ALL_TYPES));
    }

    @Test(groups = CASSANDRA)
    public void testSelectNation()
            throws SQLException
    {
        String sql = format(
                "SELECT n_nationkey, n_name, n_regionkey, n_comment FROM %s.%s.%s",
                CONNECTOR_NAME,
                KEY_SPACE,
                CASSANDRA_NATION.getName());
        QueryResult queryResult = onPresto()
                .executeQuery(sql);

        assertThat(queryResult).matches(PRESTO_NATION_RESULT);
    }

    @Test(groups = CASSANDRA)
    public void testSelectWithEqualityFilterOnPartitioningKey()
            throws SQLException
    {
        String sql = format(
                "SELECT n_nationkey FROM %s.%s.%s WHERE n_nationkey = 0",
                CONNECTOR_NAME,
                KEY_SPACE,
                CASSANDRA_NATION.getName());
        QueryResult queryResult = onPresto()
                .executeQuery(sql);

        assertThat(queryResult).containsOnly(row(0));
    }

    @Test(groups = CASSANDRA)
    public void testSelectWithFilterOnPartitioningKey()
            throws SQLException
    {
        String sql = format(
                "SELECT n_nationkey FROM %s.%s.%s WHERE n_nationkey > 23",
                CONNECTOR_NAME,
                KEY_SPACE,
                CASSANDRA_NATION.getName());
        QueryResult queryResult = onPresto()
                .executeQuery(sql);

        assertThat(queryResult).containsOnly(row(24));
    }

    @Test(groups = CASSANDRA)
    public void testSelectWithEqualityFilterOnNonPartitioningKey()
            throws SQLException
    {
        String sql = format(
                "SELECT n_name FROM %s.%s.%s WHERE n_name = 'UNITED STATES'",
                CONNECTOR_NAME,
                KEY_SPACE,
                CASSANDRA_NATION.getName());
        QueryResult queryResult = onPresto()
                .executeQuery(sql);

        assertThat(queryResult).containsOnly(row("UNITED STATES"));
    }

    @Test(groups = CASSANDRA)
    public void testSelectWithNonEqualityFilterOnNonPartitioningKey()
            throws SQLException
    {
        String sql = format(
                "SELECT n_name FROM %s.%s.%s WHERE n_name < 'B'",
                CONNECTOR_NAME,
                KEY_SPACE,
                CASSANDRA_NATION.getName());
        QueryResult queryResult = onPresto()
                .executeQuery(sql);

        assertThat(queryResult).containsOnly(row("ALGERIA"), row("ARGENTINA"));
    }

    @Test(groups = CASSANDRA)
    public void testSelectWithMorePartitioningKeysThanLimit()
            throws SQLException
    {
        String sql = format(
                "SELECT s_suppkey FROM %s.%s.%s WHERE s_suppkey = 10",
                CONNECTOR_NAME,
                KEY_SPACE,
                CASSANDRA_SUPPLIER.getName());
        QueryResult queryResult = onPresto()
                .executeQuery(sql);

        assertThat(queryResult).containsOnly(row(10));
    }

    @Test(groups = CASSANDRA)
    public void testSelectWithMorePartitioningKeysThanLimitNonPK()
            throws SQLException
    {
        String sql = format(
                "SELECT s_suppkey FROM %s.%s.%s WHERE s_name = 'Supplier#000000010'",
                CONNECTOR_NAME,
                KEY_SPACE,
                CASSANDRA_SUPPLIER.getName());
        QueryResult queryResult = onPresto()
                .executeQuery(sql);

        assertThat(queryResult).containsOnly(row(10));
    }

    @Test(groups = CASSANDRA)
    public void testAllDatatypes()
            throws SQLException
    {
        // NOTE: DECIMAL is treated like DOUBLE
        QueryResult query = query(format(
                "SELECT a, b, bl, bo, d, do, f, fr, i, integer, l, m, s, t, ti, tu, u, v, vari FROM %s.%s.%s",
                CONNECTOR_NAME, KEY_SPACE, CASSANDRA_ALL_TYPES.getName()));

        assertThat(query)
                .hasColumns(LONGNVARCHAR, BIGINT, LONGVARBINARY, BOOLEAN, DOUBLE, DOUBLE, REAL, LONGNVARCHAR, LONGNVARCHAR,
                        INTEGER, LONGNVARCHAR, LONGNVARCHAR, LONGNVARCHAR, LONGNVARCHAR, TIMESTAMP, LONGNVARCHAR, LONGNVARCHAR,
                        LONGNVARCHAR, LONGNVARCHAR)
                .containsOnly(
                        row("\0", Long.MIN_VALUE, Bytes.fromHexString("0x00").array(), false, 0f, Double.MIN_VALUE,
                                Float.MIN_VALUE, "[0]", "0.0.0.0", Integer.MIN_VALUE, "[0]", "{\"\\u0000\":-2147483648,\"a\":0}",
                                "[0]", "\0", Timestamp.valueOf("1970-01-01 00:00:00.0"),
                                "d2177dd0-eaa2-11de-a572-001b779c76e3", "01234567-0123-0123-0123-0123456789ab",
                                "\0", String.valueOf(Long.MIN_VALUE)),
                        row("the quick brown fox jumped over the lazy dog", 9223372036854775807L, "01234".getBytes(),
                                true, new Double("99999999999999999999999999999999999999"), Double.MAX_VALUE,
                                Float.MAX_VALUE, "[4,5,6,7]", "255.255.255.255", Integer.MAX_VALUE, "[4,5,6]",
                                "{\"a\":1,\"b\":2}", "[4,5,6]", "this is a text value", Timestamp.valueOf("9999-12-31 23:59:59"),
                                "d2177dd0-eaa2-11de-a572-001b779c76e3", "01234567-0123-0123-0123-0123456789ab",
                                "abc", String.valueOf(Long.MAX_VALUE)),
                        row("def", null, null, null, null, null, null, null, null, null, null, null,
                                null, null, null, null, null, null, null));
    }

    @Test(groups = CASSANDRA)
    public void testNationJoinNation()
            throws SQLException
    {
        String tableName = format("%s.%s.%s", CONNECTOR_NAME, KEY_SPACE, CASSANDRA_NATION.getName());
        String sql = format(
                "SELECT n1.n_name, n2.n_regionkey FROM %s n1 JOIN " +
                        "%s n2 ON n1.n_nationkey = n2.n_regionkey " +
                        "WHERE n1.n_nationkey=3",
                tableName,
                tableName);
        QueryResult queryResult = onPresto()
                .executeQuery(sql);

        assertThat(queryResult).containsOnly(
                row("CANADA", 3),
                row("CANADA", 3),
                row("CANADA", 3),
                row("CANADA", 3),
                row("CANADA", 3));
    }

    @Test(groups = CASSANDRA)
    public void testNationJoinRegion()
            throws SQLException
    {
        String sql = format(
                "SELECT c.n_name, t.name FROM %s.%s.%s c JOIN " +
                        "tpch.tiny.region t ON c.n_regionkey = t.regionkey " +
                        "WHERE c.n_nationkey=3",
                CONNECTOR_NAME,
                KEY_SPACE,
                CASSANDRA_NATION.getName());
        QueryResult queryResult = onPresto()
                .executeQuery(sql);

        assertThat(queryResult).containsOnly(row("CANADA", "AMERICA"));
    }
}
