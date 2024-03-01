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
import io.airlift.units.Duration;
import io.prestodb.tempto.ProductTest;
import io.prestodb.tempto.Requirement;
import io.prestodb.tempto.RequirementsProvider;
import io.prestodb.tempto.configuration.Configuration;
import io.prestodb.tempto.internal.query.CassandraQueryExecutor;
import io.prestodb.tempto.query.QueryResult;
import org.testng.annotations.Test;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDateTime;

import static com.facebook.presto.tests.TestGroups.CASSANDRA;
import static com.facebook.presto.tests.TpchTableResults.PRESTO_NATION_RESULT;
import static com.facebook.presto.tests.cassandra.CassandraTpchTableDefinitions.CASSANDRA_NATION;
import static com.facebook.presto.tests.cassandra.CassandraTpchTableDefinitions.CASSANDRA_SUPPLIER;
import static com.facebook.presto.tests.cassandra.DataTypesTableDefinition.CASSANDRA_ALL_TYPES;
import static com.facebook.presto.tests.cassandra.TestConstants.CONNECTOR_NAME;
import static com.facebook.presto.tests.cassandra.TestConstants.KEY_SPACE;
import static com.facebook.presto.tests.utils.QueryAssertions.assertContainsEventually;
import static com.facebook.presto.tests.utils.QueryExecutors.onPresto;
import static io.prestodb.tempto.Requirements.compose;
import static io.prestodb.tempto.assertions.QueryAssert.Row.row;
import static io.prestodb.tempto.assertions.QueryAssert.assertThat;
import static io.prestodb.tempto.fulfillment.table.TableRequirements.immutableTable;
import static io.prestodb.tempto.query.QueryExecutor.query;
import static java.lang.String.format;
import static java.sql.JDBCType.BIGINT;
import static java.sql.JDBCType.BOOLEAN;
import static java.sql.JDBCType.DATE;
import static java.sql.JDBCType.DOUBLE;
import static java.sql.JDBCType.INTEGER;
import static java.sql.JDBCType.REAL;
import static java.sql.JDBCType.SMALLINT;
import static java.sql.JDBCType.TIMESTAMP;
import static java.sql.JDBCType.TINYINT;
import static java.sql.JDBCType.VARBINARY;
import static java.sql.JDBCType.VARCHAR;
import static java.util.concurrent.TimeUnit.MINUTES;

public class TestSelect
        extends ProductTest
        implements RequirementsProvider
{
    private Configuration configuration;

    @Override
    public Requirement getRequirements(Configuration configuration)
    {
        this.configuration = configuration;
        return compose(
                immutableTable(CASSANDRA_NATION),
                immutableTable(CASSANDRA_SUPPLIER),
                immutableTable(CASSANDRA_ALL_TYPES));
    }

    @Test(groups = CASSANDRA)
    public void testSelectNation()
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
    public void testAllDataTypes()
    {
        // NOTE: DECIMAL is treated like DOUBLE
        QueryResult query = query(format(
                "SELECT a, b, bl, bo, d, do, dt, f, fr, i, integer, l, m, s, si, t, ti, ts, tu, u, v, vari FROM %s.%s.%s",
                CONNECTOR_NAME, KEY_SPACE, CASSANDRA_ALL_TYPES.getName()));

        assertThat(query)
                .hasColumns(VARCHAR, BIGINT, VARBINARY, BOOLEAN, DOUBLE, DOUBLE, DATE, REAL, VARCHAR, VARCHAR,
                        INTEGER, VARCHAR, VARCHAR, VARCHAR, SMALLINT, VARCHAR, TINYINT, TIMESTAMP, VARCHAR, VARCHAR,
                        VARCHAR, VARCHAR)
                .containsOnly(
                        row("\0", Long.MIN_VALUE, Bytes.fromHexString("0x00").array(), false, 0f, Double.MIN_VALUE, Date.valueOf("1970-01-02"),
                                Float.MIN_VALUE, "[0]", "0.0.0.0", Integer.MIN_VALUE, "[0]", "{\"\\u0000\":-2147483648,\"a\":0}",
                                "[0]", Short.MIN_VALUE, "\0", Byte.MIN_VALUE, Timestamp.valueOf(LocalDateTime.of(1970, 1, 1, 0, 0)),
                                "d2177dd0-eaa2-11de-a572-001b779c76e3", "01234567-0123-0123-0123-0123456789ab",
                                "\0", String.valueOf(Long.MIN_VALUE)),
                        row("the quick brown fox jumped over the lazy dog", 9223372036854775807L, "01234".getBytes(),
                                true, new Double("99999999999999999999999999999999999999"), Double.MAX_VALUE, Date.valueOf("9999-12-31"),
                                Float.MAX_VALUE, "[4,5,6,7]", "255.255.255.255", Integer.MAX_VALUE, "[4,5,6]",
                                "{\"a\":1,\"b\":2}", "[4,5,6]", Short.MAX_VALUE, "this is a text value", Byte.MAX_VALUE, Timestamp.valueOf(LocalDateTime.of(9999, 12, 31, 23, 59, 59)),
                                "d2177dd0-eaa2-11de-a572-001b779c76e3", "01234567-0123-0123-0123-0123456789ab",
                                "abc", String.valueOf(Long.MAX_VALUE)),
                        row("def", null, null, null, null, null, null, null, null, null, null, null,
                                null, null, null, null, null, null, null, null, null, null));
    }

    @Test(groups = CASSANDRA)
    public void testNationJoinNation()
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

    @Test(groups = CASSANDRA)
    public void testSelectAllTypePartitioningMaterializedView()
    {
        String materializedViewName = format("%s_partitioned_mv", CASSANDRA_ALL_TYPES.getName());
        onCassandra(format("DROP MATERIALIZED VIEW IF EXISTS %s.%s", KEY_SPACE, materializedViewName));
        onCassandra(format("CREATE MATERIALIZED VIEW %s.%s AS SELECT * FROM %s.%s WHERE b IS NOT NULL PRIMARY KEY (a, b)",
                KEY_SPACE,
                materializedViewName,
                KEY_SPACE,
                CASSANDRA_ALL_TYPES.getName()));

        assertContainsEventually(() -> query(format("SHOW TABLES FROM %s.%s", CONNECTOR_NAME, KEY_SPACE)),
                query(format("SELECT '%s'", materializedViewName)),
                new Duration(1, MINUTES));

        // Materialized view may not return all results during the creation
        assertContainsEventually(() -> query(format("SELECT status_replicated FROM %s.system.built_views WHERE view_name = '%s'", CONNECTOR_NAME, materializedViewName)),
                query("SELECT true"),
                new Duration(1, MINUTES));

        QueryResult query = query(format(
                "SELECT a, b, bl, bo, d, do, dt, f, fr, i, integer, l, m, s, si, t, ti, ts, tu, u, v, vari FROM %s.%s.%s WHERE a = '\0'",
                CONNECTOR_NAME, KEY_SPACE, materializedViewName));

        assertThat(query)
                .hasColumns(VARCHAR, BIGINT, VARBINARY, BOOLEAN, DOUBLE, DOUBLE, DATE, REAL, VARCHAR, VARCHAR,
                        INTEGER, VARCHAR, VARCHAR, VARCHAR, SMALLINT, VARCHAR, TINYINT, TIMESTAMP, VARCHAR, VARCHAR,
                        VARCHAR, VARCHAR)
                .containsOnly(
                        row("\0", Long.MIN_VALUE, Bytes.fromHexString("0x00").array(), false, 0f, Double.MIN_VALUE, Date.valueOf("1970-01-02"),
                                Float.MIN_VALUE, "[0]", "0.0.0.0", Integer.MIN_VALUE, "[0]", "{\"\\u0000\":-2147483648,\"a\":0}",
                                "[0]", Short.MIN_VALUE, "\0", Byte.MIN_VALUE, Timestamp.valueOf(LocalDateTime.of(1970, 1, 1, 0, 0)),
                                "d2177dd0-eaa2-11de-a572-001b779c76e3", "01234567-0123-0123-0123-0123456789ab",
                                "\0", String.valueOf(Long.MIN_VALUE)));

        onCassandra(format("DROP MATERIALIZED VIEW IF EXISTS %s.%s", KEY_SPACE, materializedViewName));
    }

    @Test(groups = CASSANDRA)
    public void testSelectClusteringMaterializedView()
    {
        String mvName = "clustering_mv";
        onCassandra(format("DROP MATERIALIZED VIEW IF EXISTS %s.%s", KEY_SPACE, mvName));
        onCassandra(format("CREATE MATERIALIZED VIEW %s.%s AS " +
                        "SELECT * FROM %s.%s " +
                        "WHERE s_nationkey IS NOT NULL " +
                        "PRIMARY KEY (s_nationkey, s_suppkey) " +
                        "WITH CLUSTERING ORDER BY (s_nationkey DESC)",
                KEY_SPACE,
                mvName,
                KEY_SPACE,
                CASSANDRA_SUPPLIER.getName()));

        assertContainsEventually(() -> query(format("SHOW TABLES FROM %s.%s", CONNECTOR_NAME, KEY_SPACE)),
                query(format("SELECT '%s'", mvName)),
                new Duration(1, MINUTES));

        // Materialized view may not return all results during the creation
        assertContainsEventually(() -> query(format("SELECT status_replicated FROM %s.system.built_views WHERE view_name = '%s'", CONNECTOR_NAME, mvName)),
                query("SELECT true"),
                new Duration(1, MINUTES));

        QueryResult aggregateQueryResult = onPresto()
                .executeQuery(format(
                        "SELECT MAX(s_nationkey), SUM(s_suppkey), AVG(s_acctbal) " +
                                "FROM %s.%s.%s WHERE s_suppkey BETWEEN 1 AND 10 ", CONNECTOR_NAME, KEY_SPACE, mvName));
        assertThat(aggregateQueryResult).containsOnly(
                row(24, 55, 4334.653));

        QueryResult orderedResult = onPresto()
                .executeQuery(format(
                        "SELECT s_nationkey, s_suppkey, s_acctbal " +
                                "FROM %s.%s.%s WHERE s_nationkey = 1 LIMIT 1", CONNECTOR_NAME, KEY_SPACE, mvName));
        assertThat(orderedResult).containsOnly(
                row(1, 3, 4192.4));

        onCassandra(format("DROP MATERIALIZED VIEW IF EXISTS %s.%s", KEY_SPACE, mvName));
    }

    private void onCassandra(String query)
    {
        try (CassandraQueryExecutor queryExecutor = new CassandraQueryExecutor(configuration)) {
            queryExecutor.executeQuery(query);
        }
    }
}
