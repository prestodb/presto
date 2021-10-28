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

import io.airlift.units.Duration;
import io.prestodb.tempto.ProductTest;
import io.prestodb.tempto.Requirement;
import io.prestodb.tempto.RequirementsProvider;
import io.prestodb.tempto.configuration.Configuration;
import io.prestodb.tempto.internal.fulfillment.table.TableName;
import io.prestodb.tempto.internal.query.CassandraQueryExecutor;
import io.prestodb.tempto.query.QueryResult;
import org.testng.annotations.Test;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDateTime;

import static com.facebook.presto.tests.TestGroups.CASSANDRA;
import static com.facebook.presto.tests.cassandra.DataTypesTableDefinition.CASSANDRA_ALL_TYPES;
import static com.facebook.presto.tests.cassandra.TestConstants.CONNECTOR_NAME;
import static com.facebook.presto.tests.cassandra.TestConstants.KEY_SPACE;
import static com.facebook.presto.tests.utils.QueryAssertions.assertContainsEventually;
import static io.prestodb.tempto.assertions.QueryAssert.Row.row;
import static io.prestodb.tempto.assertions.QueryAssert.assertThat;
import static io.prestodb.tempto.fulfillment.table.MutableTableRequirement.State.CREATED;
import static io.prestodb.tempto.fulfillment.table.MutableTablesState.mutableTablesState;
import static io.prestodb.tempto.fulfillment.table.TableRequirements.mutableTable;
import static io.prestodb.tempto.query.QueryExecutor.query;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MINUTES;

public class TestInsertIntoCassandraTable
        extends ProductTest
        implements RequirementsProvider
{
    private static final String CASSANDRA_INSERT_TABLE = "Insert_All_Types";
    private static final String CASSANDRA_MATERIALIZED_VIEW = "Insert_All_Types_Mview";

    private Configuration configuration;

    @Override
    public Requirement getRequirements(Configuration configuration)
    {
        this.configuration = configuration;
        return mutableTable(CASSANDRA_ALL_TYPES, CASSANDRA_INSERT_TABLE, CREATED);
    }

    @Test(groups = CASSANDRA)
    public void testInsertIntoValuesToCassandraTableAllSimpleTypes()
    {
        TableName table = mutableTablesState().get(CASSANDRA_INSERT_TABLE).getTableName();
        String tableNameInDatabase = String.format("%s.%s", CONNECTOR_NAME, table.getNameInDatabase());

        assertContainsEventually(() -> query(format("SHOW TABLES FROM %s.%s", CONNECTOR_NAME, KEY_SPACE)),
                query(format("SELECT '%s'", table.getSchemalessNameInDatabase())),
                new Duration(1, MINUTES));

        QueryResult queryResult = query("SELECT * FROM " + tableNameInDatabase);
        assertThat(queryResult).hasNoRows();

        // TODO Following types are not supported now. We need to change null into the value after fixing it
        // blob, frozen<set<type>>, inet, list<type>, map<type,type>, set<type>, timeuuid, decimal, uuid, varint
        query("INSERT INTO " + tableNameInDatabase +
                "(a, b, bl, bo, d, do, dt, f, fr, i, ti, si, integer, l, m, s, t, ts, tu, u, v, vari) VALUES (" +
                "'ascii value', " +
                "BIGINT '99999', " +
                "null, " +
                "true, " +
                "null, " +
                "123.456789, " +
                "DATE '9999-12-31'," +
                "REAL '123.45678', " +
                "null, " +
                "null, " +
                "TINYINT '-128', " +
                "SMALLINT '-32768', " +
                "123, " +
                "null, " +
                "null, " +
                "null, " +
                "'text value', " +
                "timestamp '9999-12-31 23:59:59'," +
                "null, " +
                "null, " +
                "'varchar value'," +
                "null)");

        assertThat(query("SELECT * FROM " + tableNameInDatabase)).containsOnly(
                row(
                        "ascii value",
                        99999,
                        null,
                        true,
                        null,
                        123.456789,
                        Date.valueOf("9999-12-31"),
                        123.45678,
                        null,
                        null,
                        123,
                        null,
                        null,
                        null,
                        -32768,
                        "text value",
                        -128,
                        Timestamp.valueOf(LocalDateTime.of(9999, 12, 31, 23, 59, 59)),
                        null,
                        null,
                        "varchar value",
                        null));

        // insert null for all datatypes
        query("INSERT INTO " + tableNameInDatabase +
                "(a, b, bl, bo, d, do, dt, f, fr, i, ti, si, integer, l, m, s, t, ts, tu, u, v, vari) VALUES (" +
                "'key 1', null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null) ");
        assertThat(query(format("SELECT * FROM %s WHERE a = 'key 1'", tableNameInDatabase))).containsOnly(
                row("key 1", null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null));

        // insert into only a subset of columns
        query(format("INSERT INTO %s (a, bo, integer, t) VALUES ('key 2', false, 999, 'text 2')", tableNameInDatabase));
        assertThat(query(format("SELECT * FROM %s WHERE a = 'key 2'", tableNameInDatabase))).containsOnly(
                row("key 2", null, null, false, null, null, null, null, null, null, 999, null, null, null, null, "text 2", null, null, null, null, null, null));

        // negative test: failed to insert null to primary key
        assertThat(() -> query(format("INSERT INTO %s (a) VALUES (null) ", tableNameInDatabase)))
                .failsWithMessage("Invalid null value in condition for column a");
    }

    @Test(groups = CASSANDRA)
    public void testInsertIntoValuesToCassandraMaterizedView()
            throws Exception
    {
        TableName table = mutableTablesState().get(CASSANDRA_INSERT_TABLE).getTableName();
        onCasssandra(format("DROP MATERIALIZED VIEW IF EXISTS %s.%s", KEY_SPACE, CASSANDRA_MATERIALIZED_VIEW));
        onCasssandra(format("CREATE MATERIALIZED VIEW %s.%s AS " +
                        "SELECT * FROM %s " +
                        "WHERE b IS NOT NULL " +
                        "PRIMARY KEY (a, b) " +
                        "WITH CLUSTERING ORDER BY (integer DESC)",
                KEY_SPACE,
                CASSANDRA_MATERIALIZED_VIEW,
                table.getNameInDatabase()));

        assertContainsEventually(() -> query(format("SHOW TABLES FROM %s.%s", CONNECTOR_NAME, KEY_SPACE)),
                query(format("SELECT lower('%s')", CASSANDRA_MATERIALIZED_VIEW)),
                new Duration(1, MINUTES));

        assertThat(() -> query(format("INSERT INTO %s.%s.%s (a) VALUES (null) ", CONNECTOR_NAME, KEY_SPACE, CASSANDRA_MATERIALIZED_VIEW)))
                .failsWithMessage("Inserting into materialized views not yet supported");

        assertThat(() -> query(format("DROP TABLE %s.%s.%s", CONNECTOR_NAME, KEY_SPACE, CASSANDRA_MATERIALIZED_VIEW)))
                .failsWithMessage("Dropping materialized views not yet supported");

        onCasssandra(format("DROP MATERIALIZED VIEW IF EXISTS %s.%s", KEY_SPACE, CASSANDRA_MATERIALIZED_VIEW));
    }

    private void onCasssandra(String query)
    {
        CassandraQueryExecutor queryExecutor = new CassandraQueryExecutor(configuration);
        queryExecutor.executeQuery(query);
        queryExecutor.close();
    }
}
