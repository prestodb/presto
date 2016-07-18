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
package com.facebook.presto.tests.jdbc;

import com.facebook.presto.jdbc.PrestoConnection;
import com.facebook.presto.jdbc.PrestoDatabaseMetaData;
import com.facebook.presto.tests.ImmutableTpchTablesRequirements.ImmutableNationTable;
import com.teradata.tempto.BeforeTestWithContext;
import com.teradata.tempto.ProductTest;
import com.teradata.tempto.Requirement;
import com.teradata.tempto.RequirementsProvider;
import com.teradata.tempto.Requires;
import com.teradata.tempto.configuration.Configuration;
import com.teradata.tempto.query.QueryResult;
import org.testng.annotations.Test;

import java.sql.SQLException;
import java.sql.Statement;

import static com.facebook.presto.tests.TestGroups.JDBC;
import static com.facebook.presto.tests.TestGroups.QUARANTINE;
import static com.facebook.presto.tests.TpchTableResults.PRESTO_NATION_RESULT;
import static com.teradata.tempto.Requirements.compose;
import static com.teradata.tempto.assertions.QueryAssert.Row.row;
import static com.teradata.tempto.assertions.QueryAssert.assertThat;
import static com.teradata.tempto.fulfillment.table.MutableTableRequirement.State.CREATED;
import static com.teradata.tempto.fulfillment.table.MutableTablesState.mutableTablesState;
import static com.teradata.tempto.fulfillment.table.TableRequirements.immutableTable;
import static com.teradata.tempto.fulfillment.table.TableRequirements.mutableTable;
import static com.teradata.tempto.fulfillment.table.hive.tpch.TpchTableDefinitions.NATION;
import static com.teradata.tempto.internal.convention.SqlResultDescriptor.sqlResultDescriptorForResource;
import static com.teradata.tempto.query.QueryExecutor.defaultQueryExecutor;
import static com.teradata.tempto.query.QueryExecutor.query;
import static java.util.Locale.CHINESE;
import static org.assertj.core.api.Assertions.assertThat;

public class JdbcTests
        extends ProductTest
{
    private static final String TABLE_NAME = "nation_table_name";

    private static class ImmutableAndMutableNationTable
            implements RequirementsProvider
    {
        public Requirement getRequirements(Configuration configuration)
        {
            return compose(immutableTable(NATION), mutableTable(NATION, TABLE_NAME, CREATED));
        }
    }

    private PrestoConnection connection;

    @BeforeTestWithContext
    public void setup()
            throws SQLException
    {
        connection = (PrestoConnection) defaultQueryExecutor().getConnection();
    }

    @Test(groups = JDBC)
    @Requires(ImmutableNationTable.class)
    public void shouldExecuteQuery()
            throws SQLException
    {
        try (Statement statement = connection.createStatement()) {
            QueryResult result = queryResult(statement, "select * from hive.default.nation");
            assertThat(result).matches(PRESTO_NATION_RESULT);
        }
    }

    @Test(groups = {JDBC, QUARANTINE})
    @Requires(ImmutableAndMutableNationTable.class)
    public void shouldInsertSelectQuery()
            throws SQLException
    {
        String tableNameInDatabase = mutableTablesState().get(TABLE_NAME).getNameInDatabase();
        assertThat(query("SELECT * FROM " + tableNameInDatabase)).hasNoRows();

        try (Statement statement = connection.createStatement()) {
            // TODO: fix, should return proper number of inserted rows
            assertThat(statement.executeUpdate("insert into " + tableNameInDatabase + " select * from nation"))
                    .isEqualTo(25);
        }

        assertThat(query("SELECT * FROM " + tableNameInDatabase)).matches(PRESTO_NATION_RESULT);
    }

    @Test(groups = JDBC)
    @Requires(ImmutableNationTable.class)
    public void shouldExecuteQueryWithSelectedCatalogAndSchema()
            throws SQLException
    {
        connection.setCatalog("hive");
        connection.setSchema("default");
        try (Statement statement = connection.createStatement()) {
            QueryResult result = queryResult(statement, "select * from nation");
            assertThat(result).matches(PRESTO_NATION_RESULT);
        }
    }

    @Test(groups = JDBC)
    public void shouldSetTimezone()
            throws SQLException
    {
        String timeZoneId = "Indian/Kerguelen";
        connection.setTimeZoneId(timeZoneId);
        try (Statement statement = connection.createStatement()) {
            QueryResult result = queryResult(statement, "select current_timezone()");
            assertThat(result).contains(row(timeZoneId));
        }
    }

    @Test(groups = JDBC)
    public void shouldSetLocale()
            throws SQLException
    {
        connection.setLocale(CHINESE);
        try (Statement statement = connection.createStatement()) {
            QueryResult result = queryResult(statement, "SELECT date_format(TIMESTAMP '2001-01-09 09:04', '%M')");
            assertThat(result).contains(row("一月"));
        }
    }

    @Test(groups = JDBC)
    public void shouldGetSchemas()
            throws SQLException
    {
        QueryResult result = QueryResult.forResultSet(metaData().getSchemas("hive", null));
        assertThat(result).contains(row("default", "hive"));
    }

    @Test(groups = JDBC)
    @Requires(ImmutableNationTable.class)
    public void shouldGetTables()
            throws SQLException
    {
        QueryResult result = QueryResult.forResultSet(metaData().getTables("hive", null, null, null));
        assertThat(result).contains(row("hive", "default", "nation", "TABLE", null, null, null, null, null, null));
    }

    @Test(groups = JDBC)
    @Requires(ImmutableNationTable.class)
    public void shouldGetColumns()
            throws SQLException
    {
        QueryResult result = QueryResult.forResultSet(metaData().getColumns("hive", "default", "nation", null));
        assertThat(result).matches(sqlResultDescriptorForResource("com/facebook/presto/tests/jdbc/get_nation_columns.result"));
    }

    @Test(groups = JDBC)
    @Requires(ImmutableNationTable.class)
    public void shouldGetTableTypes()
            throws SQLException
    {
        QueryResult result = QueryResult.forResultSet(metaData().getTableTypes());
        assertThat(result).contains(row("TABLE"), row("VIEW"));
    }

    private QueryResult queryResult(Statement statement, String query)
            throws SQLException
    {
        return QueryResult.forResultSet(statement.executeQuery(query));
    }

    private PrestoDatabaseMetaData metaData()
            throws SQLException
    {
        return (PrestoDatabaseMetaData) connection.getMetaData();
    }
}
