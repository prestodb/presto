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

package com.facebook.presto.tests.hive;

import com.teradata.tempto.ProductTest;
import com.teradata.tempto.query.QueryExecutor;
import com.teradata.tempto.query.QueryResult;
import org.testng.annotations.Test;

import static com.facebook.presto.tests.TestGroups.AUTHORIZATION;
import static com.facebook.presto.tests.TestGroups.HIVE_CONNECTOR;
import static com.facebook.presto.tests.TestGroups.PROFILE_SPECIFIC_TESTS;
import static com.facebook.presto.tests.utils.QueryExecutors.connectToPresto;
import static com.facebook.presto.tests.utils.Tables.uniqueTableName;
import static com.teradata.tempto.assertions.QueryAssert.assertThat;
import static com.teradata.tempto.context.ContextDsl.executeWith;
import static com.teradata.tempto.sql.SqlContexts.createViewAs;
import static java.lang.String.format;

public class TestSqlStandardAccessControlChecks
        extends ProductTest
{
    public void setup(String tableName)
    {
        queryAsAlice("DROP TABLE IF EXISTS %s", tableName);
        queryAsAlice("CREATE TABLE %s(month bigint, day bigint)", tableName);
    }

    @Test(groups = {AUTHORIZATION, HIVE_CONNECTOR, PROFILE_SPECIFIC_TESTS})
    public void testAccessControlSelect()
    {
        String tableName = uniqueTableName();
        setup(tableName);

        String format = "SELECT * FROM %s";
        assertThat(() -> queryAsBob(tableName, format))
                .failsWithMessage(format("Access Denied: Cannot select from table default.%s", tableName));

        queryAsAlice("GRANT SELECT ON %s TO bob", tableName);
        assertThat(queryAsBob(tableName, format)).hasNoRows();
    }

    @Test(groups = {AUTHORIZATION, HIVE_CONNECTOR, PROFILE_SPECIFIC_TESTS})
    public void testAccessControlInsert()
    {
        String tableName = uniqueTableName();
        setup(tableName);

        assertThat(() -> connectAsBob().executeQuery(format("INSERT INTO %s VALUES (3, 22)", tableName)))
                .failsWithMessage(format("Access Denied: Cannot insert into table default.%s", tableName));

        queryAsAlice("GRANT INSERT ON %s TO bob", tableName);
        assertThat(connectAsBob().executeQuery(format("INSERT INTO %s VALUES (3, 22)", tableName))).hasRowsCount(1);
        assertThat(queryAsAlice("SELECT * FROM %s", tableName)).hasRowsCount(1);
    }

    @Test(groups = {AUTHORIZATION, HIVE_CONNECTOR, PROFILE_SPECIFIC_TESTS})
    public void testAccessControlDelete()
    {
        String tableName = uniqueTableName();
        setup(tableName);

        queryAsAlice("INSERT INTO %s VALUES (4, 13)", tableName);
        assertThat(queryAsAlice("SELECT * FROM %s", tableName)).hasRowsCount(1);
        assertThat(() -> connectAsBob().executeQuery(format("DELETE FROM %s WHERE day=4", tableName)))
                .failsWithMessage(format("Access Denied: Cannot delete from table default.%s", tableName));

        queryAsAlice("GRANT DELETE ON %s TO bob", tableName);
        connectAsBob().executeQuery(format("DELETE FROM %s", tableName));
        assertThat(queryAsAlice("SELECT * FROM %s", tableName)).hasNoRows();
    }

    @Test(groups = {AUTHORIZATION, HIVE_CONNECTOR, PROFILE_SPECIFIC_TESTS})
    public void testAccessControlCreateTableAsSelect()
    {
        String tableName = uniqueTableName();
        setup(tableName);

        String createTableAsSelect = "bob_create_table_as_select";

        assertThat(() -> connectAsBob().executeQuery(format("CREATE TABLE %s AS SELECT * FROM %s", createTableAsSelect, tableName)))
                .failsWithMessage(format("Access Denied: Cannot select from table default.%s", tableName));

        queryAsAlice("GRANT SELECT ON %s TO bob", tableName);
        connectAsBob().executeQuery(format("CREATE TABLE %s AS SELECT * FROM %s", createTableAsSelect, tableName));
        assertThat(queryAsBob(createTableAsSelect, "SELECT * FROM %s")).hasNoRows();
    }

    @Test(groups = {AUTHORIZATION, HIVE_CONNECTOR, PROFILE_SPECIFIC_TESTS})
    public void testAccessControlDropTable()
    {
        String tableName = uniqueTableName();
        setup(tableName);

        assertThat(() -> connectAsBob().executeQuery(format("DROP TABLE %s", tableName)))
                .failsWithMessage(format("Access Denied: Cannot drop table default.%s", tableName));

        queryAsAlice("DROP TABLE %s", tableName);
        assertThat(() -> queryAsAlice("SELECT * FROM %s", tableName))
                .failsWithMessage("does not exist");
    }

    @Test(groups = {AUTHORIZATION, HIVE_CONNECTOR, PROFILE_SPECIFIC_TESTS})
    public void testAccessControlAlterTable()
    {
        String tableName = uniqueTableName();
        setup(tableName);

        assertThat(queryAsAlice("SHOW COLUMNS FROM %s", tableName)).hasRowsCount(2);
        assertThat(() -> connectAsBob().executeQuery(format("ALTER TABLE %s ADD COLUMN year bigint", tableName))).
                failsWithMessage(format("Access Denied: Cannot add a column to table default.%s", tableName));

        queryAsAlice("ALTER TABLE %s ADD COLUMN year bigint", tableName);
        assertThat(queryAsAlice("SHOW COLUMNS FROM %s", tableName)).hasRowsCount(3);
    }

    @Test(groups = {AUTHORIZATION, HIVE_CONNECTOR, PROFILE_SPECIFIC_TESTS})
    public void testAccessControlCreateView()
    {
        String tableName = uniqueTableName();
        String viewName = uniqueTableName();
        setup(tableName);

        String selectTableSql = format("SELECT * FROM %s", tableName);
        String createViewSql = format("CREATE VIEW %s AS %s", viewName, selectTableSql);

        queryAsBob(viewName, "DROP VIEW IF EXISTS %s");
        assertThat(() -> connectAsBob().executeQuery(createViewSql))
                .failsWithMessage(format("Access Denied: Cannot select from table default.%s", tableName));
        queryAsAlice("GRANT SELECT ON %s TO bob", tableName);
        assertThat(() -> connectAsBob().executeQuery(createViewSql))
                .failsWithMessage(format("Access Denied: Cannot create view that selects from default.%s", tableName));

        queryAsAlice("REVOKE SELECT ON %s FROM bob", tableName);
        queryAsAlice("GRANT SELECT ON %s TO bob WITH GRANT OPTION", tableName);
        executeWith(createViewAs(viewName, selectTableSql, connectAsBob()), view -> {
            assertThat(queryAsBob(view.getName(), "SELECT * FROM %s"))
                    .hasNoRows();
        });
    }

    @Test(groups = {AUTHORIZATION, HIVE_CONNECTOR, PROFILE_SPECIFIC_TESTS})
    public void testAccessControlDropView()
    {
        String tableName = uniqueTableName();
        String viewName = uniqueTableName();
        setup(tableName);

        connectAsAlice().executeQuery(format("CREATE VIEW %s AS SELECT * FROM %s", viewName, tableName));
        assertThat(() -> queryAsBob(viewName, "DROP VIEW %s"))
                .failsWithMessage(format("Access Denied: Cannot drop view default.%s", viewName));

        queryAsAlice("DROP VIEW %s", viewName);
        assertThat(() -> queryAsAlice("SELECT * FROM %s", viewName))
                .failsWithMessage("does not exist");
    }

    private QueryResult queryAsAlice(String format, Object... tableName)
    {
        return connectAsAlice().executeQuery(format(format, tableName));
    }

    private QueryExecutor connectAsAlice()
    {
        return connectToPresto("alice@presto");
    }

    private QueryResult queryAsBob(String tableName, String format)
    {
        return connectAsBob().executeQuery(format(format, tableName));
    }

    private QueryExecutor connectAsBob()
    {
        return connectToPresto("bob@presto");
    }
}
