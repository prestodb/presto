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

import com.teradata.tempto.BeforeTestWithContext;
import com.teradata.tempto.ProductTest;
import com.teradata.tempto.query.QueryExecutor;
import org.testng.annotations.Test;

import static com.facebook.presto.tests.TestGroups.AUTHORIZATION;
import static com.facebook.presto.tests.TestGroups.HIVE_CONNECTOR;
import static com.facebook.presto.tests.TestGroups.PROFILE_SPECIFIC_TESTS;
import static com.facebook.presto.tests.utils.QueryExecutors.connectToPresto;
import static com.teradata.tempto.assertions.QueryAssert.assertThat;
import static com.teradata.tempto.context.ContextDsl.executeWith;
import static com.teradata.tempto.sql.SqlContexts.createViewAs;
import static java.lang.String.format;

public class TestSqlStandardAccessControlChecks
    extends ProductTest
{
    private String tableName;
    private QueryExecutor aliceExecutor;
    private QueryExecutor bobExecutor;

    @BeforeTestWithContext
    public void setup()
    {
        tableName = "alice_owned_table";
        aliceExecutor = connectToPresto("alice@presto");
        bobExecutor = connectToPresto("bob@presto");

        aliceExecutor.executeQuery(format("DROP TABLE IF EXISTS %s", tableName));
        aliceExecutor.executeQuery(format("CREATE TABLE %s(month bigint, day bigint)", tableName));
    }

    @Test(groups = {AUTHORIZATION, HIVE_CONNECTOR, PROFILE_SPECIFIC_TESTS})
    public void testAccessControlSelect()
    {
        assertThat(() -> bobExecutor.executeQuery(format("SELECT * FROM %s", tableName)))
                .failsWithMessage(format("Access Denied: Cannot select from table default.%s", tableName));

        aliceExecutor.executeQuery(format("GRANT SELECT ON %s TO bob", tableName));
        assertThat(bobExecutor.executeQuery(format("SELECT * FROM %s", tableName))).hasNoRows();
    }

    @Test(groups = {AUTHORIZATION, HIVE_CONNECTOR, PROFILE_SPECIFIC_TESTS})
    public void testAccessControlInsert()
    {
        assertThat(() -> bobExecutor.executeQuery(format("INSERT INTO %s VALUES (3, 22)", tableName)))
                .failsWithMessage(format("Access Denied: Cannot insert into table default.%s", tableName));

        aliceExecutor.executeQuery(format("GRANT INSERT ON %s TO bob", tableName));
        assertThat(bobExecutor.executeQuery(format("INSERT INTO %s VALUES (3, 22)", tableName))).hasRowsCount(1);
        assertThat(aliceExecutor.executeQuery(format("SELECT * FROM %s", tableName))).hasRowsCount(1);
    }

    @Test(groups = {AUTHORIZATION, HIVE_CONNECTOR, PROFILE_SPECIFIC_TESTS})
    public void testAccessControlDelete()
    {
        aliceExecutor.executeQuery(format("INSERT INTO %s VALUES (4, 13)", tableName));
        assertThat(aliceExecutor.executeQuery(format("SELECT * FROM %s", tableName))).hasRowsCount(1);
        assertThat(() -> bobExecutor.executeQuery(format("DELETE FROM %s WHERE day=4", tableName)))
                .failsWithMessage(format("Access Denied: Cannot delete from table default.%s", tableName));

        aliceExecutor.executeQuery(format("GRANT DELETE ON %s TO bob", tableName));
        bobExecutor.executeQuery(format("DELETE FROM %s", tableName));
        assertThat(aliceExecutor.executeQuery(format("SELECT * FROM %s", tableName))).hasNoRows();
    }

    @Test(groups = {AUTHORIZATION, HIVE_CONNECTOR, PROFILE_SPECIFIC_TESTS})
    public void testAccessControlCreateTableAsSelect()
    {
        String createTableAsSelect = "bob_create_table_as_select";

        assertThat(() -> bobExecutor.executeQuery(format("CREATE TABLE %s AS SELECT * FROM %s", createTableAsSelect, tableName)))
                .failsWithMessage(format("Access Denied: Cannot select from table default.%s", tableName));

        aliceExecutor.executeQuery(format("GRANT SELECT ON %s TO bob", tableName));
        bobExecutor.executeQuery(format("CREATE TABLE %s AS SELECT * FROM %s", createTableAsSelect, tableName));
        assertThat(bobExecutor.executeQuery(format("SELECT * FROM %s", createTableAsSelect))).hasNoRows();
    }

    @Test(groups = {AUTHORIZATION, HIVE_CONNECTOR, PROFILE_SPECIFIC_TESTS})
    public void testAccessControlDropTable()
    {
        assertThat(() -> bobExecutor.executeQuery(format("DROP TABLE %s", tableName)))
                .failsWithMessage(format("Access Denied: Cannot drop table default.%s", tableName));

        aliceExecutor.executeQuery(format("DROP TABLE %s", tableName));
        assertThat(() -> aliceExecutor.executeQuery(format("SELECT * FROM %s", tableName)))
                .failsWithMessage("does not exist");
    }

    @Test(groups = {AUTHORIZATION, HIVE_CONNECTOR, PROFILE_SPECIFIC_TESTS})
    public void testAccessControlAlterTable()
    {
        assertThat(aliceExecutor.executeQuery(format("SHOW COLUMNS FROM %s", tableName))).hasRowsCount(2);
        assertThat(() -> bobExecutor.executeQuery(format("ALTER TABLE %s ADD COLUMN year bigint", tableName))).
                failsWithMessage(format("Access Denied: Cannot add a column to table default.%s", tableName));

        aliceExecutor.executeQuery(format("ALTER TABLE %s ADD COLUMN year bigint", tableName));
        assertThat(aliceExecutor.executeQuery(format("SHOW COLUMNS FROM %s", tableName))).hasRowsCount(3);
    }

    @Test(groups = {AUTHORIZATION, HIVE_CONNECTOR, PROFILE_SPECIFIC_TESTS})
    public void testAccessControlCreateView()
    {
        String viewName = "bob_view";
        String selectTableSql = format("SELECT * FROM %s", tableName);
        String createViewSql = format("CREATE VIEW %s AS %s", viewName, selectTableSql);

        bobExecutor.executeQuery(format("DROP VIEW IF EXISTS %s", viewName));
        assertThat(() -> bobExecutor.executeQuery(createViewSql))
                .failsWithMessage(format("Access Denied: Cannot select from table default.%s", tableName));
        aliceExecutor.executeQuery(format("GRANT SELECT ON %s TO bob", tableName));
        assertThat(() -> bobExecutor.executeQuery(createViewSql))
                .failsWithMessage(format("Access Denied: Cannot create view that selects from default.%s", tableName));

        aliceExecutor.executeQuery(format("REVOKE SELECT ON %s FROM bob", tableName));
        aliceExecutor.executeQuery(format("GRANT SELECT ON %s TO bob WITH GRANT OPTION", tableName));
        executeWith(createViewAs(viewName, selectTableSql, bobExecutor), view -> {
            assertThat(bobExecutor.executeQuery(format("SELECT * FROM %s", view.getName())))
                    .hasNoRows();
        });
    }

    @Test(groups = {AUTHORIZATION, HIVE_CONNECTOR, PROFILE_SPECIFIC_TESTS})
    public void testAccessControlDropView()
    {
        String viewName = "alice_view_for_drop";

        aliceExecutor.executeQuery(format("CREATE VIEW %s AS SELECT * FROM %s", viewName, tableName));
        assertThat(() -> bobExecutor.executeQuery(format("DROP VIEW %s", viewName)))
                .failsWithMessage(format("Access Denied: Cannot drop view default.%s", viewName));

        aliceExecutor.executeQuery(format("DROP VIEW %s", viewName));
        assertThat(() -> aliceExecutor.executeQuery(format("SELECT * FROM %s", viewName)))
                .failsWithMessage("does not exist");
    }
}
