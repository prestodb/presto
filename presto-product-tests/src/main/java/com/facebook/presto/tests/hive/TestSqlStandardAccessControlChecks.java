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

import io.prestodb.tempto.BeforeTestWithContext;
import io.prestodb.tempto.ProductTest;
import io.prestodb.tempto.query.QueryExecutor;
import org.testng.annotations.Test;

import static com.facebook.presto.tests.TestGroups.AUTHORIZATION;
import static com.facebook.presto.tests.TestGroups.HIVE_CONNECTOR;
import static com.facebook.presto.tests.TestGroups.PROFILE_SPECIFIC_TESTS;
import static com.facebook.presto.tests.utils.QueryExecutors.connectToPresto;
import static io.prestodb.tempto.assertions.QueryAssert.assertThat;
import static java.lang.String.format;

public class TestSqlStandardAccessControlChecks
        extends ProductTest
{
    private String tableName;
    private QueryExecutor aliceExecutor;
    private QueryExecutor bobExecutor;
    private QueryExecutor charlieExecutor;

    @BeforeTestWithContext
    public void setup()
    {
        tableName = "alice_owned_table";
        aliceExecutor = connectToPresto("alice@presto");
        bobExecutor = connectToPresto("bob@presto");
        charlieExecutor = connectToPresto("charlie@presto");

        aliceExecutor.executeQuery(format("DROP TABLE IF EXISTS %s", tableName));
        aliceExecutor.executeQuery(format("CREATE TABLE %s(month bigint, day bigint) WITH (partitioned_by = ARRAY['day'])", tableName));
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
    public void testAccessControlSelectFromPartitions()
    {
        assertThat(() -> bobExecutor.executeQuery(format("SELECT * FROM \"%s$partitions\"", tableName)))
                .failsWithMessage(format("Access Denied: Cannot select from table default.%s$partitions", tableName));

        aliceExecutor.executeQuery(format("GRANT SELECT ON %s TO bob", tableName));
        assertThat(bobExecutor.executeQuery(format("SELECT * FROM \"%s$partitions\"", tableName))).hasNoRows();
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

        bobExecutor.executeQuery("DROP TABLE IF EXISTS " + createTableAsSelect);
        assertThat(() -> bobExecutor.executeQuery(format("CREATE TABLE %s AS SELECT * FROM %s", createTableAsSelect, tableName)))
                .failsWithMessage(format("Access Denied: Cannot select from table default.%s", tableName));

        aliceExecutor.executeQuery(format("GRANT SELECT ON %s TO bob", tableName));
        bobExecutor.executeQuery(format("CREATE TABLE %s AS SELECT * FROM %s", createTableAsSelect, tableName));
        assertThat(bobExecutor.executeQuery(format("SELECT * FROM %s", createTableAsSelect))).hasNoRows();
        bobExecutor.executeQuery("DROP TABLE " + createTableAsSelect);
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
        assertThat(() -> bobExecutor.executeQuery(format("ALTER TABLE %s ADD COLUMN year bigint", tableName)))
                .failsWithMessage(format("Access Denied: Cannot add a column to table default.%s", tableName));

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

        // Bob needs SELECT on the table to create the view
        bobExecutor.executeQuery("DROP VIEW IF EXISTS " + viewName);
        assertThat(() -> bobExecutor.executeQuery(createViewSql))
                .failsWithMessage(format("Access Denied: Cannot select from table default.%s", tableName));

        // Give Bob access to table, then create and execute view
        aliceExecutor.executeQuery(format("GRANT SELECT ON %s TO bob", tableName));
        bobExecutor.executeQuery(createViewSql);
        assertThat(bobExecutor.executeQuery(format("SELECT * FROM %s", viewName))).hasNoRows();

        // Verify that Charlie does not have SELECT on the view, then grant access
        assertThat(() -> charlieExecutor.executeQuery(format("SELECT * FROM %s", viewName)))
                .failsWithMessage(format("Access Denied: Cannot select from table default.%s", viewName));
        bobExecutor.executeQuery(format("GRANT SELECT ON %s TO charlie", viewName));

        // Charlie still cannot access view because Bob does not have SELECT WITH GRANT OPTION
        assertThat(() -> charlieExecutor.executeQuery(format("SELECT * FROM %s", viewName)))
                .failsWithMessage(format("Access Denied: View owner 'bob' cannot create view that selects from default.%s", tableName));

        // Give Bob SELECT WITH GRANT OPTION on the underlying table
        aliceExecutor.executeQuery(format("REVOKE SELECT ON %s FROM bob", tableName));
        aliceExecutor.executeQuery(format("GRANT SELECT ON %s TO bob WITH GRANT OPTION", tableName));

        // Bob has GRANT OPTION, so both Bob and Charlie can access the view
        assertThat(bobExecutor.executeQuery(format("SELECT * FROM %s", viewName))).hasNoRows();
        assertThat(charlieExecutor.executeQuery(format("SELECT * FROM %s", viewName))).hasNoRows();

        bobExecutor.executeQuery("DROP VIEW " + viewName);
    }

    @Test(groups = {AUTHORIZATION, HIVE_CONNECTOR, PROFILE_SPECIFIC_TESTS})
    public void testAccessControlDropView()
    {
        String viewName = "alice_view_for_drop";

        aliceExecutor.executeQuery("DROP VIEW IF EXISTS " + viewName);
        aliceExecutor.executeQuery(format("CREATE VIEW %s AS SELECT * FROM %s", viewName, tableName));
        assertThat(() -> bobExecutor.executeQuery(format("DROP VIEW %s", viewName)))
                .failsWithMessage(format("Access Denied: Cannot drop view default.%s", viewName));

        aliceExecutor.executeQuery(format("DROP VIEW %s", viewName));
        assertThat(() -> aliceExecutor.executeQuery(format("SELECT * FROM %s", viewName)))
                .failsWithMessage("does not exist");
    }
}
