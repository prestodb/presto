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
import static java.lang.String.format;

public class TestGrantRevoke
    extends ProductTest
{
    /*
     * Pre-requisites for the tests in this class:
     *
     * (1) hive.properties file should have this property set: hive.security=sql-standard
     * (2) tempto-configuration.yaml file should have definitions for the following connections to Presto server:
     *          - "alice@presto" that has "jdbc_user: alice"
     *          - "bob@presto" that has "jdbc_user: bob"
     *     (all other values of the connection are same as that of the default "presto" connection).
    */
    private static void setup(String tableName)
    {
        String format = format("DROP TABLE IF EXISTS %s", tableName);
        executeQueryAsAlice(format);
        executeQueryAsAlice(format("CREATE TABLE %s(month bigint, day bigint)", tableName));

        assertAccessDeniedOnAllOperationsOnTable(connectToPrestoAsBob(), tableName);
    }

    @Test(groups = {HIVE_CONNECTOR, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testGrantRevoke()
    {
        String tableName = uniqueTableName();
        setup(tableName);

        // test GRANT
        executeQueryAsAlice(format("GRANT SELECT ON %s TO bob WITH GRANT OPTION", tableName));
        String selectStarFromTable = format("SELECT * FROM %s", tableName);
        assertThat(executeQueryAsBob(selectStarFromTable)).hasNoRows();
        executeQueryAsAlice(format("GRANT INSERT, SELECT ON %s TO bob", tableName));
        assertThat(executeQueryAsBob(format("INSERT INTO %s VALUES (3, 22)", tableName))).hasRowsCount(1);
        assertThat(executeQueryAsBob(selectStarFromTable)).hasRowsCount(1);
        assertThat(() -> executeQueryAsBob(format("DELETE FROM %s WHERE day=3", tableName))).
                failsWithMessage(format("Access Denied: Cannot delete from table default.%s", tableName));

        // test REVOKE
        executeQueryAsAlice(format("REVOKE INSERT ON %s FROM bob", tableName));
        assertThat(() -> executeQueryAsBob(format("INSERT INTO %s VALUES ('y', 5)", tableName))).
                failsWithMessage(format("Access Denied: Cannot insert into table default.%s", tableName));
        assertThat(executeQueryAsBob(selectStarFromTable)).hasRowsCount(1);
        executeQueryAsAlice(format("REVOKE INSERT, SELECT ON %s FROM bob", tableName));
        assertThat(() -> executeQueryAsBob(selectStarFromTable)).
                failsWithMessage(format("Access Denied: Cannot select from table default.%s", tableName));
    }

    @Test(groups = {HIVE_CONNECTOR, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testGrantRevokeAll()
    {
        String tableName = uniqueTableName();
        setup(tableName);

        executeQueryAsAlice(format("GRANT ALL PRIVILEGES ON %s TO bob", tableName));
        assertThat(executeQueryAsBob(format("INSERT INTO %s VALUES (4, 13)", tableName))).hasRowsCount(1);
        assertThat(executeQueryAsBob(format("SELECT * FROM %s", tableName))).hasRowsCount(1);
        executeQueryAsBob(format("DELETE FROM %s", tableName));
        assertThat(executeQueryAsBob(format("SELECT * FROM %s", tableName))).hasNoRows();

        executeQueryAsAlice(format("REVOKE ALL PRIVILEGES ON %s FROM bob", tableName));
        assertAccessDeniedOnAllOperationsOnTable(connectToPrestoAsBob(), tableName);
    }

    @Test(groups = {HIVE_CONNECTOR, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testPublic()
    {
        String tableName = uniqueTableName();
        setup(tableName);

        executeQueryAsAlice(format("GRANT SELECT ON %s TO PUBLIC", tableName));
        assertThat(executeQueryAsBob(format("SELECT * FROM %s", tableName))).hasNoRows();
        executeQueryAsAlice(format("REVOKE SELECT ON %s FROM PUBLIC", tableName));
        assertThat(() -> executeQueryAsBob(format("SELECT * FROM %s", tableName))).
                failsWithMessage(format("Access Denied: Cannot select from table default.%s", tableName));
        assertThat(executeQueryAsAlice(format("SELECT * FROM %s", tableName))).hasNoRows();
    }

    private static void assertAccessDeniedOnAllOperationsOnTable(QueryExecutor queryExecutor, String tableName)
    {
        assertThat(() -> queryExecutor.executeQuery(format("SELECT * FROM %s", tableName))).
                failsWithMessage(format("Access Denied: Cannot select from table default.%s", tableName));
        assertThat(() -> queryExecutor.executeQuery(format("INSERT INTO %s VALUES (3, 22)", tableName))).
                failsWithMessage(format("Access Denied: Cannot insert into table default.%s", tableName));
        assertThat(() -> queryExecutor.executeQuery(format("DELETE FROM %s WHERE day=3", tableName))).
                failsWithMessage(format("Access Denied: Cannot delete from table default.%s", tableName));
    }

    private static QueryResult executeQueryAsAlice(String format)
    {
        return connectToPrestoAsAlice().executeQuery(format);
    }

    private static QueryExecutor connectToPrestoAsAlice()
    {
        return connectToPresto("alice@presto");
    }

    private static QueryResult executeQueryAsBob(String selectStarFromTable)
    {
        return connectToPrestoAsBob().executeQuery(selectStarFromTable);
    }

    private static QueryExecutor connectToPrestoAsBob()
    {
        return connectToPresto("bob@presto");
    }
}
