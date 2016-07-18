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
import org.testng.annotations.Test;

import static com.facebook.presto.tests.TestGroups.AUTHORIZATION;
import static com.facebook.presto.tests.TestGroups.HIVE_CONNECTOR;
import static com.facebook.presto.tests.TestGroups.PROFILE_SPECIFIC_TESTS;
import static com.facebook.presto.tests.utils.QueryExecutors.connectToPresto;
import static com.teradata.tempto.assertions.QueryAssert.assertThat;
import static java.lang.String.format;

public class TestGrantRevoke
    extends ProductTest
{
    /*
     * Pre-requisites for the tests in this class:
     *
     * (1) hive.properties file should have this property set: hive.security=sql-standard
     * (2) tempto-configuration.yaml file should have definitions for two connections to Presto server:
     * "alice@presto" that has "jdbc_user: alice" and
     * "bob@presto" that has "jdbc_user: bob"
     * (all other values of the connection are same as that of the default "presto" connection).
    */
    @Test(groups = {HIVE_CONNECTOR, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testGrantRevoke()
    {
        String tableName = "alice_owned_table";
        QueryExecutor queryExecutorForAlice = connectToPresto("alice@presto");
        QueryExecutor queryExecutorForBob = connectToPresto("bob@presto");

        queryExecutorForAlice.executeQuery(format("DROP TABLE IF EXISTS %s", tableName));
        queryExecutorForAlice.executeQuery(format("CREATE TABLE %s(month bigint, day bigint)", tableName));

        assertThat(() -> queryExecutorForBob.executeQuery(format("SELECT * FROM %s", tableName))).
                failsWithMessage(format("Access Denied: Cannot select from table default.%s", tableName));
        assertThat(() -> queryExecutorForBob.executeQuery(format("INSERT INTO %s VALUES (3, 22)", tableName))).
                failsWithMessage(format("Access Denied: Cannot insert into table default.%s", tableName));

        //test GRANT
        queryExecutorForAlice.executeQuery(format("GRANT INSERT, SELECT ON %s TO bob", tableName));
        assertThat(queryExecutorForBob.executeQuery(format("INSERT INTO %s VALUES (3, 22)", tableName))).hasRowsCount(1);
        assertThat(queryExecutorForBob.executeQuery(format("SELECT * FROM %s", tableName))).hasRowsCount(1);
        assertThat(() -> queryExecutorForBob.executeQuery(format("DELETE FROM %s WHERE day=3", tableName))).
                failsWithMessage(format("Access Denied: Cannot delete from table default.%s", tableName));

        //test REVOKE
        queryExecutorForAlice.executeQuery(format("REVOKE INSERT ON %s FROM bob", tableName));
        assertThat(() -> queryExecutorForBob.executeQuery(format("INSERT INTO %s VALUES ('y', 5)", tableName))).
                failsWithMessage(format("Access Denied: Cannot insert into table default.%s", tableName));
        assertThat(queryExecutorForBob.executeQuery(format("SELECT * FROM %s", tableName))).hasRowsCount(1);
    }

    @Test(groups = {HIVE_CONNECTOR, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testGrantRevokeAll()
    {
        String tableName = "alice_owned_table";
        QueryExecutor queryExecutorForAlice = connectToPresto("alice@presto");
        QueryExecutor queryExecutorForBob = connectToPresto("bob@presto");

        queryExecutorForAlice.executeQuery(format("DROP TABLE IF EXISTS %s", tableName));
        queryExecutorForAlice.executeQuery(format("CREATE TABLE %s(month bigint, day bigint)", tableName));

        assertAccessDeniedOnAllOperationsOnTable(queryExecutorForBob, tableName);

        queryExecutorForAlice.executeQuery(format("GRANT ALL PRIVILEGES ON %s TO bob", tableName));
        assertThat(queryExecutorForBob.executeQuery(format("INSERT INTO %s VALUES (4, 13)", tableName))).hasRowsCount(1);
        assertThat(queryExecutorForBob.executeQuery(format("SELECT * FROM %s", tableName))).hasRowsCount(1);
        queryExecutorForBob.executeQuery(format("DELETE FROM %s", tableName));
        assertThat(queryExecutorForBob.executeQuery(format("SELECT * FROM %s", tableName))).hasNoRows();

        queryExecutorForAlice.executeQuery(format("REVOKE ALL PRIVILEGES ON %s FROM bob", tableName));
        assertAccessDeniedOnAllOperationsOnTable(queryExecutorForBob, tableName);
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

    //TODO: test PUBLIC: This will require adding users such as alice and bob to the hive metastore
}
