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
package com.facebook.presto.tests;

import com.facebook.presto.Session;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.testing.TestingSession;
import io.airlift.testing.Assertions;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.QUERY_MAX_MEMORY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.TestingAccessControlManager.TestingPrivilegeType.ADD_COLUMN;
import static com.facebook.presto.testing.TestingAccessControlManager.TestingPrivilegeType.CREATE_TABLE;
import static com.facebook.presto.testing.TestingAccessControlManager.TestingPrivilegeType.CREATE_VIEW;
import static com.facebook.presto.testing.TestingAccessControlManager.TestingPrivilegeType.CREATE_VIEW_WITH_SELECT_TABLE;
import static com.facebook.presto.testing.TestingAccessControlManager.TestingPrivilegeType.CREATE_VIEW_WITH_SELECT_VIEW;
import static com.facebook.presto.testing.TestingAccessControlManager.TestingPrivilegeType.DROP_TABLE;
import static com.facebook.presto.testing.TestingAccessControlManager.TestingPrivilegeType.RENAME_COLUMN;
import static com.facebook.presto.testing.TestingAccessControlManager.TestingPrivilegeType.RENAME_TABLE;
import static com.facebook.presto.testing.TestingAccessControlManager.TestingPrivilegeType.SELECT_TABLE;
import static com.facebook.presto.testing.TestingAccessControlManager.TestingPrivilegeType.SELECT_VIEW;
import static com.facebook.presto.testing.TestingAccessControlManager.TestingPrivilegeType.SET_SESSION;
import static com.facebook.presto.testing.TestingAccessControlManager.TestingPrivilegeType.SET_USER;
import static com.facebook.presto.testing.TestingAccessControlManager.privilege;
import static com.facebook.presto.tests.QueryAssertions.assertContains;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public abstract class AbstractTestIntegrationSmokeTest
        extends AbstractTestQueryFramework
{
    private final Optional<Session> sampledSession;

    protected AbstractTestIntegrationSmokeTest(QueryRunner queryRunner)
    {
        this(queryRunner, Optional.empty());
    }

    protected AbstractTestIntegrationSmokeTest(QueryRunner queryRunner, Session sampledSession)
    {
        this(queryRunner, Optional.of(requireNonNull(sampledSession, "sampledSession is null")));
    }

    private AbstractTestIntegrationSmokeTest(QueryRunner queryRunner, Optional<Session> sampledSession)
    {
        super(queryRunner);
        this.sampledSession = requireNonNull(sampledSession, "sampledSession is null");
    }

    @Test
    public void testAggregateSingleColumn()
            throws Exception
    {
        assertQuery("SELECT SUM(orderkey) FROM ORDERS");
        assertQuery("SELECT SUM(totalprice) FROM ORDERS");
        assertQuery("SELECT MAX(comment) FROM ORDERS");
    }

    @Test
    public void testApproximateQuerySum()
            throws Exception
    {
        assertApproximateQuery("SELECT SUM(totalprice) FROM orders APPROXIMATE AT 99.999 CONFIDENCE", "SELECT 2 * SUM(totalprice) FROM orders");
    }

    @Test
    public void testColumnsInReverseOrder()
            throws Exception
    {
        assertQuery("SELECT shippriority, clerk, totalprice FROM ORDERS");
    }

    @Test
    public void testCountAll()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM ORDERS");
    }

    @Test
    public void testExactPredicate()
            throws Exception
    {
        assertQuery("SELECT * FROM ORDERS WHERE orderkey = 10");
    }

    @Test
    public void testInListPredicate()
            throws Exception
    {
        assertQuery("SELECT * FROM ORDERS WHERE orderkey IN (10, 11, 20, 21)");
    }

    @Test
    public void testIsNullPredicate()
            throws Exception
    {
        assertQuery("SELECT * FROM ORDERS WHERE orderkey = 10 OR orderkey IS NULL");
    }

    @Test
    public void testMultipleRangesPredicate()
            throws Exception
    {
        assertQuery("SELECT * FROM ORDERS WHERE orderkey BETWEEN 10 AND 50 or orderkey BETWEEN 100 AND 150");
    }

    @Test
    public void testRangePredicate()
            throws Exception
    {
        assertQuery("SELECT * FROM ORDERS WHERE orderkey BETWEEN 10 AND 50");
    }

    @Test
    public void testSelectAll()
            throws Exception
    {
        assertQuery("SELECT * FROM ORDERS");
    }

    @Test
    public void testTableSampleSystem()
            throws Exception
    {
        if (!sampledSession.isPresent()) {
             return;
        }

        int total = computeActual("SELECT orderkey FROM orders").getMaterializedRows().size();

        boolean sampleSizeFound = false;
        for (int i = 0; i < 100; i++) {
            int sampleSize = computeActual("SELECT orderkey FROM ORDERS TABLESAMPLE SYSTEM (50)").getMaterializedRows().size();
            if (sampleSize > 0 && sampleSize < total) {
                sampleSizeFound = true;
                break;
            }
        }
        assertTrue(sampleSizeFound, "Table sample returned unexpected number of rows");
    }

    @Test
    public void testShowSchemas()
            throws Exception
    {
        MaterializedResult actualSchemas = computeActual("SHOW SCHEMAS").toJdbcTypes();

        MaterializedResult.Builder resultBuilder = MaterializedResult.resultBuilder(queryRunner.getDefaultSession(), VARCHAR)
                .row("tpch");

        if (sampledSession.isPresent()) {
            resultBuilder.row("tpch_sampled");
        }

        assertContains(actualSchemas, resultBuilder.build());
    }

    @Test
    public void testShowTables()
            throws Exception
    {
        MaterializedResult actualTables = computeActual("SHOW TABLES").toJdbcTypes();
        MaterializedResult expectedTables = MaterializedResult.resultBuilder(queryRunner.getDefaultSession(), VARCHAR)
                .row("orders")
                .build();
        assertContains(actualTables, expectedTables);
    }

    @Test
    public void testDescribeTable()
            throws Exception
    {
        MaterializedResult actualColumns = computeActual("DESC ORDERS").toJdbcTypes();

        // some connectors don't support dates, so test with both options
        if (!actualColumns.equals(getExpectedTableDescription(true))) {
            assertEquals(actualColumns, getExpectedTableDescription(false));
        }
    }

    @Test
    public void testNonQueryAccessControl()
            throws Exception
    {
        assertAccessDenied("SET SESSION " + QUERY_MAX_MEMORY + " = '10MB'",
                "Cannot set system session property "  + QUERY_MAX_MEMORY,
                privilege(QUERY_MAX_MEMORY, SET_SESSION));

        assertAccessDenied("CREATE TABLE foo (pk bigint)", "Cannot create table .*.foo.*", privilege("foo", CREATE_TABLE));
        assertAccessDenied("DROP TABLE orders", "Cannot drop table .*.orders.*", privilege("orders", DROP_TABLE));
        assertAccessDenied("ALTER TABLE orders RENAME TO foo", "Cannot rename table .*.orders.* to .*.foo.*", privilege("orders", RENAME_TABLE));
        assertAccessDenied("ALTER TABLE orders ADD COLUMN foo bigint", "Cannot add a column to table .*.orders.*", privilege("orders", ADD_COLUMN));
        assertAccessDenied("ALTER TABLE orders RENAME COLUMN orderkey TO foo", "Cannot rename a column in table .*.orders.*", privilege("orders", RENAME_COLUMN));
        assertAccessDenied("CREATE VIEW foo as SELECT * FROM orders", "Cannot create view .*.foo.*", privilege("foo", CREATE_VIEW));
        // todo add DROP VIEW test... not all connectors have view support

        try {
            assertAccessDenied("SELECT 1", "Principal .* cannot become user " + getSession().getUser() + ".*", privilege(getSession().getUser(), SET_USER));
        }
        catch (AssertionError e) {
            // There is no clean exception message for authorization failure.  We simply get a 403
            Assertions.assertContains(e.getMessage(), "statusCode=403");
        }
    }

    @Test
    public void testViewAccessControl()
            throws Exception
    {
        Session viewOwnerSession = TestingSession.testSessionBuilder()
                .setIdentity(new Identity("test_view_access_owner", Optional.empty()))
                .setCatalog(getSession().getCatalog().get())
                .setSchema(getSession().getSchema().get())
                .build();

        // verify creation of view over a table requires special view creation privileges for the table
        assertAccessDenied(
                viewOwnerSession,
                "CREATE VIEW test_view_access AS SELECT * FROM orders",
                "Cannot select from table .*.orders.*",
                privilege("orders", CREATE_VIEW_WITH_SELECT_TABLE));

        // create the view
        assertAccessAllowed(
                viewOwnerSession,
                "CREATE VIEW test_view_access AS SELECT * FROM orders",
                privilege("bogus", "bogus privilege to disable security", SELECT_TABLE));

        // verify selecting from a view over a table requires the view owner to have special view creation privileges for the table
        assertAccessDenied(
                "SELECT * FROM test_view_access",
                "Cannot select from table .*.orders.*",
                privilege(viewOwnerSession.getUser(), "orders", CREATE_VIEW_WITH_SELECT_TABLE));

        // verify selecting from a view over a table does not require the session user to have SELECT privileges on the underlying table
        assertAccessAllowed(
                "SELECT * FROM test_view_access",
                privilege(getSession().getUser(), "orders", CREATE_VIEW_WITH_SELECT_TABLE));
        assertAccessAllowed(
                "SELECT * FROM test_view_access",
                privilege(getSession().getUser(), "orders", SELECT_TABLE));

        Session nestedViewOwnerSession = TestingSession.testSessionBuilder()
                .setIdentity(new Identity("test_nested_view_access_owner", Optional.empty()))
                .setCatalog(getSession().getCatalog().get())
                .setSchema(getSession().getSchema().get())
                .build();

        // verify creation of view over a view requires special view creation privileges for the view
        assertAccessDenied(
                nestedViewOwnerSession,
                "CREATE VIEW test_nested_view_access AS SELECT * FROM test_view_access",
                "Cannot select from view .*.test_view_access.*",
                privilege("test_view_access", CREATE_VIEW_WITH_SELECT_VIEW));

        // create the nested view
        assertAccessAllowed(
                nestedViewOwnerSession,
                "CREATE VIEW test_nested_view_access AS SELECT * FROM test_view_access",
                privilege("bogus", "bogus privilege to disable security", SELECT_TABLE));

        // verify selecting from a view over a view requires the view owner of the outer view to have special view creation privileges for the inner view
        assertAccessDenied(
                "SELECT * FROM test_nested_view_access",
                "Cannot select from view .*.test_view_access.*",
                privilege(nestedViewOwnerSession.getUser(), "test_view_access", CREATE_VIEW_WITH_SELECT_VIEW));

        // verify selecting from a view over a view does not require the session user to have SELECT privileges for the inner view
        assertAccessAllowed(
                "SELECT * FROM test_nested_view_access",
                privilege(getSession().getUser(), "test_view_access", CREATE_VIEW_WITH_SELECT_VIEW));
        assertAccessAllowed(
                "SELECT * FROM test_nested_view_access",
                privilege(getSession().getUser(), "test_view_access", SELECT_VIEW));

        assertAccessAllowed(nestedViewOwnerSession, "DROP VIEW test_nested_view_access");
        assertAccessAllowed(viewOwnerSession, "DROP VIEW test_view_access");
    }

    private MaterializedResult getExpectedTableDescription(boolean dateSupported)
    {
        String orderDateType;
        if (dateSupported) {
            orderDateType = "date";
        }
        else {
            orderDateType = "varchar";
        }
        return MaterializedResult.resultBuilder(queryRunner.getDefaultSession(), VARCHAR, VARCHAR, VARCHAR)
                    .row("orderkey", "bigint", "")
                    .row("custkey", "bigint", "")
                    .row("orderstatus", "varchar", "")
                    .row("totalprice", "double", "")
                    .row("orderdate", orderDateType, "")
                    .row("orderpriority", "varchar", "")
                    .row("clerk", "varchar", "")
                    .row("shippriority", "integer", "")
                    .row("comment", "varchar", "")
                    .build();
    }

    protected void assertApproximateQuery(@Language("SQL") String actual, @Language("SQL") String expected)
            throws Exception
    {
        if (sampledSession.isPresent()) {
            assertApproximateQuery(sampledSession.get(), actual, expected);
        }
    }
}
