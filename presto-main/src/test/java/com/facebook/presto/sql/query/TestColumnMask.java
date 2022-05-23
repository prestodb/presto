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
package com.facebook.presto.sql.query;

import com.facebook.presto.Session;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.security.ViewExpression;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.testing.TestingAccessControlManager;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;

public class TestColumnMask
{
    private static final String CATALOG = "local";
    private static final String USER = "user";
    private static final String RUN_AS_USER = "run-as-user";

    private QueryAssertions assertions;
    private TestingAccessControlManager accessControl;

    @BeforeClass
    public void init()
    {
        Session session = testSessionBuilder()
                .setCatalog(CATALOG)
                .setSchema(TINY_SCHEMA_NAME)
                .setIdentity(new Identity(USER, Optional.empty())).build();

        LocalQueryRunner runner = new LocalQueryRunner(session);

        runner.createCatalog(CATALOG, new TpchConnectorFactory(1), ImmutableMap.of());

        assertions = new QueryAssertions(runner);
        accessControl = assertions.getQueryRunner().getAccessControl();
    }

    @AfterClass(alwaysRun = true)
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    public void testSimpleMask()
    {
        assertions.executeExclusively(() -> {
            accessControl.reset();
            accessControl.columnMask(
                    new QualifiedObjectName(CATALOG, "tiny", "orders"),
                    "custkey",
                    USER,
                    new ViewExpression(USER, Optional.empty(), Optional.empty(), "-custkey"));
            assertions.assertQuery("SELECT custkey FROM orders WHERE orderkey = 1", "VALUES BIGINT '-370'");
        });

        assertions.executeExclusively(() -> {
            accessControl.reset();
            accessControl.columnMask(
                    new QualifiedObjectName(CATALOG, "tiny", "orders"),
                    "custkey",
                    USER,
                    new ViewExpression(USER, Optional.empty(), Optional.empty(), "NULL"));
            assertions.assertQuery("SELECT custkey FROM orders WHERE orderkey = 1", "VALUES CAST(NULL AS BIGINT)");
        });
    }

    @Test
    public void testMultipleMasks()
    {
        assertions.executeExclusively(() -> {
            accessControl.reset();
            accessControl.columnMask(
                    new QualifiedObjectName(CATALOG, "tiny", "orders"),
                    "custkey",
                    USER,
                    new ViewExpression(USER, Optional.empty(), Optional.empty(), "-custkey"));

            accessControl.columnMask(
                    new QualifiedObjectName(CATALOG, "tiny", "orders"),
                    "custkey",
                    USER,
                    new ViewExpression(USER, Optional.empty(), Optional.empty(), "custkey * 2"));

            assertions.assertQuery("SELECT custkey FROM orders WHERE orderkey = 1", "VALUES BIGINT '-740'");
        });
    }

    @Test
    public void testCoercibleType()
    {
        assertions.executeExclusively(() -> {
            accessControl.reset();
            accessControl.columnMask(
                    new QualifiedObjectName(CATALOG, "tiny", "orders"),
                    "clerk",
                    USER,
                    new ViewExpression(USER, Optional.empty(), Optional.empty(), "CAST(clerk AS VARCHAR(5))"));
            assertions.assertQuery("SELECT clerk FROM orders WHERE orderkey = 1", "VALUES CAST('Clerk' AS VARCHAR(15))");
        });
    }

    @Test
    public void testSubquery()
    {
        // uncorrelated
        assertions.executeExclusively(() -> {
            accessControl.reset();
            accessControl.columnMask(
                    new QualifiedObjectName(CATALOG, "tiny", "orders"),
                    "clerk",
                    USER,
                    new ViewExpression(USER, Optional.of(CATALOG), Optional.of("tiny"), "(SELECT cast(max(name) AS VARCHAR(15)) FROM nation)"));
            assertions.assertQuery("SELECT clerk FROM orders WHERE orderkey = 1", "VALUES CAST('VIETNAM' AS VARCHAR(15))");
        });

        // correlated
        assertions.executeExclusively(() -> {
            accessControl.reset();
            accessControl.columnMask(
                    new QualifiedObjectName(CATALOG, "tiny", "orders"),
                    "clerk",
                    USER,
                    new ViewExpression(USER, Optional.of(CATALOG), Optional.of("tiny"), "(SELECT cast(max(name) AS VARCHAR(15)) FROM nation WHERE nationkey = orderkey)"));
            assertions.assertQuery("SELECT clerk FROM orders WHERE orderkey = 1", "VALUES CAST('ARGENTINA' AS VARCHAR(15))");
        });
    }

    @Test
    public void testTableReferenceInWithClause()
    {
        assertions.executeExclusively(() -> {
            accessControl.reset();
            accessControl.columnMask(
                    new QualifiedObjectName(CATALOG, "tiny", "orders"),
                    "custkey",
                    USER,
                    new ViewExpression(USER, Optional.empty(), Optional.empty(), "-custkey"));
            assertions.assertQuery("WITH t AS (SELECT custkey FROM orders WHERE orderkey = 1) SELECT * FROM t", "VALUES BIGINT '-370'");
        });
    }

    @Test
    public void testOtherSchema()
    {
        assertions.executeExclusively(() -> {
            accessControl.reset();
            accessControl.columnMask(
                    new QualifiedObjectName(CATALOG, "tiny", "orders"),
                    "orderkey",
                    USER,
                    new ViewExpression(USER, Optional.of(CATALOG), Optional.of("sf1"), "(SELECT count(*) FROM customer)")); // count is 15000 only when evaluating against sf1
            assertions.assertQuery("SELECT max(orderkey) FROM orders", "VALUES BIGINT '150000'");
        });
    }

    @Test
    public void testDifferentIdentity()
    {
        assertions.executeExclusively(() -> {
            accessControl.reset();
            accessControl.columnMask(
                    new QualifiedObjectName(CATALOG, "tiny", "orders"),
                    "orderkey",
                    RUN_AS_USER,
                    new ViewExpression(RUN_AS_USER, Optional.of(CATALOG), Optional.of("tiny"), "100"));

            accessControl.columnMask(
                    new QualifiedObjectName(CATALOG, "tiny", "orders"),
                    "orderkey",
                    USER,
                    new ViewExpression(RUN_AS_USER, Optional.of(CATALOG), Optional.of("tiny"), "(SELECT sum(orderkey) FROM orders)"));

            assertions.assertQuery("SELECT max(orderkey) FROM orders", "VALUES BIGINT '1500000'");
        });
    }

    @Test
    public void testRecursion()
    {
        assertions.executeExclusively(() -> {
            accessControl.reset();
            accessControl.columnMask(
                    new QualifiedObjectName(CATALOG, "tiny", "orders"),
                    "orderkey",
                    USER,
                    new ViewExpression(USER, Optional.of(CATALOG), Optional.of("tiny"), "(SELECT orderkey FROM orders)"));

            assertions.assertFails("SELECT orderkey FROM orders", ".*\\QColumn mask for 'local.tiny.orders.orderkey' is recursive\\E.*");
        });

        // different reference style to same table
        assertions.executeExclusively(() -> {
            accessControl.reset();
            accessControl.columnMask(
                    new QualifiedObjectName(CATALOG, "tiny", "orders"),
                    "orderkey",
                    USER,
                    new ViewExpression(USER, Optional.of(CATALOG), Optional.of("tiny"), "(SELECT orderkey FROM local.tiny.orders)"));

            assertions.assertFails("SELECT orderkey FROM orders", ".*\\QColumn mask for 'local.tiny.orders.orderkey' is recursive\\E.*");
        });

        // mutual recursion
        assertions.executeExclusively(() -> {
            accessControl.reset();
            accessControl.columnMask(
                    new QualifiedObjectName(CATALOG, "tiny", "orders"),
                    "orderkey",
                    RUN_AS_USER,
                    new ViewExpression(RUN_AS_USER, Optional.of(CATALOG), Optional.of("tiny"), "(SELECT orderkey FROM orders)"));

            accessControl.columnMask(
                    new QualifiedObjectName(CATALOG, "tiny", "orders"),
                    "orderkey",
                    USER,
                    new ViewExpression(RUN_AS_USER, Optional.of(CATALOG), Optional.of("tiny"), "(SELECT orderkey FROM orders)"));

            assertions.assertFails("SELECT orderkey FROM orders", ".*\\QColumn mask for 'local.tiny.orders.orderkey' is recursive\\E.*");
        });
    }

    @Test
    public void testLimitedScope()
    {
        assertions.executeExclusively(() -> {
            accessControl.reset();
            accessControl.columnMask(
                    new QualifiedObjectName(CATALOG, "tiny", "customer"),
                    "custkey",
                    USER,
                    new ViewExpression(USER, Optional.of(CATALOG), Optional.of("tiny"), "orderkey"));
            assertions.assertFails(
                    "SELECT (SELECT min(custkey) FROM customer WHERE customer.custkey = orders.custkey) FROM orders",
                    "\\Qline 1:34: Invalid column mask for 'local.tiny.customer.custkey': Column 'orderkey' cannot be resolved\\E");
        });
    }

    @Test
    public void testSqlInjection()
    {
        assertions.executeExclusively(() -> {
            accessControl.reset();
            accessControl.columnMask(
                    new QualifiedObjectName(CATALOG, "tiny", "nation"),
                    "name",
                    USER,
                    new ViewExpression(USER, Optional.of(CATALOG), Optional.of("tiny"), "(SELECT name FROM region WHERE regionkey = 0)"));
            assertions.assertQuery(
                    "WITH region(regionkey, name) AS (VALUES (0, 'ASIA'))" +
                            "SELECT name FROM nation ORDER BY name LIMIT 1",
                    "VALUES CAST('AFRICA' AS VARCHAR(25))"); // if sql-injection would work then query would return ASIA
        });
    }

    @Test
    public void testInvalidMasks()
    {
        // parse error
        assertions.executeExclusively(() -> {
            accessControl.reset();
            accessControl.columnMask(
                    new QualifiedObjectName(CATALOG, "tiny", "orders"),
                    "orderkey",
                    USER,
                    new ViewExpression(RUN_AS_USER, Optional.of(CATALOG), Optional.of("tiny"), "$$$"));

            assertions.assertFails("SELECT orderkey FROM orders", "\\QInvalid column mask for 'local.tiny.orders.orderkey': mismatched input '$'. Expecting: <expression>\\E");
        });

        // unknown column
        assertions.executeExclusively(() -> {
            accessControl.reset();
            accessControl.columnMask(
                    new QualifiedObjectName(CATALOG, "tiny", "orders"),
                    "orderkey",
                    USER,
                    new ViewExpression(RUN_AS_USER, Optional.of(CATALOG), Optional.of("tiny"), "unknown_column"));

            assertions.assertFails("SELECT orderkey FROM orders", "\\Qline 1:1: Column 'unknown_column' cannot be resolved\\E");
        });

        // invalid type
        assertions.executeExclusively(() -> {
            accessControl.reset();
            accessControl.columnMask(
                    new QualifiedObjectName(CATALOG, "tiny", "orders"),
                    "orderkey",
                    USER,
                    new ViewExpression(RUN_AS_USER, Optional.of(CATALOG), Optional.of("tiny"), "'foo'"));

            assertions.assertFails("SELECT orderkey FROM orders", "\\QExpected column mask for 'local.tiny.orders.orderkey' to be of type bigint, but was varchar(3)\\E");
        });

        // aggregation
        assertions.executeExclusively(() -> {
            accessControl.reset();
            accessControl.columnMask(
                    new QualifiedObjectName(CATALOG, "tiny", "orders"),
                    "orderkey",
                    USER,
                    new ViewExpression(RUN_AS_USER, Optional.of(CATALOG), Optional.of("tiny"), "count(*) > 0"));

            assertions.assertFails("SELECT orderkey FROM orders", "\\Qline 1:10: Column mask for 'orders.orderkey' cannot contain aggregations, window functions or grouping operations: [\"count\"(*)]\\E");
        });

        // window function
        assertions.executeExclusively(() -> {
            accessControl.reset();
            accessControl.columnMask(
                    new QualifiedObjectName(CATALOG, "tiny", "orders"),
                    "orderkey",
                    USER,
                    new ViewExpression(RUN_AS_USER, Optional.of(CATALOG), Optional.of("tiny"), "row_number() OVER () > 0"));

            assertions.assertFails("SELECT orderkey FROM orders", "\\Qline 1:22: Column mask for 'orders.orderkey' cannot contain aggregations, window functions or grouping operations: [\"row_number\"() OVER ()]\\E");
        });

        // grouping function
        assertions.executeExclusively(() -> {
            accessControl.reset();
            accessControl.columnMask(
                    new QualifiedObjectName(CATALOG, "tiny", "orders"),
                    "orderkey",
                    USER,
                    new ViewExpression(USER, Optional.of(CATALOG), Optional.of("tiny"), "grouping(orderkey) = 0"));

            assertions.assertFails("SELECT orderkey FROM orders", "\\Qline 1:20: Column mask for 'orders.orderkey' cannot contain aggregations, window functions or grouping operations: [GROUPING (orderkey)]\\E");
        });
    }
}
