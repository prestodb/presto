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
package com.facebook.presto.cassandra;

import com.facebook.presto.Session;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestDistributedQueries;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.ADD_DISTINCT_BELOW_SEMI_JOIN_BUILD;
import static com.facebook.presto.SystemSessionProperties.SIMPLIFY_PLAN_WITH_EMPTY_INPUT;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static java.lang.String.format;

//Integrations tests fail when parallel, due to a bug or configuration error in the embedded
//cassandra instance. This problem results in either a hang in Thrift calls or broken sockets.

public class TestCassandraDistributed
        extends AbstractTestDistributedQueries
{
    private CassandraServer server;
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        this.server = new CassandraServer();
        return CassandraQueryRunner.createCassandraQueryRunner(server);
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        server.close();
    }
    @Override
    protected boolean supportsViews()
    {
        return false;
    }

    @Override
    protected boolean supportsNotNullColumns()
    {
        return false;
    }

    public void testJoinWithLessThanOnDatesInJoinClause()
    {
        // Cassandra does not support DATE
    }

    @Override
    public void testRenameTable()
    {
        // Cassandra does not support renaming tables
    }

    @Override
    public void testAddColumn()
    {
        // Cassandra does not support adding columns
    }

    @Override
    public void testRenameColumn()
    {
        // Cassandra does not support renaming columns
    }

    @Override
    public void testDropColumn()
    {
        // Cassandra does not support dropping columns
    }

    @Override
    public void testInsert()
    {
        // TODO Cassandra connector supports inserts, but the test would fail
    }

    @Override
    public void testCreateTable()
    {
        // Cassandra connector currently does not support create table
    }

    @Override
    public void testDelete()
    {
        // Cassandra connector currently does not support delete
    }

    @Override
    public void testNonAutoCommitTransactionWithFailAndRollback()
    {
        // Ignore since Cassandra connector currently does not support create table
    }

    @Override
    public void testUpdate()
    {
        // Updates are not supported by the connector
    }

    @Override
    public void testShowColumns(@Optional("PARQUET") String storageFormat)
    {
        MaterializedResult actual = computeActual("SHOW COLUMNS FROM orders");

        MaterializedResult expectedParametrizedVarchar = resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR, BIGINT, BIGINT, BIGINT)
                .row("orderkey", "bigint", "", "", Long.valueOf(19), null, null)
                .row("custkey", "bigint", "", "", Long.valueOf(19), null, null)
                .row("orderstatus", "varchar", "", "", null, null, Long.valueOf(2147483647))
                .row("totalprice", "double", "", "", Long.valueOf(53), null, null)
                .row("orderdate", "varchar", "", "", null, null, Long.valueOf(2147483647))
                .row("orderpriority", "varchar", "", "", null, null, Long.valueOf(2147483647))
                .row("clerk", "varchar", "", "", null, null, Long.valueOf(2147483647))
                .row("shippriority", "integer", "", "", Long.valueOf(10), null, null)
                .row("comment", "varchar", "", "", null, null, Long.valueOf(2147483647))
                .build();

        assertEquals(actual, expectedParametrizedVarchar);
    }

    @Override
    public void testDescribeOutput()
    {
        // this connector uses a non-canonical type for varchar columns in tpch
    }

    @Override
    public void testDescribeOutputNamedAndUnnamed()
    {
        // this connector uses a non-canonical type for varchar columns in tpch
    }

    @Override
    public void testWrittenStats()
    {
        // TODO Cassandra connector supports CTAS and inserts, but the test would fail
    }

    @Override
    public void testPayloadJoinApplicability()
    {
        // no op -- test not supported due to lack of support for array types.
    }

    @Override
    public void testPayloadJoinCorrectness()
    {
        // no op -- test not supported due to lack of support for array types.
    }
    @Override
    public void testNonAutoCommitTransactionWithCommit()
    {
        // Connector only supports writes using ctas
    }

    @Override
    public void testNonAutoCommitTransactionWithRollback()
    {
        // Connector only supports writes using ctas
    }

    @Override
    public void testRemoveRedundantCastToVarcharInJoinClause()
    {
        // no op -- test not supported due to lack of support for array types.
    }

    @Override
    public void testStringFilters()
    {
        // no op -- test not supported due to lack of support for char type.
    }

    @Override
    public void testSubfieldAccessControl()
    {
        // no op -- test not supported due to lack of support for array types.
    }

    @Override
    @Parameters("storageFormat")
    @Test
    public void testAddDistinctForSemiJoinBuild(@Optional("PARQUET") String storageFormat)
    {
        Session enabled = Session.builder(getSession())
                .setSystemProperty(ADD_DISTINCT_BELOW_SEMI_JOIN_BUILD, "true")
                .build();
        Session disabled = Session.builder(getSession())
                .setSystemProperty(ADD_DISTINCT_BELOW_SEMI_JOIN_BUILD, "false")
                .build();

        // DWRF does not support date type.
        String format = (System.getProperty("storageFormat") == null) ? storageFormat : System.getProperty("storageFormat");
        String orderdate = "CAST(o.orderdate AS DATE)";

        String sql = format("SELECT * FROM customer c WHERE custkey in ( SELECT custkey FROM orders o WHERE %s > date('1995-01-01'))", orderdate);
        assertQueryWithSameQueryRunner(enabled, sql, disabled);
        sql = format("SELECT * FROM customer c WHERE custkey in ( SELECT distinct custkey FROM orders o WHERE %s > date('1995-01-01'))", orderdate);
        assertQueryWithSameQueryRunner(enabled, sql, disabled);
        sql = "SELECT *\n" +
                "FROM customer c\n" +
                "WHERE c.custkey IN (\n" +
                "  SELECT o.custkey\n" +
                "  FROM orders o\n" +
                "  WHERE o.totalprice > 1000\n" +
                ")";
        assertQueryWithSameQueryRunner(enabled, sql, disabled);
        sql = "SELECT c.name\n" +
                "FROM customer c\n" +
                "WHERE c.custkey IN (\n" +
                "    SELECT o.custkey\n" +
                "    FROM orders o\n" +
                ")";
        assertQueryWithSameQueryRunner(enabled, sql, disabled);
        sql = "SELECT s.name\n" +
                "FROM supplier s\n" +
                "WHERE s.suppkey IN (\n" +
                "    SELECT l.suppkey\n" +
                "    FROM lineitem l\n" +
                ")";
        assertQueryWithSameQueryRunner(enabled, sql, disabled);
        sql = "SELECT c.name FROM customer c WHERE c.custkey IN ( SELECT o.custkey FROM orders o JOIN lineitem l ON o.orderkey = l.orderkey WHERE l.partkey > 1235 )";
        assertQueryWithSameQueryRunner(enabled, sql, disabled);
        sql = "SELECT p.name\n" +
                "FROM part p\n" +
                "WHERE p.partkey IN (\n" +
                "    SELECT l.partkey\n" +
                "    FROM lineitem l\n" +
                "    JOIN orders o ON l.orderkey = o.orderkey\n" +
                "    JOIN customer c ON o.custkey = c.custkey\n" +
                "    JOIN nation n ON c.nationkey = n.nationkey\n" +
                "    WHERE n.name = 'UNITED STATES'\n" +
                ")";
        assertQueryWithSameQueryRunner(enabled, sql, disabled);
    }

    @Override
    @Parameters("storageFormat")
    @Test
    public void testQueryWithEmptyInput(@Optional("PARQUET") String storageFormat)
    {
        Session enableOptimization = Session.builder(getSession())
                .setSystemProperty(SIMPLIFY_PLAN_WITH_EMPTY_INPUT, "true")
                .build();

        String orderdate = "cast(o.orderdate as DATE)";
        String shipdate = "cast(l.shipdate as DATE)";

        assertQuery(enableOptimization, "select o.orderkey, o.custkey, l.linenumber from orders o join (select orderkey, linenumber from lineitem where false) l on o.orderkey = l.orderkey");
        assertQuery(enableOptimization, "select o.orderkey, o.custkey, l.linenumber from orders o left join (select orderkey, linenumber from lineitem where false) l on o.orderkey = l.orderkey");
        assertQuery(enableOptimization, "select o.orderkey, o.custkey, l.linenumber from (select orderkey, linenumber from lineitem where false) l right join orders o on l.orderkey = o.orderkey");
        assertQuery(enableOptimization, "select orderkey, partkey from lineitem union all select orderkey, custkey as partkey from orders where false");
        assertQuery(enableOptimization, "select orderkey, partkey from lineitem where orderkey in (select orderkey from orders where false)");
        assertQuery(enableOptimization, "select orderkey, partkey from lineitem where orderkey not in (select orderkey from orders where false)");
        assertQuery(enableOptimization, "select count(*) as count from (select orderkey from orders where false)");
        assertQuery(enableOptimization, "select orderkey, count(*) as count from (select orderkey from orders where false) group by orderkey");
        assertQuery(enableOptimization, "select o.orderkey, o.custkey, l.linenumber from orders o join (select orderkey, max(linenumber) as linenumber from lineitem where false group by orderkey) l on o.orderkey = l.orderkey");
        assertQuery(enableOptimization, "select orderkey from orders where orderkey = (select orderkey from lineitem where false)");
        assertQuery(enableOptimization, "SELECT (SELECT 1 FROM orders WHERE false LIMIT 1) FROM lineitem");
        assertQuery(enableOptimization, "SELECT (SELECT 1 FROM orders WHERE false LIMIT 1) FROM lineitem");
        assertQuery(enableOptimization, "WITH emptyorders as (select orderkey, totalprice, orderdate from orders where false) SELECT orderkey, orderdate, totalprice, " +
                "ROW_NUMBER() OVER (ORDER BY orderdate) as row_num FROM emptyorders WHERE totalprice > 10 ORDER BY orderdate ASC LIMIT 10");
        String sql = format("WITH emptyorders as (select * from orders where false) SELECT p.name, l.orderkey, l.partkey, l.quantity, RANK() OVER (PARTITION BY p.name ORDER BY l.quantity DESC) AS rank_quantity " +
                "FROM lineitem l JOIN emptyorders o ON l.orderkey = o.orderkey JOIN part p ON l.partkey = p.partkey WHERE %s BETWEEN DATE '1995-03-01' AND DATE '1995-03-31' " +
                "AND %s BETWEEN DATE '1995-03-01' AND DATE '1995-03-31' AND p.size = 15 ORDER BY p.name, rank_quantity LIMIT 100", orderdate, shipdate);
        assertQuery(enableOptimization, sql);
        assertQuery(enableOptimization, "select orderkey, row_number() over (partition by orderpriority), orderpriority from (select orderkey, orderpriority from orders where false)");
        assertQuery(enableOptimization, "select * from (select orderkey, row_number() over (partition by orderpriority order by orderkey) row_number, orderpriority from (select orderkey, orderpriority from orders where false)) where row_number < 2");

        emptyJoinQueries(enableOptimization);
    }

    private void emptyJoinQueries(Session session)
    {
        // Empty predicate and inner join
        assertQuery(session, "select 1 from (select * from orders where 1 = 0) DT join customer on DT.custkey=customer.custkey",
                "select 1 from orders where 1 =0");

        // Empty non-null producing side for outer join.
        assertQuery(session, "select 1 from (select * from orders where 1 = 0) DT left outer join customer on DT.custkey=customer.custkey",
                "select 1 from orders where 1 =0");

        // 3 way join with empty non-null producing side for outer join
        assertQuery(session, "select 1 from (select * from orders where 1 = 0) DT"
                        + " left outer join customer C1 on DT.custkey=C1.custkey"
                        + " left outer join customer C2 on C1.custkey=C2.custkey",
                "select 1 from orders where 1 =0");

        // Zero limit
        assertQuery(session, "select 1 from (select * from orders LIMIT 0) DT join customer on DT.custkey=customer.custkey",
                "select 1 from orders where 1 =0");

        // Negative test.
        assertQuery(session, "select 1 from (select * from orders) DT join customer on DT.custkey=customer.custkey",
                "select 1 from orders");

        // Empty null producing side for outer join.
        assertQuery(session, "select 1 from (select * from orders) ORD left outer join (select custkey from customer where 1=0) " +
                        "CUST on ORD.custkey=CUST.custkey",
                "select 1 from orders");

        // Empty null producing side for left outer join with constant field.
        assertQuery(session, "select One from (select * from orders) ORD left outer join (select 1 as One, custkey from customer where 1=0) " +
                        "CUST on ORD.custkey=CUST.custkey",
                "select null as One from orders");

        // Empty null producing side for right outer join with constant field.
        assertQuery(session, "select One from (select 1 as One, custkey from customer where 1=0) CUST right outer join (select * from orders) ORD " +
                        " ON ORD.custkey=CUST.custkey",
                "select null as One from orders");

        // 3 way join with mix of left and right outer joins. DT left outer join C1 right outer join O2.
        // DT is empty which produces DT right outer join O2 which produce O2 as final result.
        assertQuery(session, "select 1 from (select * from orders where 1 = 0) DT"
                        + " left outer join customer C1 on DT.custkey=C1.custkey"
                        + " right outer join orders O2 on C1.custkey=O2.custkey",
                "select 1 from orders");

        // Empty side for full outer join.
        assertQuery(session, "select 1 from (select * from orders) ORD full outer join (select custkey from customer where 1=0) " +
                        "CUST on ORD.custkey=CUST.custkey",
                "select 1 from orders");

        // Empty side for full outer join as input to aggregation.
        assertQuery(session, "select count(*), orderkey from (select * from orders) ORD full outer join (select custkey from customer where 1=0) " +
                        "CUST on ORD.custkey=CUST.custkey group by orderkey order by orderkey",
                "select count(*), orderkey from orders group by orderkey order by orderkey");
    }
}
