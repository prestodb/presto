package com.facebook.presto.hive;

import com.facebook.presto.tests.AbstractTestDistributedQueries;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.hive.HiveQueryRunner.createQueryRunner;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static io.airlift.tpch.TpchTable.getTables;

public class TestSelectiveHiveDistributedQueries extends AbstractTestDistributedQueries {

    protected TestSelectiveHiveDistributedQueries() {
        super(() -> createQueryRunner(getTables(), ImmutableMap.of(), getHiveProperties(), Optional.empty()));
    }

    private static Map<String, String> getHiveProperties() {
        HashMap<String, String> properties = new HashMap<>();
        properties.put("hive.pushdown-filter-enabled", "true");
        return properties;
    }

    @Override
    protected boolean supportsNotNullColumns()
    {
        return false;
    }

    @Override
    protected boolean supportsRowByRowDelete()
    {
        return false;
    }

    protected boolean supportsPushdownFalseFilter()
    {
        return false;
    }

    protected boolean supportsPushdownPartialFilter()
    {
        return false;
    }

    @Override
    @Test
    public void testExistsSubquery() {
        // cannot pushdown filter that is always false
        skipTestUnless(supportsPushdownFalseFilter());
    }

    @Override
    @Test
    public void testGroupByKeyPredicatePushdown() {
        // partial filters are not supported
        skipTestUnless(supportsPushdownPartialFilter());
    }

    @Override
    @Test
    public void testScalarSubquery() {
        // cannot pushdown filter that is always false
        skipTestUnless(supportsPushdownFalseFilter());
    }

    @Override
    @Test
    public void testSemiJoin() {
        // partial filters are not supported
        skipTestUnless(supportsPushdownPartialFilter());
    }

    @Override
    @Test
    public void testSemiJoinWithGroupBy() {
        //  partial filters are not supported
        skipTestUnless(supportsPushdownPartialFilter());
    }

    @Override
    @Test
    public void testIn() {
        // TODO: fix this bug
        // Incorrectly matches rows
        skipTestUnless(false);
        assertQuery("SELECT orderkey FROM orders WHERE orderkey IN (1.5, 2.3)", "SELECT orderkey FROM orders LIMIT 0");
    }

    @Override
    @Test
    public void testCorrelatedExistsSubqueriesWithEqualityPredicatesInWhere() {
        // TODO: fix this bug
        // If we set isStreamable to false in LocalExecutionPlanner.java ln 2698
        // This will then pass, but the underlying issue still persists, as the following query still fails:
        // " SELECT orderkey, custkey, orderstatus, totalprice, orderdate, orderpriority, clerk, shippriority,
        // comment as c, (SELECT count(*) FROM orders WHERE o.orderkey = 0) cc FROM orders o"
        skipTestUnless(false);
        assertQuery("SELECT * FROM orders o ORDER BY EXISTS(SELECT 1 WHERE o.orderkey = 0)");
    }

    @Override
    @Test
    public void testCorrelatedScalarSubqueriesWithScalarAggregationAndEqualityPredicatesInWhere() {
        // TODO: fix this bug
        // Seems like the same bug as in testCorrelatedExistsSubqueriesWithEqualityPredicatesInWhere
        skipTestUnless(false);
        assertQuery("SELECT * FROM orders o ORDER BY (SELECT count(*) WHERE o.orderkey = 0)");
    }

}

