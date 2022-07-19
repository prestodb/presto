package com.facebook.presto.tests;

import com.facebook.presto.Session;
import com.facebook.presto.testing.QueryRunner;
import org.testng.annotations.Test;

import java.util.HashMap;

import static com.facebook.presto.SystemSessionProperties.AGGREGATION_OPERATOR_UNSPILL_MEMORY_LIMIT;
import static com.facebook.presto.SystemSessionProperties.DEDUP_BASED_DISTINCT_AGGREGATION_SPILL_ENABLED;
import static com.facebook.presto.SystemSessionProperties.QUERY_MAX_MEMORY_PER_NODE;
import static com.facebook.presto.SystemSessionProperties.QUERY_MAX_REVOCABLE_MEMORY_PER_NODE;
import static com.facebook.presto.SystemSessionProperties.QUERY_MAX_TOTAL_MEMORY_PER_NODE;
import static com.facebook.presto.SystemSessionProperties.TOPN_SPILL_ENABLED;
import static com.facebook.presto.SystemSessionProperties.USE_MARK_DISTINCT;
import static com.facebook.presto.SystemSessionProperties.WINDOW_SPILL_ENABLED;

public class TestSpilledTopNQueries
        extends AbstractTestTopN

{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        HashMap<String, String> spillProperties = new HashMap<>();
        spillProperties.put(TOPN_SPILL_ENABLED, "true");
        spillProperties.put(QUERY_MAX_MEMORY_PER_NODE, "10B");

        return TestDistributedSpilledQueries.createDistributedSpillingQueryRunner(spillProperties);
    }

    @Test
    public void testDoesNotSpillTopNWhenDisabled()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(TOPN_SPILL_ENABLED, "false")
                // set this low so that if we ran without spill the query would fail
                .setSystemProperty(QUERY_MAX_TOTAL_MEMORY_PER_NODE, "1B")
                .build();
        assertQueryFails(session,
                "SELECT orderpriority, custkey FROM orders ORDER BY orderpriority  LIMIT 1000", "Query exceeded.*memory limit.*");
    }
}
