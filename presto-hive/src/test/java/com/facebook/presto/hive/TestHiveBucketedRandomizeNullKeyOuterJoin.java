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
package com.facebook.presto.hive;

import com.facebook.presto.Session;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableList;
import io.airlift.tpch.TpchTable;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static com.facebook.presto.SystemSessionProperties.RANDOMIZE_OUTER_JOIN_NULL_KEY;
import static java.lang.String.format;
import static org.testng.Assert.assertTrue;

/**
 * Verifies {@code RandomizeNullKeyInOuterJoin} over BUCKETED Hive tables joined on the bucket column.
 * This is the one path that routes the randomized key through the connector's bucket partition function
 * (the TPC-H-based tests deliberately join on non-bucket columns and so only exercise the system-hash
 * exchange). It confirms the rewrite fires, does not overflow/crash a real (hash-based) bucket function,
 * and returns results identical to the un-optimized plan.
 */
public class TestHiveBucketedRandomizeNullKeyOuterJoin
        extends AbstractTestQueryFramework
{
    private static final String RANDOMIZE_MARKER = "RandomizeNullKeyInOuterJoin";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HiveQueryRunner.createQueryRunner(ImmutableList.<TpchTable<?>>of());
    }

    private Session randomizeEnabled()
    {
        return Session.builder(getSession())
                .setSystemProperty(RANDOMIZE_OUTER_JOIN_NULL_KEY, "true")
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "PARTITIONED")
                .build();
    }

    private boolean rewriteFires(Session session, String sql)
    {
        return ((String) computeActual(session, "EXPLAIN (TYPE DISTRIBUTED) " + sql).getOnlyValue()).contains(RANDOMIZE_MARKER);
    }

    @Test
    public void testBucketedBigintKey()
    {
        assertUpdate("CREATE TABLE rz_probe_bigint (k bigint, v bigint) WITH (bucketed_by = ARRAY['k'], bucket_count = 4)");
        assertUpdate("INSERT INTO rz_probe_bigint VALUES (1, 10), (2, 20), (3, 30), (4, 40), (cast(null as bigint), 50), (cast(null as bigint), 60)", 6);
        assertUpdate("CREATE TABLE rz_build_bigint (k bigint, w bigint) WITH (bucketed_by = ARRAY['k'], bucket_count = 4)");
        assertUpdate("INSERT INTO rz_build_bigint VALUES (1, 100), (2, 200), (4, 400), (cast(null as bigint), 500)", 4);
        try {
            Session enabled = randomizeEnabled();
            for (String joinType : ImmutableList.of("left join", "right join", "full join")) {
                String sql = format("SELECT p.k, p.v, b.w FROM rz_probe_bigint p %s rz_build_bigint b ON p.k = b.k", joinType);
                // The rewrite fires on the bucket-column join, so the randomized key flows through the Hive bucket function.
                assertTrue(rewriteFires(enabled, sql), "expected randomization to fire for BIGINT " + joinType);
                // Spreading null keys must not change results versus the un-optimized plan.
                assertQueryWithSameQueryRunner(enabled, sql, getSession());
            }
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS rz_probe_bigint");
            assertUpdate("DROP TABLE IF EXISTS rz_build_bigint");
        }
    }

    @Test
    public void testBucketedVarcharKey()
    {
        assertUpdate("CREATE TABLE rz_probe_varchar (k varchar, v bigint) WITH (bucketed_by = ARRAY['k'], bucket_count = 4)");
        assertUpdate("INSERT INTO rz_probe_varchar VALUES ('1', 10), ('2', 20), ('3', 30), ('4', 40), (cast(null as varchar), 50), (cast(null as varchar), 60)", 6);
        assertUpdate("CREATE TABLE rz_build_varchar (k varchar, w bigint) WITH (bucketed_by = ARRAY['k'], bucket_count = 4)");
        assertUpdate("INSERT INTO rz_build_varchar VALUES ('1', 100), ('2', 200), ('4', 400), (cast(null as varchar), 500)", 4);
        try {
            Session enabled = randomizeEnabled();
            for (String joinType : ImmutableList.of("left join", "right join", "full join")) {
                String sql = format("SELECT p.k, p.v, b.w FROM rz_probe_varchar p %s rz_build_varchar b ON p.k = b.k", joinType);
                // The right key is cast to VARCHAR so both sides share a type; the rewrite fires for all outer
                // join types, including FULL.
                assertTrue(rewriteFires(enabled, sql), "expected randomization to fire for VARCHAR " + joinType);
                assertQueryWithSameQueryRunner(enabled, sql, getSession());
            }
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS rz_probe_varchar");
            assertUpdate("DROP TABLE IF EXISTS rz_build_varchar");
        }
    }
}
