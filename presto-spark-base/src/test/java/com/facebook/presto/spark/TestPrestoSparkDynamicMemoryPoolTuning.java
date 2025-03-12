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
package com.facebook.presto.spark;

import com.facebook.presto.Session;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.QUERY_MAX_MEMORY_PER_NODE;
import static com.facebook.presto.SystemSessionProperties.QUERY_MAX_TOTAL_MEMORY_PER_NODE;
import static com.facebook.presto.spark.PrestoSparkQueryRunner.createHivePrestoSparkQueryRunner;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.ATTEMPT_NUMBER_TO_APPLY_DYNAMIC_MEMORY_POOL_TUNING;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.DYNAMIC_PRESTO_MEMORY_POOL_TUNING_ENABLED;
import static io.airlift.tpch.TpchTable.getTables;

public class TestPrestoSparkDynamicMemoryPoolTuning
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
    {
        return createHivePrestoSparkQueryRunner(getTables());
    }

    @Test
    public void testDynamicMemoryPoolTuning()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(QUERY_MAX_MEMORY_PER_NODE, "100B")
                .setSystemProperty(QUERY_MAX_TOTAL_MEMORY_PER_NODE, "100B")
                .setSystemProperty(DYNAMIC_PRESTO_MEMORY_POOL_TUNING_ENABLED, "false")
                .build();

        // Query should fail with OOM
        assertQueryFails(
                session,
                "select * from lineitem l join orders o on l.orderkey = o.orderkey",
                ".*Query exceeded per-node total memory limit.*");

        session = Session.builder(getSession())
                .setSystemProperty(QUERY_MAX_MEMORY_PER_NODE, "100B")
                .setSystemProperty(QUERY_MAX_TOTAL_MEMORY_PER_NODE, "100B")
                .setSystemProperty(DYNAMIC_PRESTO_MEMORY_POOL_TUNING_ENABLED, "true")
                .setSystemProperty(ATTEMPT_NUMBER_TO_APPLY_DYNAMIC_MEMORY_POOL_TUNING, "0")
                .build();

        // Query should succeed since dynamic memory tuning will override the static presto memory config values
        assertQuery(
                session,
                "select * from lineitem l join orders o on l.orderkey = o.orderkey");
    }
}
