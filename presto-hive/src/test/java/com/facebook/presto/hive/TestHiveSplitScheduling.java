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
import com.facebook.presto.spi.eventlistener.EventListener;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.hive.HiveSessionProperties.DYNAMIC_SPLIT_SIZES_ENABLED;
import static io.airlift.tpch.TpchTable.ORDERS;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestHiveSplitScheduling
        extends AbstractTestQueryFramework
{
    @Override
    public QueryRunner createQueryRunner()
            throws Exception
    {
        return HiveQueryRunner.createQueryRunner(
                ImmutableList.of(ORDERS),
                ImmutableMap.of(),
                "sql-standard",
                ImmutableMap.of("hive.max-initial-split-size", "1kB"),
                Optional.empty());
    }

    @Test
    public void testDynamicSplits()
    {
        try {
            getQueryRunner().execute("CREATE TABLE test_orders WITH (partitioned_by = ARRAY['ds', 'ts']) AS " +
                    "SELECT orderkey, orderpriority, comment, custkey, '2020-09-01' as ds, '00:01' as ts FROM orders WHERE orderkey < 1000 " +
                    "UNION ALL " +
                    "SELECT orderkey, orderpriority, comment, custkey, '2020-09-02' as ds, '00:02' as ts FROM orders WHERE orderkey >= 1000 AND orderkey < 2000");

            TestHiveEventListenerPlugin.TestingHiveEventListener eventListener = getEventListener();

            executeExclusively(() -> {
                eventListener.resetSplits();
                getQueryRunner().execute("SELECT orderpriority FROM test_orders where ds = '2020-09-01' and substr(orderpriority, 1, 1) = '1'");
                int numberOfSplitsWithoutDynamicSplitScheduling = eventListener.getTotalSplits();
                eventListener.resetSplits();
                getQueryRunner().execute(dynamicSplitsSession(), "SELECT orderpriority FROM test_orders where ds = '2020-09-01' and substr(orderpriority, 1, 1) = '1'");
                // Less splits using dynamic number of splits.
                int numberOfSplitsWithDynamicSplitScheduling = eventListener.getTotalSplits();
                assertTrue(numberOfSplitsWithDynamicSplitScheduling < numberOfSplitsWithoutDynamicSplitScheduling, "Expected less splits with dynamic split scheduling");
            });
        }
        finally {
            getQueryRunner().execute("DROP TABLE IF EXISTS test_orders");
        }
    }

    private TestHiveEventListenerPlugin.TestingHiveEventListener getEventListener()
    {
        Optional<EventListener> eventListener = getQueryRunner().getEventListener();
        assertTrue(eventListener.isPresent());
        assertTrue(eventListener.get() instanceof TestHiveEventListenerPlugin.TestingHiveEventListener, eventListener.get().getClass().getName());
        return (TestHiveEventListenerPlugin.TestingHiveEventListener) eventListener.get();
    }

    private Session dynamicSplitsSession()
    {
        Session.SessionBuilder builder = Session.builder(getQueryRunner().getDefaultSession())
                .setCatalogSessionProperty("hive", DYNAMIC_SPLIT_SIZES_ENABLED, "true");
        return builder.build();
    }
}
