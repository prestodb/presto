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
package com.facebook.presto.raptorx;

import com.facebook.presto.Session;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.COLOCATED_JOIN;
import static com.facebook.presto.raptorx.RaptorQueryRunner.createRaptorQueryRunner;
import static org.assertj.core.api.Assertions.assertThat;

public class TestRaptorSmokeBucketed
        extends TestRaptorSmoke
{
    public TestRaptorSmokeBucketed()
    {
        super(() -> createRaptorQueryRunner(ImmutableMap.of(), true, true));
    }

    @Test
    public void testChunksSystemTableBucketNumber()
    {
        assertQuery("" +
                        "SELECT count(DISTINCT bucket_number)\n" +
                        "FROM system.chunks\n" +
                        "WHERE table_schema = 'tpch'\n" +
                        "  AND table_name = 'orders'",
                "SELECT 25");
    }

    @Test
    public void testBucketingTableLayout()
    {
        String plan = (String) computeActual(
                Session.builder(getSession()).setSystemProperty(COLOCATED_JOIN, "true").build(),
                "EXPLAIN SELECT count(*) FROM orders JOIN lineitem USING (orderkey)").getOnlyValue();
        assertThat(plan).containsOnlyOnce("RemoteExchange");
    }
}
