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
import com.facebook.presto.tests.tpch.TpchQueryRunnerBuilder;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.DISTRIBUTED_SORT;
import static com.facebook.presto.SystemSessionProperties.LOCAL_DISTRIBUTED_SORT;
import static com.facebook.presto.SystemSessionProperties.REDISTRIBUTE_SORT;

public class TestDistributedSortQueries
        extends AbstractTestQueries
{
    public TestDistributedSortQueries()
    {
        super(() -> TpchQueryRunnerBuilder.builder()
                .setExtraProperties(ImmutableMap.of(
                        "experimental.distributed-sort", "true",
                        "experimental.redistribute-sort", "true",
                        "experimental.local-distributed-sort", "true"))
                .build());
    }

    @Test
    public void testSimpleOrderByNoRedistribution()
    {
        Session distributedOrderByNoRedistribution = Session.builder(getSession())
                .setSystemProperty(DISTRIBUTED_SORT, "true")
                .setSystemProperty(REDISTRIBUTE_SORT, "false")
                .setSystemProperty(LOCAL_DISTRIBUTED_SORT, "true")
                .build();
        assertQueryOrdered(distributedOrderByNoRedistribution, "SELECT orderstatus FROM orders ORDER BY orderstatus");
    }

    @Test
    public void testSimpleOrderByNoLocalDistributedSort()
    {
        Session distributedOrderByNoRedistribution = Session.builder(getSession())
                .setSystemProperty(DISTRIBUTED_SORT, "true")
                .setSystemProperty(REDISTRIBUTE_SORT, "true")
                .setSystemProperty(LOCAL_DISTRIBUTED_SORT, "false")
                .build();
        assertQueryOrdered(distributedOrderByNoRedistribution, "SELECT orderstatus FROM orders ORDER BY orderstatus");
    }
}
