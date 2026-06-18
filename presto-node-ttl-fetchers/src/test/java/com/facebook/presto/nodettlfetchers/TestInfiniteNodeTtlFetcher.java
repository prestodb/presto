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
package com.facebook.presto.nodettlfetchers;

import com.facebook.presto.nodettlfetchers.infinite.InfiniteNodeTtlFetcher;
import com.facebook.presto.spi.ttl.NodeInfo;
import com.facebook.presto.spi.ttl.NodeTtl;
import com.facebook.presto.spi.ttl.NodeTtlFetcher;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.ttl.ConfidenceBasedTtlInfo.getInfiniteTtl;
import static org.testng.Assert.assertEquals;

public class TestInfiniteNodeTtlFetcher
{
    private final NodeInfo nodeInfo = new NodeInfo("id", "host");
    private final NodeTtlFetcher infiniteNodeTtlFetcher = new InfiniteNodeTtlFetcher();

    @Test
    public void testWithNoNodes()
    {
        assertEquals(infiniteNodeTtlFetcher.getTtlInfo(ImmutableSet.of()), ImmutableMap.of());
    }

    @Test
    public void testWithOneNode()
    {
        assertEquals(infiniteNodeTtlFetcher.getTtlInfo(ImmutableSet.of(nodeInfo)), ImmutableMap.of(nodeInfo, new NodeTtl(ImmutableSet.of(getInfiniteTtl()))));
    }
}
