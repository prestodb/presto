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

package com.facebook.presto.nodettlfetchers.infinite;

import com.facebook.presto.spi.ttl.NodeInfo;
import com.facebook.presto.spi.ttl.NodeTtl;
import com.facebook.presto.spi.ttl.NodeTtlFetcher;
import com.google.common.collect.ImmutableSet;

import java.util.Map;
import java.util.Set;

import static com.facebook.presto.spi.ttl.ConfidenceBasedTtlInfo.getInfiniteTtl;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

public class InfiniteNodeTtlFetcher
        implements NodeTtlFetcher
{
    @Override
    public Map<NodeInfo, NodeTtl> getTtlInfo(Set<NodeInfo> nodes)
    {
        requireNonNull(nodes, "nodes is null");
        return nodes.stream().collect(toImmutableMap(identity(), node -> new NodeTtl(ImmutableSet.of(getInfiniteTtl()))));
    }

    @Override
    public boolean needsPeriodicRefresh()
    {
        return false;
    }
}
