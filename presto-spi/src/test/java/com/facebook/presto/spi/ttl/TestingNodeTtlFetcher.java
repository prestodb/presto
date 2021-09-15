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
package com.facebook.presto.spi.ttl;

import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;

public class TestingNodeTtlFetcher
        implements NodeTtlFetcher
{
    private final Map<NodeInfo, NodeTtl> ttlInfo;

    public TestingNodeTtlFetcher(Map<NodeInfo, NodeTtl> ttlInfo)
    {
        this.ttlInfo = ImmutableMap.copyOf(requireNonNull(ttlInfo, "ttlInfo is null"));
    }

    @Override
    public Map<NodeInfo, NodeTtl> getTtlInfo(Set<NodeInfo> nodes)
    {
        return nodes.stream().filter(ttlInfo::containsKey).collect(toImmutableMap(Function.identity(), ttlInfo::get));
    }

    @Override
    public boolean needsPeriodicRefresh()
    {
        return false;
    }
}
