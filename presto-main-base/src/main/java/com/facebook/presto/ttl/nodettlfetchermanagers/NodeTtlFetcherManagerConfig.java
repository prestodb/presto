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
package com.facebook.presto.ttl.nodettlfetchermanagers;

import com.facebook.airlift.configuration.Config;
import io.airlift.units.Duration;

public class NodeTtlFetcherManagerConfig
{
    private Duration initialDelayBeforeRefresh = Duration.valueOf("1m");
    private Duration staleTtlThreshold = Duration.valueOf("15m");
    private NodeTtlFetcherManagerType nodeTtlFetcherManagerType = NodeTtlFetcherManagerType.THROWING;

    public enum NodeTtlFetcherManagerType
    {
        THROWING,
        CONFIDENCE
    }

    public Duration getInitialDelayBeforeRefresh()
    {
        return initialDelayBeforeRefresh;
    }

    @Config("node-ttl-fetcher-manager.initial-delay-before-refresh")
    public NodeTtlFetcherManagerConfig setInitialDelayBeforeRefresh(Duration initialDelayBeforeRefresh)
    {
        this.initialDelayBeforeRefresh = initialDelayBeforeRefresh;
        return this;
    }

    @Config("node-ttl-fetcher-manager.stale-ttl-threshold")
    public NodeTtlFetcherManagerConfig setStaleTtlThreshold(Duration staleTtlThreshold)
    {
        this.staleTtlThreshold = staleTtlThreshold;
        return this;
    }

    public Duration getStaleTtlThreshold()
    {
        return staleTtlThreshold;
    }

    @Config("node-ttl-fetcher-manager.type")
    public NodeTtlFetcherManagerConfig setNodeTtlFetcherManagerType(NodeTtlFetcherManagerType nodeTtlFetcherManagerType)
    {
        this.nodeTtlFetcherManagerType = nodeTtlFetcherManagerType;
        return this;
    }

    public NodeTtlFetcherManagerType getNodeTtlFetcherManagerType()
    {
        return nodeTtlFetcherManagerType;
    }
}
