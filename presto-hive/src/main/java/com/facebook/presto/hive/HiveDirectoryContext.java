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

import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.spi.security.ConnectorIdentity;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class HiveDirectoryContext
{
    private final NestedDirectoryPolicy nestedDirectoryPolicy;
    private final ConnectorIdentity connectorIdentity;
    private final Map<String, String> additionalProperties;
    private final RuntimeStats runtimeStats;
    private boolean cacheable;
    private boolean skipEmptyFiles;

    public HiveDirectoryContext(
            NestedDirectoryPolicy nestedDirectoryPolicy,
            boolean cacheable,
            boolean skipEmptyFiles,
            ConnectorIdentity connectorIdentity,
            Map<String, String> additionalProperties,
            RuntimeStats runtimeStats)
    {
        this.nestedDirectoryPolicy = requireNonNull(nestedDirectoryPolicy, "nestedDirectoryPolicy is null");
        this.connectorIdentity = requireNonNull(connectorIdentity, "connectorIdentity is null");
        this.additionalProperties = ImmutableMap.copyOf(requireNonNull(additionalProperties, "additionalProperties is null"));
        this.runtimeStats = requireNonNull(runtimeStats, "runtimeStats is null");

        // this can be disabled
        this.cacheable = cacheable;
        this.skipEmptyFiles = skipEmptyFiles;
    }

    public NestedDirectoryPolicy getNestedDirectoryPolicy()
    {
        return nestedDirectoryPolicy;
    }

    public boolean isCacheable()
    {
        return cacheable;
    }

    public void disableCaching()
    {
        cacheable = false;
    }

    public ConnectorIdentity getConnectorIdentity()
    {
        return connectorIdentity;
    }

    public Map<String, String> getAdditionalProperties()
    {
        return additionalProperties;
    }

    public boolean isSkipEmptyFilesEnabled()
    {
        return skipEmptyFiles;
    }

    public RuntimeStats getRuntimeStats()
    {
        return runtimeStats;
    }
}
