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

import com.google.common.collect.ImmutableMap;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class HiveDirectoryContext
{
    private final NestedDirectoryPolicy nestedDirectoryPolicy;
    private final boolean cacheable;
    private final Map<String, String> additionalProperties;

    public HiveDirectoryContext(NestedDirectoryPolicy nestedDirectoryPolicy, boolean cacheable, Map<String, String> additionalProperties)
    {
        this.nestedDirectoryPolicy = requireNonNull(nestedDirectoryPolicy, "nestedDirectoryPolicy is null");
        this.cacheable = cacheable;
        this.additionalProperties = ImmutableMap.copyOf(requireNonNull(additionalProperties, "additionalProperties is null"));
    }

    public NestedDirectoryPolicy getNestedDirectoryPolicy()
    {
        return nestedDirectoryPolicy;
    }

    public boolean isCacheable()
    {
        return cacheable;
    }

    public Map<String, String> getAdditionalProperties()
    {
        return additionalProperties;
    }
}
