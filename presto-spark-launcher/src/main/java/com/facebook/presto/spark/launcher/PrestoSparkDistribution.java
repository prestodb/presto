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
package com.facebook.presto.spark.launcher;

import com.google.common.collect.ImmutableMap;
import org.apache.spark.SparkContext;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;

public class PrestoSparkDistribution
{
    private final SparkContext sparkContext;
    private final PackageSupplier packageSupplier;
    private final Map<String, String> configProperties;
    private final Map<String, Map<String, String>> catalogProperties;
    private final Optional<Map<String, String>> eventListenerProperties;

    public PrestoSparkDistribution(
            SparkContext sparkContext,
            PackageSupplier packageSupplier,
            Map<String, String> configProperties,
            Map<String, Map<String, String>> catalogProperties,
            Optional<Map<String, String>> eventListenerProperties)
    {
        this.sparkContext = requireNonNull(sparkContext, "sparkContext is null");
        this.packageSupplier = requireNonNull(packageSupplier, "packageSupplier is null");
        this.configProperties = ImmutableMap.copyOf(requireNonNull(configProperties, "configProperties is null"));
        this.catalogProperties = requireNonNull(catalogProperties, "catalogProperties is null").entrySet().stream()
                .collect(toImmutableMap(Map.Entry::getKey, entry -> ImmutableMap.copyOf(entry.getValue())));
        this.eventListenerProperties = requireNonNull(eventListenerProperties, "eventListenerProperties is null")
                .map(properties -> unmodifiableMap(new HashMap<>(properties)));
    }

    public SparkContext getSparkContext()
    {
        return sparkContext;
    }

    public PackageSupplier getPackageSupplier()
    {
        return packageSupplier;
    }

    public Map<String, String> getConfigProperties()
    {
        return configProperties;
    }

    public Map<String, Map<String, String>> getCatalogProperties()
    {
        return catalogProperties;
    }

    public Optional<Map<String, String>> getEventListenerProperties()
    {
        return eventListenerProperties;
    }
}
