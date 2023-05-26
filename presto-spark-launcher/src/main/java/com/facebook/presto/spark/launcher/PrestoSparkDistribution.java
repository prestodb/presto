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

import static com.facebook.presto.spark.classloader_interface.PrestoSparkConfiguration.METADATA_STORAGE_TYPE_KEY;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;

public class PrestoSparkDistribution
{
    private final SparkContext sparkContext;
    private final PackageSupplier packageSupplier;
    private final Map<String, String> configProperties;
    private final Map<String, Map<String, String>> catalogProperties;
    private final Map<String, String> prestoSparkProperties;
    private final Optional<Map<String, String>> eventListenerProperties;
    private final Optional<Map<String, String>> accessControlProperties;
    private final Optional<Map<String, String>> sessionPropertyConfigurationProperties;
    private final Optional<Map<String, Map<String, String>>> functionNamespaceProperties;
    private final Optional<Map<String, Map<String, String>>> tempStorageProperties;
    private final boolean isNativeExecutor;

    @Deprecated
    public PrestoSparkDistribution(
            SparkContext sparkContext,
            PackageSupplier packageSupplier,
            Map<String, String> configProperties,
            Map<String, Map<String, String>> catalogProperties,
            String metadataStorageType,
            Optional<Map<String, String>> eventListenerProperties,
            Optional<Map<String, String>> accessControlProperties,
            Optional<Map<String, String>> sessionPropertyConfigurationProperties,
            Optional<Map<String, Map<String, String>>> functionNamespaceProperties,
            Optional<Map<String, Map<String, String>>> tempStorageProperties,
            boolean isNativeExecutor)
    {
        this(
                sparkContext,
                packageSupplier,
                configProperties,
                catalogProperties,
                ImmutableMap.of(METADATA_STORAGE_TYPE_KEY, metadataStorageType),
                eventListenerProperties,
                accessControlProperties,
                sessionPropertyConfigurationProperties,
                functionNamespaceProperties,
                tempStorageProperties,
                isNativeExecutor);
    }

    public PrestoSparkDistribution(
            SparkContext sparkContext,
            PackageSupplier packageSupplier,
            Map<String, String> configProperties,
            Map<String, Map<String, String>> catalogProperties,
            Map<String, String> prestoSparkProperties,
            Optional<Map<String, String>> eventListenerProperties,
            Optional<Map<String, String>> accessControlProperties,
            Optional<Map<String, String>> sessionPropertyConfigurationProperties,
            Optional<Map<String, Map<String, String>>> functionNamespaceProperties,
            Optional<Map<String, Map<String, String>>> tempStorageProperties)
    {
        this(
                sparkContext,
                packageSupplier,
                configProperties,
                catalogProperties,
                prestoSparkProperties,
                eventListenerProperties,
                accessControlProperties,
                sessionPropertyConfigurationProperties,
                functionNamespaceProperties,
                tempStorageProperties,
                // TODO: testing-only value set to true
                // The actual code is invoked by presto-facebook repo
                // Set to false before submitting for review
                true);
    }

    public PrestoSparkDistribution(
            SparkContext sparkContext,
            PackageSupplier packageSupplier,
            Map<String, String> configProperties,
            Map<String, Map<String, String>> catalogProperties,
            Map<String, String> prestoSparkProperties,
            Optional<Map<String, String>> eventListenerProperties,
            Optional<Map<String, String>> accessControlProperties,
            Optional<Map<String, String>> sessionPropertyConfigurationProperties,
            Optional<Map<String, Map<String, String>>> functionNamespaceProperties,
            Optional<Map<String, Map<String, String>>> tempStorageProperties,
            boolean isNativeExecutor)
    {
        this.sparkContext = requireNonNull(sparkContext, "sparkContext is null");
        this.packageSupplier = requireNonNull(packageSupplier, "packageSupplier is null");
        this.configProperties = ImmutableMap.copyOf(requireNonNull(configProperties, "configProperties is null"));
        this.catalogProperties = requireNonNull(catalogProperties, "catalogProperties is null").entrySet().stream()
                .collect(toImmutableMap(Map.Entry::getKey, entry -> ImmutableMap.copyOf(entry.getValue())));
        this.prestoSparkProperties = ImmutableMap.copyOf(requireNonNull(prestoSparkProperties, "prestoSparkProperties is null"));
        this.eventListenerProperties = requireNonNull(eventListenerProperties, "eventListenerProperties is null")
                .map(properties -> unmodifiableMap(new HashMap<>(properties)));
        this.accessControlProperties = requireNonNull(accessControlProperties, "accessControlProperties is null")
                .map(properties -> unmodifiableMap(new HashMap<>(properties)));
        this.sessionPropertyConfigurationProperties = requireNonNull(sessionPropertyConfigurationProperties, "sessionPropertyConfigurationProperties is null")
                .map(properties -> unmodifiableMap(new HashMap<>(properties)));
        this.functionNamespaceProperties = requireNonNull(functionNamespaceProperties, "functionNamespaceProperties is null")
                .map(map -> map.entrySet().stream()
                        .collect(toMap(Map.Entry::getKey, entry -> unmodifiableMap(new HashMap<>(entry.getValue())))));
        this.tempStorageProperties = requireNonNull(tempStorageProperties, "tempStorageProperties is null")
                .map(map -> map.entrySet().stream()
                        .collect(toMap(Map.Entry::getKey, entry -> unmodifiableMap(new HashMap<>(entry.getValue())))));
        this.isNativeExecutor = isNativeExecutor;
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

    public Map<String, String> getPrestoSparkProperties()
    {
        return prestoSparkProperties;
    }

    public Optional<Map<String, String>> getEventListenerProperties()
    {
        return eventListenerProperties;
    }

    public Optional<Map<String, String>> getAccessControlProperties()
    {
        return accessControlProperties;
    }

    public Optional<Map<String, String>> getSessionPropertyConfigurationProperties()
    {
        return sessionPropertyConfigurationProperties;
    }

    public Optional<Map<String, Map<String, String>>> getFunctionNamespaceProperties()
    {
        return functionNamespaceProperties;
    }

    public Optional<Map<String, Map<String, String>>> getTempStorageProperties()
    {
        return tempStorageProperties;
    }

    public boolean isNativeExecutor()
    {
        return isNativeExecutor;
    }
}
