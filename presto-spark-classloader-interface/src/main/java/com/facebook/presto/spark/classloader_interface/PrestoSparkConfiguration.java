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
package com.facebook.presto.spark.classloader_interface;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;

public class PrestoSparkConfiguration
{
    public static final String METADATA_STORAGE_TYPE_KEY = "metadata_storage_type";
    public static final String METADATA_STORAGE_TYPE_LOCAL = "LOCAL";

    private final Map<String, String> configProperties;
    private final String pluginsDirectoryPath;
    private final Map<String, Map<String, String>> catalogProperties;
    private final Map<String, String> prestoSparkProperties;
    private final Optional<Map<String, String>> nativeWorkerConfigProperties;
    private final Optional<Map<String, String>> eventListenerProperties;
    private final Optional<Map<String, String>> accessControlProperties;
    private final Optional<Map<String, String>> sessionPropertyConfigurationProperties;
    private final Optional<Map<String, Map<String, String>>> functionNamespaceProperties;
    private final Optional<Map<String, Map<String, String>>> tempStorageProperties;

    public PrestoSparkConfiguration(
            Map<String, String> configProperties,
            String pluginsDirectoryPath,
            Map<String, Map<String, String>> catalogProperties,
            Map<String, String> prestoSparkProperties,
            Optional<Map<String, String>> nativeWorkerConfigProperties,
            Optional<Map<String, String>> eventListenerProperties,
            Optional<Map<String, String>> accessControlProperties,
            Optional<Map<String, String>> sessionPropertyConfigurationProperties,
            Optional<Map<String, Map<String, String>>> functionNamespaceProperties,
            Optional<Map<String, Map<String, String>>> tempStorageProperties)
    {
        this.configProperties = unmodifiableMap(new HashMap<>(requireNonNull(configProperties, "configProperties is null")));
        this.pluginsDirectoryPath = requireNonNull(pluginsDirectoryPath, "pluginsDirectoryPath is null");
        this.catalogProperties = unmodifiableMap(requireNonNull(catalogProperties, "catalogProperties is null").entrySet().stream()
                .collect(toMap(Map.Entry::getKey, entry -> unmodifiableMap(new HashMap<>(entry.getValue())))));
        this.prestoSparkProperties = unmodifiableMap(new HashMap<>(requireNonNull(prestoSparkProperties, "prestoSparkProperties is null")));
        requireNonNull(prestoSparkProperties.get(METADATA_STORAGE_TYPE_KEY), "prestoSparkProperties must contain " + METADATA_STORAGE_TYPE_KEY);
        this.nativeWorkerConfigProperties = requireNonNull(nativeWorkerConfigProperties, "nativeWorkerConfigProperties is null");
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
    }

    public Map<String, String> getConfigProperties()
    {
        return configProperties;
    }

    public String getPluginsDirectoryPath()
    {
        return pluginsDirectoryPath;
    }

    public Map<String, Map<String, String>> getCatalogProperties()
    {
        return catalogProperties;
    }

    public Map<String, String> getPrestoSparkProperties()
    {
        return prestoSparkProperties;
    }

    public String getMetadataStorageType()
    {
        return prestoSparkProperties.get(METADATA_STORAGE_TYPE_KEY);
    }

    public Optional<Map<String, String>> getNativeWorkerConfigProperties()
    {
        return nativeWorkerConfigProperties;
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
}
