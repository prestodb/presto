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
package com.facebook.presto.spark.execution.property;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Hosts all the configuration for Presto-on-spark native/Cpp Workers
 * Contains 3 parts
 *  1. Config Properties - this is written out as config.properties file
 *  2. Node Properties   - This is written out as node.properties file
 *  3. Catalog properties - These are nested properties 1 map for each catalog. They are written out as corresponding file
 *      (eg: hive.properties, xdb.properties, tpch.properties, ... ) under the catalogs/ folder.
 */
public class NativeWorkerConfiguration
{
    private final Map<String, Map<String, String>> connectorConfigs;
    private final Map<String, String> nodeProperties;
    private final Map<String, String> configProperties;

    public NativeWorkerConfiguration(
            Map<String, Map<String, String>> connectorConfigs,
            Map<String, String> nodeProperties,
            Map<String, String> configProperties)
    {
        this.configProperties = requireNonNull(configProperties, "systemConfig is null");
        this.nodeProperties = requireNonNull(nodeProperties, "nodeConfig is null");
        this.connectorConfigs = requireNonNull(connectorConfigs, "connectorConfig is null");
    }

    public void setConfigProperty(String name, String value)
    {
        configProperties.put(name, value);
    }

    public void writeAllPropertyFiles(Path veloxPropertiesPath, Path configPropertiesPath, Path nodePropertiesPath, String connectorConfigDir)
            throws IOException
    {
        writePropertiesFile(configProperties, veloxPropertiesPath);
        writePropertiesFile(configProperties, configPropertiesPath);
        writePropertiesFile(nodeProperties, nodePropertiesPath);
        for (Map<String, String> connectorConfig : connectorConfigs.values()) {
            String connectorName = connectorConfig.get("connector.name");
            checkArgument(connectorName != null, "connector.name is null for connectorConfig");
            writePropertiesFile(connectorConfig, Paths.get(connectorConfigDir + connectorName));
        }
    }

    private void writePropertiesFile(Map<String, String> properties, Path path)
            throws IOException
    {
        File file = new File(path.toString());
        file.getParentFile().mkdirs();
        try {
            // We're not using Java's Properties here because colon is a reserved character in Properties but our configs contains colon in certain config values (e.g http://)
            FileWriter fileWriter = new FileWriter(file);
            for (Map.Entry<String, String> entry : properties.entrySet()) {
                checkArgument(!entry.getKey().contains("="), format("Config key %s contains invalid character: =", entry.getKey()));
                fileWriter.write(entry.getKey() + "=" + entry.getValue() + "\n");
            }
            fileWriter.close();
        }
        catch (IOException e) {
            Files.deleteIfExists(path);
            throw e;
        }
    }
}
