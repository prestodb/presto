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
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * The implementor of any new WorkerProperty is expected to provide three configuration classes (or their variants) as minimal requirements to run the native process.
 * The variants of the new configuration classes need to extend these three as base class to provide any new configs or override existing configs. For example:
 * <p>
 * // new configuration variance
 * public class InternalNativeExecutionNodeConfig
 * extends NativeExecutionNodeConfig
 * {
 * private static final String SMC_SERVICE_INVENTORY_TIER = "smc-service-inventory.tier";
 * <p>
 * private String smcServiceInventoryTier = "dummy.tier";
 *
 * @Override public Map<String, String> getAllProperties()
 * {
 * ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
 * return builder.putAll(super.getAllProperties())
 * .put(SMC_SERVICE_INVENTORY_TIER, smcServiceInventoryTier)
 * .build();
 * }
 * @Config(SMC_SERVICE_INVENTORY_TIER) public PrestoFacebookSparkNativeExecutionNodeConfig setSmcServiceInventoryTier(String smcServiceInventoryTier)
 * {
 * this.smcServiceInventoryTier = smcServiceInventoryTier;
 * return this;
 * }
 * }
 * <p>
 * // New WorkerProperty with new InternalNativeExecutionNodeConfig
 * public class InternalWorkerProperty
 * extends WorkerProperty<NativeExecutionConnectorConfig, InternalNativeExecutionNodeConfig, NativeExecutionSystemConfig, NativeExecutionVeloxConfig>
 * {
 * @Inject public InternalWorkerProperty(
 * NativeExecutionConnectorConfig connectorConfig,
 * InternalNativeExecutionNodeConfig nodeConfig,
 * NativeExecutionSystemConfig systemConfig,
 * NativeExecutionVeloxConfig veloxConfig)
 * {
 * super(connectorConfig, nodeConfig, systemConfig);
 * }
 * }
 */
public class WorkerProperty<T1 extends NativeExecutionConnectorConfig, T2 extends NativeExecutionNodeConfig, T3 extends NativeExecutionSystemConfig>
{
    private final T1 connectorConfig;
    private final T2 nodeConfig;
    private final T3 systemConfig;

    public WorkerProperty(
            T1 connectorConfig,
            T2 nodeConfig,
            T3 systemConfig)
    {
        this.systemConfig = requireNonNull(systemConfig, "systemConfig is null");
        this.nodeConfig = requireNonNull(nodeConfig, "nodeConfig is null");
        this.connectorConfig = requireNonNull(connectorConfig, "connectorConfig is null");
    }

    public T1 getConnectorConfig()
    {
        return connectorConfig;
    }

    public T2 getNodeConfig()
    {
        return nodeConfig;
    }

    public T3 getSystemConfig()
    {
        return systemConfig;
    }

    public void populateAllProperties(Path systemConfigPath, Path nodeConfigPath, Path connectorConfigPath)
            throws IOException
    {
        populateProperty(systemConfig.getAllProperties(), systemConfigPath);
        populateProperty(nodeConfig.getAllProperties(), nodeConfigPath);
        populateProperty(connectorConfig.getAllProperties(), connectorConfigPath);
    }

    private void populateProperty(Map<String, String> properties, Path path)
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
