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

import com.facebook.airlift.configuration.testing.ConfigAssertions;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Properties;

import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestNativeExecutionSystemConfig
{
    @Test
    public void testNativeExecutionSystemConfig()
    {
        // Test defaults
        NativeExecutionSystemConfig nativeExecutionSystemConfig = new NativeExecutionSystemConfig(
                ImmutableMap.of());
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        ImmutableMap<String, String> expectedConfigs = builder
                .put("concurrent-lifespans-per-task", "5")
                .put("enable-serialized-page-checksum", "true")
                .put("enable_velox_expression_logging", "false")
                .put("enable_velox_task_logging", "true")
                .put("http-server.http.port", "7777")
                .put("http-server.reuse-port", "true")
                .put("http-server.bind-to-node-internal-address-only-enabled", "true")
                .put("http-server.https.port", "7778")
                .put("http-server.https.enabled", "false")
                .put("https-supported-ciphers", "AES128-SHA,AES128-SHA256,AES256-GCM-SHA384")
                .put("https-cert-path", "")
                .put("https-key-path", "")
                .put("http-server.num-io-threads-hw-multiplier", "1.0")
                .put("exchange.http-client.num-io-threads-hw-multiplier", "1.0")
                .put("async-data-cache-enabled", "false")
                .put("async-cache-ssd-gb", "0")
                .put("connector.num-io-threads-hw-multiplier", "0")
                .put("presto.version", "dummy.presto.version")
                .put("shutdown-onset-sec", "10")
                .put("system-memory-gb", "10")
                .put("query-memory-gb", "8")
                .put("query.max-memory-per-node", "8GB")
                .put("use-mmap-allocator", "true")
                .put("memory-arbitrator-kind", "SHARED")
                .put("shared-arbitrator.reserved-capacity", "0GB")
                .put("shared-arbitrator.memory-pool-initial-capacity", "4GB")
                .put("shared-arbitrator.max-memory-arbitration-time", "5m")
                .put("experimental.spiller-spill-path", "")
                .put("task.max-drivers-per-task", "15")
                .put("enable-old-task-cleanup", "false")
                .put("shuffle.name", "local")
                .put("http-server.enable-access-log", "true")
                .put("core-on-allocation-failure-enabled", "false")
                .put("spill-enabled", "true")
                .put("aggregation-spill-enabled", "true")
                .put("join-spill-enabled", "true")
                .put("order-by-spill-enabled", "true")
                .put("max-spill-bytes", String.valueOf(600L << 30))
                .build();
        assertEquals(nativeExecutionSystemConfig.getAllProperties(), expectedConfigs);

        // Test explicit property mapping. Also makes sure properties returned by getAllProperties() covers full property list.
        builder = ImmutableMap.builder();
        ImmutableMap<String, String> systemConfig = builder
                .put("concurrent-lifespans-per-task", "15")
                .put("enable-serialized-page-checksum", "false")
                .put("enable_velox_expression_logging", "true")
                .put("enable_velox_task_logging", "false")
                .put("http-server.http.port", "8777")
                .put("http-server.reuse-port", "false")
                .put("http-server.bind-to-node-internal-address-only-enabled", "false")
                .put("http-server.https.port", "8778")
                .put("http-server.https.enabled", "true")
                .put("https-supported-ciphers", "override-cipher")
                .put("https-cert-path", "/override/path/cert")
                .put("https-key-path", "/override/path/key")
                .put("http-server.num-io-threads-hw-multiplier", "1.5")
                .put("exchange.http-client.num-io-threads-hw-multiplier", "1.5")
                .put("async-data-cache-enabled", "true")
                .put("async-cache-ssd-gb", "1")
                .put("connector.num-io-threads-hw-multiplier", "0.1")
                .put("presto.version", "override.presto.version")
                .put("shutdown-onset-sec", "15")
                .put("system-memory-gb", "5")
                .put("query-memory-gb", "4")
                .put("query.max-memory-per-node", "4GB")
                .put("use-mmap-allocator", "false")
                .put("memory-arbitrator-kind", "NOOP")
                .put("shared-arbitrator.reserved-capacity", "1GB")
                .put("shared-arbitrator.memory-pool-initial-capacity", "1GB")
                .put("shared-arbitrator.max-memory-arbitration-time", "1s")
                .put("experimental.spiller-spill-path", "/abc")
                .put("task.max-drivers-per-task", "25")
                .put("enable-old-task-cleanup", "true")
                .put("shuffle.name", "remote")
                .put("http-server.enable-access-log", "false")
                .put("core-on-allocation-failure-enabled", "true")
                .put("spill-enabled", "false")
                .put("aggregation-spill-enabled", "false")
                .put("join-spill-enabled", "false")
                .put("order-by-spill-enabled", "false")
                .put("non-defined-property-key-0", "non-defined-property-value-0")
                .put("non-defined-property-key-1", "non-defined-property-value-1")
                .put("non-defined-property-key-2", "non-defined-property-value-2")
                .put("non-defined-property-key-3", "non-defined-property-value-3")
                .build();
        nativeExecutionSystemConfig = new NativeExecutionSystemConfig(systemConfig);

        builder = ImmutableMap.builder();
        expectedConfigs = builder
                .put("concurrent-lifespans-per-task", "15")
                .put("enable-serialized-page-checksum", "false")
                .put("enable_velox_expression_logging", "true")
                .put("enable_velox_task_logging", "false")
                .put("http-server.http.port", "8777")
                .put("http-server.reuse-port", "false")
                .put("http-server.bind-to-node-internal-address-only-enabled", "false")
                .put("http-server.https.port", "8778")
                .put("http-server.https.enabled", "true")
                .put("https-supported-ciphers", "override-cipher")
                .put("https-cert-path", "/override/path/cert")
                .put("https-key-path", "/override/path/key")
                .put("http-server.num-io-threads-hw-multiplier", "1.5")
                .put("exchange.http-client.num-io-threads-hw-multiplier", "1.5")
                .put("async-data-cache-enabled", "true")
                .put("async-cache-ssd-gb", "1")
                .put("connector.num-io-threads-hw-multiplier", "0.1")
                .put("presto.version", "override.presto.version")
                .put("shutdown-onset-sec", "15")
                .put("system-memory-gb", "5")
                .put("query-memory-gb", "4")
                .put("query.max-memory-per-node", "4GB")
                .put("use-mmap-allocator", "false")
                .put("memory-arbitrator-kind", "NOOP")
                .put("shared-arbitrator.reserved-capacity", "1GB")
                .put("shared-arbitrator.memory-pool-initial-capacity", "1GB")
                .put("shared-arbitrator.max-memory-arbitration-time", "1s")
                .put("experimental.spiller-spill-path", "/abc")
                .put("task.max-drivers-per-task", "25")
                .put("enable-old-task-cleanup", "true")
                .put("shuffle.name", "remote")
                .put("http-server.enable-access-log", "false")
                .put("core-on-allocation-failure-enabled", "true")
                .put("spill-enabled", "false")
                .put("aggregation-spill-enabled", "false")
                .put("join-spill-enabled", "false")
                .put("order-by-spill-enabled", "false")
                .put("non-defined-property-key-0", "non-defined-property-value-0")
                .put("non-defined-property-key-1", "non-defined-property-value-1")
                .put("non-defined-property-key-2", "non-defined-property-value-2")
                .put("non-defined-property-key-3", "non-defined-property-value-3")
                .put("max-spill-bytes", String.valueOf(600L << 30)) // default spill bytes
                .build();

        assertEquals(nativeExecutionSystemConfig.getAllProperties(), expectedConfigs);
    }

    @Test
    public void testNativeExecutionNodeConfig()
    {
        // Test defaults
        assertRecordedDefaults(ConfigAssertions.recordDefaults(NativeExecutionNodeConfig.class)
                .setNodeEnvironment("spark-velox")
                .setNodeLocation("/dummy/location")
                .setNodeInternalAddress("127.0.0.1")
                .setNodeId(0)
                .setNodeMemoryGb(10));

        // Test explicit property mapping. Also makes sure properties returned by getAllProperties() covers full property list.
        NativeExecutionNodeConfig expected = new NativeExecutionNodeConfig()
                .setNodeEnvironment("next-gen-spark")
                .setNodeId(1)
                .setNodeLocation("/extra/dummy/location")
                .setNodeInternalAddress("1.1.1.1")
                .setNodeMemoryGb(40);
        Map<String, String> properties = expected.getAllProperties();
        assertFullMapping(properties, expected);
    }

    @Test
    public void testNativeExecutionCatalogProperties()
    {
        // Test default constructor
        NativeExecutionCatalogProperties config = new NativeExecutionCatalogProperties(ImmutableMap.of());
        assertEquals(config.getAllCatalogProperties(), ImmutableMap.of());

        // Test constructor with catalog properties
        Map<String, Map<String, String>> catalogProperties = ImmutableMap.of(
                "hive", ImmutableMap.of("hive.metastore.uri", "thrift://localhost:9083"),
                "tpch", ImmutableMap.of("tpch.splits-per-node", "4"));
        NativeExecutionCatalogProperties configWithProps = new NativeExecutionCatalogProperties(catalogProperties);
        assertEquals(configWithProps.getAllCatalogProperties(), catalogProperties);
    }

    @Test
    public void testFilePropertiesPopulator()
    {
        PrestoSparkWorkerProperty workerProperty = new PrestoSparkWorkerProperty(
                new NativeExecutionCatalogProperties(ImmutableMap.of()), new NativeExecutionNodeConfig(),
                new NativeExecutionSystemConfig(ImmutableMap.of()));
        testPropertiesPopulate(workerProperty);
    }

    private void testPropertiesPopulate(PrestoSparkWorkerProperty workerProperty)
    {
        Path directory = null;
        try {
            directory = Files.createTempDirectory("presto");
            Path configPropertiesPath = Paths.get(directory.toString(), "config.properties");
            Path nodePropertiesPath = Paths.get(directory.toString(), "node.properties");
            Path catalogDirectory = Paths.get(directory.toString(), "catalog");  // Directory path for catalogs
            workerProperty.populateAllProperties(configPropertiesPath, nodePropertiesPath, catalogDirectory);

            verifyProperties(workerProperty.getSystemConfig().getAllProperties(), readPropertiesFromDisk(configPropertiesPath));
            verifyProperties(workerProperty.getNodeConfig().getAllProperties(), readPropertiesFromDisk(nodePropertiesPath));
            // Verify each catalog file was created properly
            workerProperty.getCatalogProperties().getAllCatalogProperties().forEach(
                    (catalogName, catalogProperties) -> {
                        Path catalogFilePath = catalogDirectory.resolve(catalogName + ".properties");
                        verifyProperties(catalogProperties, readPropertiesFromDisk(catalogFilePath));
                    });
        }
        catch (Exception exception) {
            exception.printStackTrace();
            fail();
        }
    }

    private Properties readPropertiesFromDisk(Path path)
    {
        Properties properties = new Properties();
        File file = new File(path.toString());
        try {
            FileReader fileReader = new FileReader(file);
            properties.load(fileReader);
            fileReader.close();
            return properties;
        }
        catch (IOException e) {
            e.printStackTrace();
            fail();
        }
        return null;
    }

    private void verifyProperties(Map<String, String> expected, Properties actual)
    {
        for (Map.Entry<String, String> entry : expected.entrySet()) {
            assertEquals(entry.getValue(), actual.get(entry.getKey()));
        }
        for (Map.Entry<Object, Object> entry : actual.entrySet()) {
            assertEquals((String) entry.getValue(), expected.get((String) entry.getKey()));
        }
    }
}
