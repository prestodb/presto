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
import io.airlift.units.DataSize;
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
    public void testNativeExecutionVeloxConfig()
    {
        // Test defaults
        assertRecordedDefaults(ConfigAssertions.recordDefaults(NativeExecutionVeloxConfig.class)
                .setCodegenEnabled(false));

        // Test explicit property mapping. Also makes sure properties returned by getAllProperties() covers full property list.
        NativeExecutionVeloxConfig expected = new NativeExecutionVeloxConfig()
                .setCodegenEnabled(true);
        Map<String, String> properties = expected.getAllProperties();
        assertFullMapping(properties, expected);
    }

    @Test
    public void testNativeExecutionSystemConfig()
    {
        // Test defaults
        assertRecordedDefaults(ConfigAssertions.recordDefaults(NativeExecutionSystemConfig.class)
                .setEnableSerializedPageChecksum(true)
                .setEnableVeloxExpressionLogging(false)
                .setEnableVeloxTaskLogging(true)
                .setHttpServerReusePort(true)
                .setHttpServerPort(7777)
                .setHttpExecThreads(32)
                .setHttpsServerPort(7778)
                .setEnableHttpsCommunication(false)
                .setHttpsCiphers("AES128-SHA,AES128-SHA256,AES256-GCM-SHA384")
                .setHttpsCertPath("")
                .setHttpsKeyPath("")
                .setNumIoThreads(30)
                .setShutdownOnsetSec(10)
                .setSystemMemoryGb(10)
                .setQueryMemoryGb(new DataSize(10, DataSize.Unit.GIGABYTE))
                .setConcurrentLifespansPerTask(5)
                .setMaxDriversPerTask(15)
                .setPrestoVersion("dummy.presto.version")
                .setShuffleName("local")
                .setRegisterTestFunctions(false)
                .setEnableHttpServerAccessLog(true));

        // Test explicit property mapping. Also makes sure properties returned by getAllProperties() covers full property list.
        NativeExecutionSystemConfig expected = new NativeExecutionSystemConfig()
                .setConcurrentLifespansPerTask(15)
                .setEnableSerializedPageChecksum(false)
                .setEnableVeloxExpressionLogging(true)
                .setEnableVeloxTaskLogging(false)
                .setHttpServerReusePort(false)
                .setHttpServerPort(8080)
                .setHttpExecThreads(256)
                .setHttpsServerPort(8081)
                .setEnableHttpsCommunication(true)
                .setHttpsCiphers("AES128-SHA")
                .setHttpsCertPath("/tmp/non_existent.cert")
                .setHttpsKeyPath("/tmp/non_existent.key")
                .setNumIoThreads(50)
                .setPrestoVersion("presto-version")
                .setShutdownOnsetSec(30)
                .setSystemMemoryGb(40)
                .setQueryMemoryGb(new DataSize(20, DataSize.Unit.GIGABYTE))
                .setMaxDriversPerTask(30)
                .setShuffleName("custom")
                .setRegisterTestFunctions(true)
                .setEnableHttpServerAccessLog(false);
        Map<String, String> properties = expected.getAllProperties();
        assertFullMapping(properties, expected);
    }

    @Test
    public void testNativeExecutionNodeConfig()
    {
        // Test defaults
        assertRecordedDefaults(ConfigAssertions.recordDefaults(NativeExecutionNodeConfig.class)
                .setNodeEnvironment("spark-velox")
                .setNodeLocation("/dummy/location")
                .setNodeIp("0.0.0.0")
                .setNodeId(0)
                .setNodeMemoryGb(10));

        // Test explicit property mapping. Also makes sure properties returned by getAllProperties() covers full property list.
        NativeExecutionNodeConfig expected = new NativeExecutionNodeConfig()
                .setNodeEnvironment("next-gen-spark")
                .setNodeId(1)
                .setNodeLocation("/extra/dummy/location")
                .setNodeIp("1.1.1.1")
                .setNodeMemoryGb(40);
        Map<String, String> properties = expected.getAllProperties();
        assertFullMapping(properties, expected);
    }

    @Test
    public void testNativeExecutionConnectorConfig()
    {
        // Test defaults
        assertRecordedDefaults(ConfigAssertions.recordDefaults(NativeExecutionConnectorConfig.class)
                .setCacheEnabled(false)
                .setMaxCacheSize(new DataSize(0, DataSize.Unit.MEGABYTE))
                .setConnectorName("hive"));

        // Test explicit property mapping. Also makes sure properties returned by getAllProperties() covers full property list.
        NativeExecutionConnectorConfig expected = new NativeExecutionConnectorConfig()
                .setConnectorName("custom")
                .setMaxCacheSize(new DataSize(32, DataSize.Unit.MEGABYTE))
                .setCacheEnabled(true);
        Map<String, String> properties = new java.util.HashMap<>(expected.getAllProperties());
        // Since the cache.max-cache-size requires to be size without the unit which to be compatible with the C++,
        // here we convert the size from Long type (in string format) back to DataSize for comparison
        properties.put("cache.max-cache-size", String.valueOf(new DataSize(Double.parseDouble(properties.get("cache.max-cache-size")), DataSize.Unit.MEGABYTE)));
        assertFullMapping(properties, expected);
    }

    @Test
    public void testFilePropertiesPopulator()
    {
        PrestoSparkWorkerProperty workerProperty = new PrestoSparkWorkerProperty(new NativeExecutionConnectorConfig(), new NativeExecutionNodeConfig(), new NativeExecutionSystemConfig(), new NativeExecutionVeloxConfig());
        testPropertiesPopulate(workerProperty);
    }

    private void testPropertiesPopulate(PrestoSparkWorkerProperty workerProperty)
    {
        Path directory = null;
        try {
            directory = Files.createTempDirectory("presto");
            Path veloxPropertiesPath = Paths.get(directory.toString(), "velox.properties");
            Path configPropertiesPath = Paths.get(directory.toString(), "config.properties");
            Path nodePropertiesPath = Paths.get(directory.toString(), "node.properties");
            Path connectorPropertiesPath = Paths.get(directory.toString(), "catalog/hive.properties");
            workerProperty.populateAllProperties(veloxPropertiesPath, configPropertiesPath, nodePropertiesPath, connectorPropertiesPath);

            verifyProperties(workerProperty.getSystemConfig().getAllProperties(), readPropertiesFromDisk(configPropertiesPath));
            verifyProperties(workerProperty.getNodeConfig().getAllProperties(), readPropertiesFromDisk(nodePropertiesPath));
            verifyProperties(workerProperty.getConnectorConfig().getAllProperties(), readPropertiesFromDisk(connectorPropertiesPath));
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
