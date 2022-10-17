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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestProperties
{
    @Test
    public void testFilePropertiesPopulator()
    {
        Path directory = null;
        try {
            directory = Files.createTempDirectory("presto");
        }
        catch (IOException e) {
            e.printStackTrace();
            fail();
        }
        Path configPropertiesPath = Paths.get(directory.toString(), "config.properties");
        Map<String, String> workerConfigProperties = createSampleConfigProperties(configPropertiesPath);
        testPropertiesPopulate(workerConfigProperties, configPropertiesPath);

        Path nodePropertiesPath = Paths.get(directory.toString(), "node.properties");
        Map<String, String> nodeConfigProperties = createSampleNodeProperties(nodePropertiesPath);
        testPropertiesPopulate(nodeConfigProperties, nodePropertiesPath);

        Path connectorPropertiesPath = Paths.get(directory.toString(), "catalog/hive.properties");
        Map<String, String> connectorProperties = createSampleConnectorProperties(connectorPropertiesPath);
        testPropertiesPopulate(connectorProperties, connectorPropertiesPath);
    }

    private void testPropertiesPopulate(Map<String, String> properties, Path populatePath)
    {
        try {
            WorkerProperty.populateProperty(properties, populatePath);
        }
        catch (Exception exception) {
            exception.printStackTrace();
            fail();
        }
        Properties actual = readPropertiesFromDisk(populatePath);
        verifyProperties(properties, actual);
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

    private Map<String, String> createSampleConfigProperties(Path propertyPath)
    {
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        builder.put("concurrent-lifespans-per-task", "5")
                .put("enable-serialized-page-checksum", "true")
                .put("enable_velox_expression_logging", "false")
                .put("enable_velox_task_logging", "true")
                .put("http-server.http.port", "7777")
                .put("http_exec_threads", "128")
                .put("num-io-threads", "30")
                .put("presto.version", "presto-version")
                .put("shutdown-onset-sec", "10")
                .put("system-memory-gb", "10")
                .put("task.max-drivers-per-task", "15")
                .put("discovery.uri", "http://127.0.0.1");
        return builder.build();
    }

    private Map<String, String> createSampleNodeProperties(Path propertyPath)
    {
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        builder.put("node.environment", "Sapphire-Velox")
                .put("node.id", "0")
                .put("node.location", "/dummy/location")
                .put("node.ip", "0.0.0.0")
                .put("node.memory_gb", "10");
        return builder.build();
    }

    private Map<String, String> createSampleConnectorProperties(Path propertyPath)
    {
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        builder.put("cache.enabled", "false")
                .put("cache.max-cache-size", "0")
                .put("connector.name", "hive");
        return builder.build();
    }
}
