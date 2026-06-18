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
package com.facebook.presto.kafka;

import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.testing.TestingConnectorContext;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static com.facebook.airlift.testing.Assertions.assertInstanceOf;
import static com.google.common.collect.Iterables.getOnlyElement;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.testng.Assert.assertNotNull;

@Test
public class TestKafkaPlugin
{
    @Test
    public void testSpinup()
    {
        KafkaPlugin plugin = new KafkaPlugin();

        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
        assertInstanceOf(factory, KafkaConnectorFactory.class);

        Connector c = factory.create(
                "test-connector",
                ImmutableMap.<String, String>builder()
                        .put("kafka.table-names", "test")
                        .put("kafka.nodes", "localhost:9092")
                        .build(),
                new TestingConnectorContext());
        assertNotNull(c);
    }

    @Test
    public void testSslSpinup()
            throws IOException
    {
        KafkaPlugin plugin = new KafkaPlugin();

        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
        assertThat(factory).isInstanceOf(KafkaConnectorFactory.class);

        String secret = "confluent";
        Path truststorePath = Files.createTempFile("truststore", ".jks");

        writeToFile(truststorePath, secret);

        Connector connector = factory.create(
                "test-connector",
                ImmutableMap.<String, String>builder()
                        .put("kafka.table-names", "test")
                        .put("kafka.nodes", "localhost:9092")
                        .put("kafka.security-protocol", "SASL_SSL")
                        .put("kafka.sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"testuser\" password=\"testpassword\"")
                        .put("kafka.sasl.mechanism", "SCRAM-SHA-512")
                        .put("kafka.truststore.type", "JKS")
                        .put("kafka.truststore.path", truststorePath.toString())
                        .put("kafka.truststore.password", "truststore-password")
                        .buildOrThrow(),
                new TestingConnectorContext());
        assertThat(connector).isNotNull();
        connector.shutdown();
    }

    @Test
    public void testConfigResourceSpinup()
            throws IOException
    {
        KafkaPlugin plugin = new KafkaPlugin();

        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
        assertThat(factory).isInstanceOf(KafkaConnectorFactory.class);

        String nativeContent = "security.protocol=SSL";
        Path nativeKafkaResourcePath = Files.createTempFile("native_kafka", ".properties");
        writeToFile(nativeKafkaResourcePath, nativeContent);

        Connector connector = factory.create(
                "test-connector",
                ImmutableMap.<String, String>builder()
                        .put("kafka.table-names", "test")
                        .put("kafka.nodes", "localhost:9092")
                        .put("kafka.config.resources", nativeKafkaResourcePath.toString())
                        .buildOrThrow(),
                new TestingConnectorContext());
        assertThat(connector).isNotNull();
        connector.shutdown();
    }

    @Test
    public void testResourceConfigMissingFileSpindown()
    {
        KafkaPlugin plugin = new KafkaPlugin();

        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
        assertThat(factory).isInstanceOf(KafkaConnectorFactory.class);

        assertThatThrownBy(() -> factory.create(
                "test-connector",
                ImmutableMap.<String, String>builder()
                        .put("kafka.table-names", "test")
                        .put("kafka.nodes", "localhost:9092")
                        .put("kafka.security-protocol", "SASL_PLAINTEXT")
                        .put("kafka.config.resources", "/not/a/real/path")
                        .buildOrThrow(),
                new TestingConnectorContext()))
                .hasMessageContaining("IllegalArgumentException: File does not exist: /not/a/real/path");
    }

    private void writeToFile(Path filepath, String content)
            throws IOException
    {
        try (FileWriter writer = new FileWriter(filepath.toFile())) {
            writer.write(content);
        }
    }
}
