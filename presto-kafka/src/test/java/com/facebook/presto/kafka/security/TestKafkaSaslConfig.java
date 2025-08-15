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
package com.facebook.presto.kafka.security;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.testng.annotations.Test;

import javax.validation.constraints.AssertTrue;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.AbstractMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static com.facebook.airlift.testing.ValidationAssertions.assertFailsValidation;
import static com.facebook.presto.kafka.security.KafkaKeystoreTruststoreType.JKS;
import static com.facebook.presto.kafka.security.KafkaKeystoreTruststoreType.PKCS12;
import static org.apache.kafka.clients.CommonClientConfigs.SECURITY_PROTOCOL_CONFIG;
import static org.apache.kafka.common.config.SaslConfigs.SASL_JAAS_CONFIG;
import static org.apache.kafka.common.config.SaslConfigs.SASL_MECHANISM;
import static org.apache.kafka.common.config.SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG;
import static org.apache.kafka.common.security.auth.SecurityProtocol.SASL_SSL;
import static org.testng.Assert.assertTrue;

public class TestKafkaSaslConfig
{
    private static final String KAFKA_SASL_JAAS_CONFIG = "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"testuser\" password=\"testpassword\"";
    private static final String KAFKA_SASL_MECHANISM = "SCRAM-SHA-512";
    private static final String KAFKA_TRUSTSTORE_PATH = "/some/path/to/truststore";
    private static final String KAFKA_TRUSTSTORE_PASSWORD = "my-password";
    private static final String KAFKA_TRUSTSTORE_TYPE = "PKCS12";

    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(KafkaSaslConfig.class)
                .setSaslJaasConfig(null)
                .setSaslMechanism(null)
                .setTruststorePath(null)
                .setTruststorePassword(null)
                .setTruststoreType(JKS));
    }

    @Test
    public void testExplicitPropertyMappings()
            throws Exception
    {
        String secret = "confluent";
        Path truststorePath = Files.createTempFile("truststore", ".p12");
        writeToFile(truststorePath, secret);

        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("kafka.sasl.jaas.config", KAFKA_SASL_JAAS_CONFIG)
                .put("kafka.sasl.mechanism", KAFKA_SASL_MECHANISM)
                .put("kafka.truststore.path", truststorePath.toString())
                .put("kafka.truststore.password", KAFKA_TRUSTSTORE_PASSWORD)
                .put("kafka.truststore.type", KAFKA_TRUSTSTORE_TYPE)
                .build();

        KafkaSaslConfig expected = new KafkaSaslConfig()
                .setSaslJaasConfig(KAFKA_SASL_JAAS_CONFIG)
                .setSaslMechanism(KAFKA_SASL_MECHANISM)
                .setTruststorePath(truststorePath.toString())
                .setTruststorePassword(KAFKA_TRUSTSTORE_PASSWORD)
                .setTruststoreType(PKCS12);

        assertFullMapping(properties, expected);
    }

    @Test
    public void testAllConfigPropertiesAreContained()
    {
        KafkaSaslConfig config = new KafkaSaslConfig()
                .setSaslJaasConfig(KAFKA_SASL_JAAS_CONFIG)
                .setSaslMechanism(KAFKA_SASL_MECHANISM)
                .setTruststorePath(KAFKA_TRUSTSTORE_PATH)
                .setTruststorePassword(KAFKA_TRUSTSTORE_PASSWORD)
                .setTruststoreType(JKS);

        Map<String, Object> securityProperties = config.getKafkaSaslProperties();
        // Since security related properties are all passed to the underlying kafka-clients library,
        // the property names must match those expected by kafka-clients
        Map<String, String> map = Stream.of(
                        new AbstractMap.SimpleImmutableEntry<>(SASL_JAAS_CONFIG, KAFKA_SASL_JAAS_CONFIG),
                        new AbstractMap.SimpleImmutableEntry<>(SASL_MECHANISM, KAFKA_SASL_MECHANISM),
                        new AbstractMap.SimpleImmutableEntry<>(SSL_TRUSTSTORE_LOCATION_CONFIG, KAFKA_TRUSTSTORE_PATH),
                        new AbstractMap.SimpleImmutableEntry<>(SSL_TRUSTSTORE_PASSWORD_CONFIG, KAFKA_TRUSTSTORE_PASSWORD),
                        new AbstractMap.SimpleImmutableEntry<>(SSL_TRUSTSTORE_TYPE_CONFIG, JKS.name()),
                        new AbstractMap.SimpleImmutableEntry<>(SECURITY_PROTOCOL_CONFIG, SASL_SSL.name()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        assertTrue(Maps.difference(map, securityProperties).areEqual());
    }

    @Test
    public void testFailOnMissingSaslJaasConfigWithSaslMechanism()
    {
        KafkaSaslConfig config = new KafkaSaslConfig();
        config.setSaslMechanism(KAFKA_SASL_MECHANISM);

        assertFailsValidation(
                config,
                "saslJaasConfigValid",
                "kafka.sasl.jaas.config must be set when kafka.sasl.mechanism is given",
                AssertTrue.class);
    }

    @Test
    public void testFailOnMissingTruststorePasswordWithTruststoreLocationSet()
            throws Exception
    {
        String secret = "confluent";
        Path truststorePath = Files.createTempFile("truststore", ".p12");

        writeToFile(truststorePath, secret);

        KafkaSaslConfig config = new KafkaSaslConfig();
        config.setTruststorePath(truststorePath.toString());

        assertFailsValidation(
                config,
                "truststorePasswordValid",
                "kafka.ssl.truststore.password must be set when kafka.ssl.truststore.location is given",
                AssertTrue.class);
    }

    private void writeToFile(Path filepath, String content)
            throws IOException
    {
        try (FileWriter writer = new FileWriter(filepath.toFile())) {
            writer.write(content);
        }
    }
}
