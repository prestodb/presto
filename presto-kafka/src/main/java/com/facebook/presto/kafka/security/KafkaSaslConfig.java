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

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;
import com.facebook.airlift.configuration.ConfigSecuritySensitive;
import com.google.common.collect.ImmutableMap;
import jakarta.validation.constraints.AssertTrue;

import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.kafka.security.KafkaKeystoreTruststoreType.JKS;
import static org.apache.kafka.clients.CommonClientConfigs.SECURITY_PROTOCOL_CONFIG;
import static org.apache.kafka.common.config.SaslConfigs.SASL_JAAS_CONFIG;
import static org.apache.kafka.common.config.SaslConfigs.SASL_MECHANISM;
import static org.apache.kafka.common.config.SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG;
import static org.apache.kafka.common.security.auth.SecurityProtocol.SASL_SSL;

/**
 *  Manages Kafka SASL authentication and encryption between clients and brokers.
 */
public class KafkaSaslConfig
{
    private String saslJaasConfig;
    private String saslMechanism;
    private String truststorePath;
    private String truststorePassword;
    private KafkaKeystoreTruststoreType truststoreType = JKS;

    public Optional<String> getSaslJaasConfig()
    {
        return Optional.ofNullable(saslJaasConfig);
    }

    @Config("kafka.sasl.jaas.config")
    @ConfigDescription("The JAAS config of the SASL authentication")
    public KafkaSaslConfig setSaslJaasConfig(String saslJaasConfig)
    {
        this.saslJaasConfig = saslJaasConfig;
        return this;
    }

    public Optional<String> getSaslMechanism()
    {
        return Optional.ofNullable(saslMechanism);
    }

    @Config("kafka.sasl.mechanism")
    @ConfigDescription("The authentication mechanism of the SASL")
    @ConfigSecuritySensitive
    public KafkaSaslConfig setSaslMechanism(String saslMechanism)
    {
        this.saslMechanism = saslMechanism;
        return this;
    }

    public Optional<String> getTruststorePath()
    {
        return Optional.ofNullable(truststorePath);
    }

    @Config("kafka.truststore.path")
    @ConfigDescription("The path of the trust store file")
    public KafkaSaslConfig setTruststorePath(String truststorePath)
    {
        this.truststorePath = truststorePath;
        return this;
    }

    public Optional<String> getTruststorePassword()
    {
        return Optional.ofNullable(truststorePassword);
    }

    @Config("kafka.truststore.password")
    @ConfigSecuritySensitive
    @ConfigDescription("The password for the trust store file")
    public KafkaSaslConfig setTruststorePassword(String truststorePassword)
    {
        this.truststorePassword = truststorePassword;
        return this;
    }

    public Optional<KafkaKeystoreTruststoreType> getTruststoreType()
    {
        return Optional.ofNullable(truststoreType);
    }

    @Config("kafka.truststore.type")
    @ConfigDescription("The file format of the trust store file")
    public KafkaSaslConfig setTruststoreType(KafkaKeystoreTruststoreType truststoreType)
    {
        this.truststoreType = truststoreType;
        return this;
    }

    public Map<String, Object> getKafkaSaslProperties()
    {
        ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();
        getSaslMechanism().ifPresent(v -> properties.put(SASL_MECHANISM, v));
        getSaslJaasConfig().ifPresent(v -> properties.put(SASL_JAAS_CONFIG, v));
        getTruststorePath().ifPresent(v -> properties.put(SSL_TRUSTSTORE_LOCATION_CONFIG, v));
        getTruststorePassword().ifPresent(v -> properties.put(SSL_TRUSTSTORE_PASSWORD_CONFIG, v));
        getTruststoreType().ifPresent(v -> properties.put(SSL_TRUSTSTORE_TYPE_CONFIG, v.name()));
        properties.put(SECURITY_PROTOCOL_CONFIG, SASL_SSL.name());
        return properties.buildOrThrow();
    }

    @AssertTrue(message = "kafka.sasl.jaas.config must be set when kafka.sasl.mechanism is given")
    public boolean isSaslJaasConfigValid()
    {
        return !getSaslMechanism().isPresent() || getSaslJaasConfig().isPresent();
    }

    @AssertTrue(message = "kafka.ssl.truststore.password must be set when kafka.ssl.truststore.location is given")
    public boolean isTruststorePasswordValid()
    {
        return !getTruststorePath().isPresent() || getTruststorePassword().isPresent();
    }
}
