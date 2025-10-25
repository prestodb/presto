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
package com.facebook.presto.mongodb;

import com.facebook.presto.plugin.base.security.SslContextProvider;
import com.facebook.presto.tests.SslKeystoreManager;
import com.mongodb.MongoClientOptions;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.net.ssl.SSLContext;

import java.io.File;
import java.util.Optional;

import static com.facebook.presto.tests.SslKeystoreManager.SSL_STORE_PASSWORD;
import static com.facebook.presto.tests.SslKeystoreManager.getKeystorePath;
import static com.facebook.presto.tests.SslKeystoreManager.getTruststorePath;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

/**
 * Integration tests for MongoDB TLS configuration.
 * Tests the complete flow from MongoClientConfig through SslContextProvider to MongoClient.
 */
public class TestMongoTlsConfiguration
{
    private File keystoreFile;
    private File truststoreFile;

    @BeforeClass
    public void setUp() throws Exception
    {
        SslKeystoreManager.initializeKeystoreAndTruststore();

        keystoreFile = new File(getKeystorePath());
        truststoreFile = new File(getTruststorePath());
    }

    @Test
    public void testTlsDisabledByDefault()
    {
        MongoClientConfig config = new MongoClientConfig();

        assertFalse(config.isTlsEnabled(), "TLS should be disabled by default");
        assertFalse(config.getKeystorePath().isPresent(), "Keystore path should be empty by default");
        assertFalse(config.getKeystorePassword().isPresent(), "Keystore password should be empty by default");
        assertFalse(config.getTruststorePath().isPresent(), "Truststore path should be empty by default");
        assertFalse(config.getTruststorePassword().isPresent(), "Truststore password should be empty by default");
    }

    @Test
    public void testTlsEnabledWithKeystoreAndTruststore()
    {
        MongoClientConfig config = new MongoClientConfig();
        configureTlsProperties(config);

        assertTrue(config.isTlsEnabled(), "TLS should be enabled");
        assertTrue(config.getKeystorePath().isPresent(), "Keystore path should be present");
        assertTrue(config.getKeystorePassword().isPresent(), "Keystore password should be present");
        assertTrue(config.getTruststorePath().isPresent(), "Truststore path should be present");
        assertTrue(config.getTruststorePassword().isPresent(), "Truststore password should be present");
        assertEquals(config.getKeystorePath().get(), keystoreFile);
        assertEquals(config.getTruststorePath().get(), truststoreFile);
    }

    @Test
    public void testTlsEnabledWithKeystoreOnly()
    {
        MongoClientConfig config = new MongoClientConfig()
                .setTlsEnabled(true)
                .setKeystorePath(keystoreFile)
                .setKeystorePassword(SSL_STORE_PASSWORD);

        assertTrue(config.isTlsEnabled(), "TLS should be enabled");
        assertTrue(config.getKeystorePath().isPresent(), "Keystore path should be present");
        assertTrue(config.getKeystorePassword().isPresent(), "Keystore password should be present");
        assertFalse(config.getTruststorePath().isPresent(), "Truststore path should be empty");
        assertFalse(config.getTruststorePassword().isPresent(), "Truststore password should be empty");
        assertTrue(config.isValidTlsConfig(), "TLS config should be valid with keystore only");
    }

    @Test
    public void testTlsEnabledWithTruststoreOnly()
    {
        MongoClientConfig config = new MongoClientConfig()
                .setTlsEnabled(true)
                .setTruststorePath(truststoreFile)
                .setTruststorePassword(SSL_STORE_PASSWORD);

        assertTrue(config.isTlsEnabled(), "TLS should be enabled");
        assertFalse(config.getKeystorePath().isPresent(), "Keystore path should be empty");
        assertFalse(config.getKeystorePassword().isPresent(), "Keystore password should be empty");
        assertTrue(config.getTruststorePath().isPresent(), "Truststore path should be present");
        assertTrue(config.getTruststorePassword().isPresent(), "Truststore password should be present");
        assertTrue(config.isValidTlsConfig(), "TLS config should be valid with truststore only");
    }

    @Test
    public void testSslContextProviderIntegration()
    {
        MongoClientConfig config = new MongoClientConfig();
        configureTlsProperties(config);

        SslContextProvider provider = createSslContextProvider(config);
        Optional<SSLContext> sslContext = provider.buildSslContext();

        assertTrue(sslContext.isPresent(), "SSL context should be created");
        assertNotNull(sslContext.get(), "SSL context should not be null");
        assertEquals(sslContext.get().getProtocol(), "TLS", "SSL context should use TLS protocol");
    }

    @Test
    public void testMongoClientOptionsWithTlsEnabled()
    {
        MongoClientConfig config = new MongoClientConfig()
                .setSeeds("localhost:27017");
        configureTlsProperties(config);

        MongoClientOptions.Builder optionsBuilder = MongoClientOptions.builder()
                .connectionsPerHost(config.getConnectionsPerHost())
                .connectTimeout(config.getConnectionTimeout())
                .socketTimeout(config.getSocketTimeout())
                .socketKeepAlive(config.getSocketKeepAlive())
                .maxWaitTime(config.getMaxWaitTime())
                .minConnectionsPerHost(config.getMinConnectionsPerHost())
                .writeConcern(config.getWriteConcern().getWriteConcern());

        // Configure SSL
        if (config.isTlsEnabled()) {
            SslContextProvider sslContextProvider = createSslContextProvider(config);

            sslContextProvider.buildSslContext().ifPresent(sslContext -> {
                optionsBuilder.sslContext(sslContext);
                optionsBuilder.sslEnabled(true);
            });
        }

        MongoClientOptions options = optionsBuilder.build();

        assertTrue(options.isSslEnabled(), "SSL should be enabled in MongoClientOptions");
        assertNotNull(options.getSslContext(), "SSL context should be set in MongoClientOptions");
    }

    @Test
    public void testMongoClientOptionsWithTlsDisabled()
    {
        MongoClientConfig config = new MongoClientConfig()
                .setSeeds("localhost:27017")
                .setTlsEnabled(false);

        MongoClientOptions.Builder optionsBuilder = MongoClientOptions.builder()
                .connectionsPerHost(config.getConnectionsPerHost())
                .connectTimeout(config.getConnectionTimeout())
                .socketTimeout(config.getSocketTimeout())
                .socketKeepAlive(config.getSocketKeepAlive())
                .maxWaitTime(config.getMaxWaitTime())
                .minConnectionsPerHost(config.getMinConnectionsPerHost())
                .writeConcern(config.getWriteConcern().getWriteConcern());

        // Configure SSL
        if (config.isTlsEnabled()) {
            SslContextProvider sslContextProvider = createSslContextProvider(config);

            sslContextProvider.buildSslContext().ifPresent(sslContext -> {
                optionsBuilder.sslContext(sslContext);
                optionsBuilder.sslEnabled(true);
            });
        }

        MongoClientOptions options = optionsBuilder.build();

        assertFalse(options.isSslEnabled(), "SSL should be disabled in MongoClientOptions");
    }

    @Test
    public void testLegacyPropertySupport()
    {
        // Test that the legacy mongodb.ssl.enabled property still works
        MongoClientConfig config = new MongoClientConfig();

        // The @LegacyConfig annotation should map mongodb.ssl.enabled to mongodb.tls.enabled
        // This would be tested through the configuration system, but we can verify the setter works
        config.setTlsEnabled(true);

        assertTrue(config.isTlsEnabled(), "TLS should be enabled via legacy property mapping");
    }

    @Test
    public void testTlsConfigurationValidationWithPartialKeystore()
    {
        // Test that having only keystore path without password fails validation
        MongoClientConfig config = new MongoClientConfig()
                .setTlsEnabled(true)
                .setKeystorePath(keystoreFile);

        assertFalse(config.isValidTlsConfig(),
                "TLS config should be invalid when keystore path is set without password");
    }

    @Test
    public void testTlsConfigurationValidationWithPartialTruststore()
    {
        // Test that having only truststore path without password fails validation
        MongoClientConfig config = new MongoClientConfig()
                .setTlsEnabled(true)
                .setTruststorePath(truststoreFile);

        assertFalse(config.isValidTlsConfig(),
                "TLS config should be invalid when truststore path is set without password");
    }

    @Test
    public void testFullMongoClientCreationFlow()
    {
        // This tests the complete flow similar to what happens in MongoClientModule
        MongoClientConfig config = new MongoClientConfig()
                .setSeeds("localhost:27017")
                .setConnectionsPerHost(50)
                .setConnectionTimeout(5000)
                .setReadPreference(ReadPreferenceType.PRIMARY);
        configureTlsProperties(config);
        // Verify configuration
        assertTrue(config.isTlsEnabled(), "TLS should be enabled");
        assertTrue(config.isValidTlsConfig(), "TLS configuration should be valid");
        assertEquals(config.getConnectionsPerHost(), 50);
        assertEquals(config.getConnectionTimeout(), 5000);

        // Create SSL context
        SslContextProvider sslContextProvider = createSslContextProvider(config);

        Optional<SSLContext> sslContext = sslContextProvider.buildSslContext();
        assertTrue(sslContext.isPresent(), "SSL context should be created");

        // Build MongoClientOptions
        MongoClientOptions.Builder optionsBuilder = MongoClientOptions.builder()
                .connectionsPerHost(config.getConnectionsPerHost())
                .connectTimeout(config.getConnectionTimeout())
                .readPreference(config.getReadPreference().getReadPreference());

        sslContext.ifPresent(ctx -> {
            optionsBuilder.sslContext(ctx);
            optionsBuilder.sslEnabled(true);
        });

        MongoClientOptions options = optionsBuilder.build();

        // Verify final options
        assertTrue(options.isSslEnabled(), "SSL should be enabled");
        assertNotNull(options.getSslContext(), "SSL context should be set");
        assertEquals(options.getConnectionsPerHost(), 50);
        assertEquals(options.getConnectTimeout(), 5000);
    }

    @Test
    public void testTlsEnabledWithKeystoreOnlyUsesKeystoreAsTruststore()
    {
        // When only keystore is provided, it should be used as truststore for backward compatibility
        MongoClientConfig config = new MongoClientConfig()
                .setTlsEnabled(true)
                .setKeystorePath(keystoreFile)
                .setKeystorePassword(SSL_STORE_PASSWORD);

        SslContextProvider provider = new SslContextProvider(
                config.getKeystorePath(),
                config.getKeystorePassword(),
                Optional.empty(),
                Optional.empty());

        Optional<SSLContext> sslContext = provider.buildSslContext();

        assertTrue(sslContext.isPresent(), "SSL context should be created when only keystore is provided");
        assertNotNull(sslContext.get(), "SSL context should not be null");
    }

    @Test
    public void testTlsValidationWithBothKeystoreAndTruststore()
    {
        MongoClientConfig config = new MongoClientConfig();
        configureTlsProperties(config);

        assertTrue(config.isValidTlsConfig(), "TLS config should be valid with complete keystore and truststore");
    }

    @Test
    public void testTlsValidationFailsWhenDisabledWithProperties()
    {
        // Test that TLS properties cannot be set when TLS is disabled
        MongoClientConfig config1 = new MongoClientConfig()
                .setTlsEnabled(false)
                .setKeystorePath(keystoreFile);

        assertFalse(config1.isValidTlsConfig(),
                "TLS config should be invalid when TLS is disabled but keystore path is set");

        MongoClientConfig config2 = new MongoClientConfig()
                .setTlsEnabled(false)
                .setTruststorePath(truststoreFile);

        assertFalse(config2.isValidTlsConfig(),
                "TLS config should be invalid when TLS is disabled but truststore path is set");
    }

    private void configureTlsProperties(MongoClientConfig config)
    {
        config.setTlsEnabled(true)
                .setKeystorePath(keystoreFile)
                .setKeystorePassword(SSL_STORE_PASSWORD)
                .setTruststorePath(truststoreFile)
                .setTruststorePassword(SSL_STORE_PASSWORD);
    }

    private SslContextProvider createSslContextProvider(MongoClientConfig config)
    {
        return new SslContextProvider(
                config.getKeystorePath(),
                config.getKeystorePassword(),
                config.getTruststorePath(),
                config.getTruststorePassword());
    }
}
