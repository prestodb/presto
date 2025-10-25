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

import com.facebook.airlift.configuration.testing.ConfigAssertions;
import com.google.common.collect.ImmutableMap;
import com.mongodb.MongoCredential;
import jakarta.validation.constraints.AssertTrue;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static com.facebook.airlift.testing.ValidationAssertions.assertFailsValidation;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestMongoClientConfig
{
    private static Path keystoreFile;
    private static Path truststoreFile;

    @BeforeClass
    public void setUp()
            throws IOException
    {
        keystoreFile = Files.createTempFile("test-keystore", ".jks");
        truststoreFile = Files.createTempFile("test-truststore", ".jks");
    }

    @AfterClass
    public void tearDown()
            throws IOException
    {
        if (keystoreFile != null) {
            Files.deleteIfExists(keystoreFile);
        }
        if (truststoreFile != null) {
            Files.deleteIfExists(truststoreFile);
        }
    }

    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(MongoClientConfig.class)
                .setSchemaCollection("_schema")
                .setSeeds("")
                .setCredentials("")
                .setMinConnectionsPerHost(0)
                .setConnectionsPerHost(100)
                .setMaxWaitTime(120_000)
                .setConnectionTimeout(10_000)
                .setSocketTimeout(0)
                .setSocketKeepAlive(false)
                .setTlsEnabled(false)
                .setKeystorePath(null)
                .setKeystorePassword(null)
                .setTruststorePath(null)
                .setTruststorePassword(null)
                .setCursorBatchSize(0)
                .setReadPreference(ReadPreferenceType.PRIMARY)
                .setReadPreferenceTags("")
                .setWriteConcern(WriteConcernType.ACKNOWLEDGED)
                .setRequiredReplicaSetName(null)
                .setImplicitRowFieldPrefix("_pos")
                .setCaseSensitiveNameMatchingEnabled(false));
    }

    @Test
    public void testExplicitPropertyMappings() throws IOException
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("mongodb.schema-collection", "_my_schema")
                .put("mongodb.seeds", "host1,host2:27016")
                .put("mongodb.credentials", "username:password@collection")
                .put("mongodb.min-connections-per-host", "1")
                .put("mongodb.connections-per-host", "99")
                .put("mongodb.max-wait-time", "120001")
                .put("mongodb.connection-timeout", "9999")
                .put("mongodb.socket-timeout", "1")
                .put("mongodb.socket-keep-alive", "true")
                .put("mongodb.tls.enabled", "true") // Use the primary TLS config
                .put("mongodb.tls.keystore-path", keystoreFile.toString())
                .put("mongodb.tls.keystore-password", "keystore-password")
                .put("mongodb.tls.truststore-path", truststoreFile.toString())
                .put("mongodb.tls.truststore-password", "truststore-password")
                .put("mongodb.cursor-batch-size", "1")
                .put("mongodb.read-preference", "NEAREST")
                .put("mongodb.read-preference-tags", "tag_name:tag_value")
                .put("mongodb.write-concern", "UNACKNOWLEDGED")
                .put("mongodb.required-replica-set", "replica_set")
                .put("mongodb.implicit-row-field-prefix", "_prefix")
                .put("case-sensitive-name-matching", "true")
                .build();

        MongoClientConfig expected = new MongoClientConfig()
                .setSchemaCollection("_my_schema")
                .setSeeds("host1", "host2:27016")
                .setCredentials("username:password@collection")
                .setMinConnectionsPerHost(1)
                .setConnectionsPerHost(99)
                .setMaxWaitTime(120_001)
                .setConnectionTimeout(9_999)
                .setSocketTimeout(1)
                .setSocketKeepAlive(true);

        configureTlsProperties(expected, "keystore-password", "truststore-password");

        expected.setCursorBatchSize(1)
                .setReadPreference(ReadPreferenceType.NEAREST)
                .setReadPreferenceTags("tag_name:tag_value")
                .setWriteConcern(WriteConcernType.UNACKNOWLEDGED)
                .setRequiredReplicaSetName("replica_set")
                .setImplicitRowFieldPrefix("_prefix")
                .setCaseSensitiveNameMatchingEnabled(true);

        ConfigAssertions.assertFullMapping(properties, expected);
    }

    @Test
    public void testTlsConfigurationWithAllProperties() throws IOException
    {
        MongoClientConfig config = new MongoClientConfig();
        configureTlsProperties(config, "keystore-password", "truststore-password");

        assertTrue(config.isTlsEnabled(), "TLS should be enabled when explicitly set to true");
        assertEquals(config.getKeystorePath().get(), keystoreFile.toFile());
        assertEquals(config.getKeystorePassword().get(), "keystore-password");
        assertEquals(config.getTruststorePath().get(), truststoreFile.toFile());
        assertEquals(config.getTruststorePassword().get(), "truststore-password");
    }

    @Test
    public void testSpecialCharacterCredential()
    {
        MongoClientConfig config = new MongoClientConfig()
                .setCredentials("username:P@ss:w0rd@database");

        MongoCredential credential = config.getCredentials().get(0);
        MongoCredential expected = MongoCredential.createCredential("username", "database", "P@ss:w0rd".toCharArray());
        assertEquals(credential, expected);
    }

    @Test
    public void testTlsPropertyValidationFailsIfTlsIsDisabled()
            throws Exception
    {
        assertFailsTlsValidation(new MongoClientConfig().setKeystorePath(keystoreFile.toFile()));
        assertFailsTlsValidation(new MongoClientConfig().setKeystorePassword("keystore password"));
        assertFailsTlsValidation(new MongoClientConfig().setTruststorePath(truststoreFile.toFile()));
        assertFailsTlsValidation(new MongoClientConfig().setTruststorePassword("truststore password"));
    }

    @Test
    public void testTlsPropertyValidationPassesIfTlsIsEnabled()
            throws Exception
    {
        // These should all pass validation when TLS is enabled
        MongoClientConfig config1 = new MongoClientConfig()
                .setTlsEnabled(true)
                .setKeystorePath(keystoreFile.toFile())
                .setKeystorePassword("keystore password");
        assertTrue(config1.isValidTlsConfig());

        MongoClientConfig config2 = new MongoClientConfig()
                .setTlsEnabled(true)
                .setTruststorePath(truststoreFile.toFile())
                .setTruststorePassword("truststore password");
        assertTrue(config2.isValidTlsConfig());

        MongoClientConfig config3 = new MongoClientConfig();
        configureTlsProperties(config3, "keystore password", "truststore password");
        assertTrue(config3.isValidTlsConfig());
    }

    private static void configureTlsProperties(MongoClientConfig config, String keystorePassword, String truststorePassword)
    {
        config.setTlsEnabled(true)
                .setKeystorePath(keystoreFile.toFile())
                .setKeystorePassword(keystorePassword)
                .setTruststorePath(truststoreFile.toFile())
                .setTruststorePassword(truststorePassword);
    }

    private static void assertFailsTlsValidation(MongoClientConfig config)
    {
        assertFailsValidation(
                config,
                "validTlsConfig",
                "'mongodb.tls.keystore-path', 'mongodb.tls.keystore-password', 'mongodb.tls.truststore-path' and 'mongodb.tls.truststore-password' must be empty when TLS is disabled",
                AssertTrue.class);
    }
}
