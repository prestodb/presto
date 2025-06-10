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
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static com.facebook.airlift.testing.ValidationAssertions.assertFailsValidation;
import static org.testng.Assert.assertEquals;

public class TestMongoClientConfig
{
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
                .setSslEnabled(false)
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
                .setImplicitRowFieldPrefix("_pos"));
    }

    @Test
    public void testExplicitPropertyMappings() throws IOException
    {
        Path keystoreFile = Files.createTempFile(null, null);
        Path truststoreFile = Files.createTempFile(null, null);

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
                .put("mongodb.ssl.enabled", "true")
                .put("mongodb.tls.enabled", "true")
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
                .setSocketKeepAlive(true)
                .setSslEnabled(true)
                .setTlsEnabled(true)
                .setKeystorePath(keystoreFile.toFile())
                .setKeystorePassword("keystore-password")
                .setTruststorePath(truststoreFile.toFile())
                .setTruststorePassword("truststore-password")
                .setCursorBatchSize(1)
                .setReadPreference(ReadPreferenceType.NEAREST)
                .setReadPreferenceTags("tag_name:tag_value")
                .setWriteConcern(WriteConcernType.UNACKNOWLEDGED)
                .setRequiredReplicaSetName("replica_set")
                .setImplicitRowFieldPrefix("_prefix");

        ConfigAssertions.assertFullMapping(properties, expected);
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
    public void testValidation()
            throws Exception
    {
        Path keystoreFile = Files.createTempFile(null, null);
        Path truststoreFile = Files.createTempFile(null, null);

        assertFailsTlsValidation(new MongoClientConfig().setKeystorePath(keystoreFile.toFile()));
        assertFailsTlsValidation(new MongoClientConfig().setKeystorePassword("keystore password"));
        assertFailsTlsValidation(new MongoClientConfig().setTruststorePath(truststoreFile.toFile()));
        assertFailsTlsValidation(new MongoClientConfig().setTruststorePassword("truststore password"));
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
