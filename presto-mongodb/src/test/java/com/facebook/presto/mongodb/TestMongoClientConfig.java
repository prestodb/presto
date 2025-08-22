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
import org.testng.annotations.Test;

import java.util.Map;

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
                .setCursorBatchSize(0)
                .setReadPreference(ReadPreferenceType.PRIMARY)
                .setReadPreferenceTags("")
                .setWriteConcern(WriteConcernType.ACKNOWLEDGED)
                .setRequiredReplicaSetName(null)
                .setImplicitRowFieldPrefix("_pos")
                .setCaseSensitiveNameMatchingEnabled(false));
    }

    @Test
    public void testExplicitPropertyMappings()
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
                .put("mongodb.ssl.enabled", "true")
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
                .setSocketKeepAlive(true)
                .setSslEnabled(true)
                .setCursorBatchSize(1)
                .setReadPreference(ReadPreferenceType.NEAREST)
                .setReadPreferenceTags("tag_name:tag_value")
                .setWriteConcern(WriteConcernType.UNACKNOWLEDGED)
                .setRequiredReplicaSetName("replica_set")
                .setImplicitRowFieldPrefix("_prefix")
                .setCaseSensitiveNameMatchingEnabled(true);

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
}
