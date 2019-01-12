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
package io.prestosql.server;

import com.google.common.collect.ImmutableMap;
import io.airlift.node.NodeInfo;
import io.prestosql.Session;
import io.prestosql.metadata.SessionPropertyManager;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.resourcegroups.ResourceGroupId;
import io.prestosql.spi.security.Identity;
import io.prestosql.spi.session.SessionPropertyConfigurationManagerFactory;
import io.prestosql.spi.session.TestingSessionPropertyConfigurationManagerFactory;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.prestosql.SystemSessionProperties.HASH_PARTITION_COUNT;
import static io.prestosql.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.prestosql.SystemSessionProperties.QUERY_MAX_MEMORY;
import static org.testng.Assert.assertEquals;

public class TestSessionPropertyDefaults
{
    private static final ResourceGroupId TEST_RESOURCE_GROUP_ID = new ResourceGroupId("test");
    private static final NodeInfo TEST_NODE_INFO = new NodeInfo("test");

    @Test
    public void testApplyDefaultProperties()
    {
        SessionPropertyDefaults sessionPropertyDefaults = new SessionPropertyDefaults(TEST_NODE_INFO);
        SessionPropertyConfigurationManagerFactory factory = new TestingSessionPropertyConfigurationManagerFactory(
                ImmutableMap.<String, String>builder()
                        .put(QUERY_MAX_MEMORY, "override")
                        .put("system_default", "system_default")
                        .build(),
                ImmutableMap.of(
                        "testCatalog",
                        ImmutableMap.<String, String>builder()
                                .put("explicit_set", "override")
                                .put("catalog_default", "catalog_default")
                                .build()));
        sessionPropertyDefaults.addConfigurationManagerFactory(factory);
        sessionPropertyDefaults.setConfigurationManager(factory.getName(), ImmutableMap.of());

        Session session = Session.builder(new SessionPropertyManager())
                .setQueryId(new QueryId("test_query_id"))
                .setIdentity(new Identity("testUser", Optional.empty()))
                .setSystemProperty(QUERY_MAX_MEMORY, "1GB")
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "partitioned")
                .setSystemProperty(HASH_PARTITION_COUNT, "43")
                .setCatalogSessionProperty("testCatalog", "explicit_set", "explicit_set")
                .build();

        assertEquals(session.getSystemProperties(), ImmutableMap.<String, String>builder()
                .put(QUERY_MAX_MEMORY, "1GB")
                .put(JOIN_DISTRIBUTION_TYPE, "partitioned")
                .put(HASH_PARTITION_COUNT, "43")
                .build());
        assertEquals(
                session.getUnprocessedCatalogProperties(),
                ImmutableMap.of(
                        "testCatalog",
                        ImmutableMap.<String, String>builder()
                                .put("explicit_set", "explicit_set")
                                .build()));

        session = sessionPropertyDefaults.newSessionWithDefaultProperties(session, Optional.empty(), TEST_RESOURCE_GROUP_ID);

        assertEquals(session.getSystemProperties(), ImmutableMap.<String, String>builder()
                .put(QUERY_MAX_MEMORY, "1GB")
                .put(JOIN_DISTRIBUTION_TYPE, "partitioned")
                .put(HASH_PARTITION_COUNT, "43")
                .put("system_default", "system_default")
                .build());
        assertEquals(
                session.getUnprocessedCatalogProperties(),
                ImmutableMap.of(
                        "testCatalog",
                        ImmutableMap.<String, String>builder()
                                .put("explicit_set", "explicit_set")
                                .put("catalog_default", "catalog_default")
                                .build()));
    }
}
