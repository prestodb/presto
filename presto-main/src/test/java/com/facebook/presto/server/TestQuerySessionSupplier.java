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
package com.facebook.presto.server;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.security.AllowAllAccessControl;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.facebook.presto.spi.session.SessionPropertyConfigurationManagerFactory;
import com.facebook.presto.spi.session.TestingSessionPropertyConfigurationManagerFactory;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import javax.servlet.http.HttpServletRequest;

import java.util.Locale;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.DISTRIBUTED_JOIN;
import static com.facebook.presto.SystemSessionProperties.HASH_PARTITION_COUNT;
import static com.facebook.presto.SystemSessionProperties.QUERY_MAX_MEMORY;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_CATALOG;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_CLIENT_INFO;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_CLIENT_TAGS;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_LANGUAGE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_PREPARED_STATEMENT;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SCHEMA;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SESSION;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SOURCE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_TIME_ZONE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_USER;
import static com.facebook.presto.spi.type.TimeZoneKey.getTimeZoneKey;
import static com.facebook.presto.transaction.TransactionManager.createTestTransactionManager;
import static org.testng.Assert.assertEquals;

public class TestQuerySessionSupplier
{
    private static final HttpServletRequest TEST_REQUEST = new MockHttpServletRequest(
            ImmutableListMultimap.<String, String>builder()
                    .put(PRESTO_USER, "testUser")
                    .put(PRESTO_SOURCE, "testSource")
                    .put(PRESTO_CATALOG, "testCatalog")
                    .put(PRESTO_SCHEMA, "testSchema")
                    .put(PRESTO_LANGUAGE, "zh-TW")
                    .put(PRESTO_TIME_ZONE, "Asia/Taipei")
                    .put(PRESTO_CLIENT_INFO, "client-info")
                    .put(PRESTO_CLIENT_TAGS, "tag1,tag2 ,tag3, tag2")
                    .put(PRESTO_SESSION, QUERY_MAX_MEMORY + "=1GB")
                    .put(PRESTO_SESSION, DISTRIBUTED_JOIN + "=true," + HASH_PARTITION_COUNT + " = 43")
                    .put(PRESTO_PREPARED_STATEMENT, "query1=select * from foo,query2=select * from bar")
                    .build(),
            "testRemote");
    private static final ResourceGroupId TEST_RESOURCE_GROUP_ID = new ResourceGroupId("test");

    @Test
    public void testCreateSession()
    {
        HttpRequestSessionContext context = new HttpRequestSessionContext(TEST_REQUEST);
        QuerySessionSupplier sessionSupplier = new QuerySessionSupplier(
                createTestTransactionManager(),
                new AllowAllAccessControl(),
                new SessionPropertyManager());
        Session session = sessionSupplier.createSession(new QueryId("test_query_id"), context, Optional.empty(), TEST_RESOURCE_GROUP_ID);

        assertEquals(session.getQueryId(), new QueryId("test_query_id"));
        assertEquals(session.getUser(), "testUser");
        assertEquals(session.getSource().get(), "testSource");
        assertEquals(session.getCatalog().get(), "testCatalog");
        assertEquals(session.getSchema().get(), "testSchema");
        assertEquals(session.getLocale(), Locale.TAIWAN);
        assertEquals(session.getTimeZoneKey(), getTimeZoneKey("Asia/Taipei"));
        assertEquals(session.getRemoteUserAddress().get(), "testRemote");
        assertEquals(session.getClientInfo().get(), "client-info");
        assertEquals(session.getClientTags(), ImmutableSet.of("tag1", "tag2", "tag3"));
        assertEquals(session.getSystemProperties(), ImmutableMap.<String, String>builder()
                .put(QUERY_MAX_MEMORY, "1GB")
                .put(DISTRIBUTED_JOIN, "true")
                .put(HASH_PARTITION_COUNT, "43")
                .build());
        assertEquals(session.getPreparedStatements(), ImmutableMap.<String, String>builder()
                .put("query1", "select * from foo")
                .put("query2", "select * from bar")
                .build());
    }

    @Test
    public void testApplySessionPropertyConfigurationManager()
    {
        HttpRequestSessionContext context = new HttpRequestSessionContext(TEST_REQUEST);
        QuerySessionSupplier sessionSupplier = new QuerySessionSupplier(
                createTestTransactionManager(),
                new AllowAllAccessControl(),
                new SessionPropertyManager());
        SessionPropertyConfigurationManagerFactory factory = new TestingSessionPropertyConfigurationManagerFactory(
                ImmutableMap.of(QUERY_MAX_MEMORY, "10GB", "key2", "20", "key3", "3"),
                ImmutableMap.of("testCatalog", ImmutableMap.of("key1", "10")));
        sessionSupplier.addConfigurationManager(factory);
        sessionSupplier.setConfigurationManager(factory.getName(), ImmutableMap.of());
        Session session = sessionSupplier.createSession(new QueryId("test_query_id"), context, Optional.empty(), TEST_RESOURCE_GROUP_ID);
        assertEquals(session.getSystemProperties(), ImmutableMap.<String, String>builder()
                .put(QUERY_MAX_MEMORY, "1GB")
                .put(DISTRIBUTED_JOIN, "true")
                .put(HASH_PARTITION_COUNT, "43")
                .put("key2", "20")
                .put("key3", "3")
                .build());
        assertEquals(session.getUnprocessedCatalogProperties(), ImmutableMap.of("testCatalog", ImmutableMap.of("key1", "10")));
    }

    @Test
    public void testEmptyClientTags()
    {
        HttpServletRequest request1 = new MockHttpServletRequest(
                ImmutableListMultimap.<String, String>builder()
                        .put(PRESTO_USER, "testUser")
                        .build(),
                "remoteAddress");
        HttpRequestSessionContext context1 = new HttpRequestSessionContext(request1);
        assertEquals(context1.getClientTags(), ImmutableSet.of());

        HttpServletRequest request2 = new MockHttpServletRequest(
                ImmutableListMultimap.<String, String>builder()
                        .put(PRESTO_USER, "testUser")
                        .put(PRESTO_CLIENT_TAGS, "")
                        .build(),
                "remoteAddress");
        HttpRequestSessionContext context2 = new HttpRequestSessionContext(request2);
        assertEquals(context2.getClientTags(), ImmutableSet.of());
    }

    @Test(expectedExceptions = PrestoException.class)
    public void testInvalidTimeZone()
    {
        HttpServletRequest request = new MockHttpServletRequest(
                ImmutableListMultimap.<String, String>builder()
                        .put(PRESTO_USER, "testUser")
                        .put(PRESTO_TIME_ZONE, "unknown_timezone")
                        .build(),
                "testRemote");
        HttpRequestSessionContext context = new HttpRequestSessionContext(request);
        QuerySessionSupplier sessionSupplier = new QuerySessionSupplier(
                createTestTransactionManager(),
                new AllowAllAccessControl(),
                new SessionPropertyManager());
        sessionSupplier.createSession(new QueryId("test_query_id"), context, Optional.empty(), TEST_RESOURCE_GROUP_ID);
    }
}
