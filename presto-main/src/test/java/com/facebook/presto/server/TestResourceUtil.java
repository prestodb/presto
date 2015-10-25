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
import com.facebook.presto.execution.QueryId;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.security.AllowAllAccessControl;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import javax.servlet.http.HttpServletRequest;

import java.util.Locale;

import static com.facebook.presto.SystemSessionProperties.DISTRIBUTED_JOIN;
import static com.facebook.presto.SystemSessionProperties.HASH_PARTITION_COUNT;
import static com.facebook.presto.SystemSessionProperties.QUERY_MAX_MEMORY;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_CATALOG;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_LANGUAGE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SCHEMA;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SESSION;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SOURCE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_TIME_ZONE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_USER;
import static com.facebook.presto.server.ResourceUtil.createSessionForRequest;
import static com.facebook.presto.spi.type.TimeZoneKey.getTimeZoneKey;
import static org.testng.Assert.assertEquals;

public class TestResourceUtil
{
    @Test
    public void testCreateSession()
            throws Exception
    {
        HttpServletRequest request = new MockHttpServletRequest(
                ImmutableListMultimap.<String, String>builder()
                        .put(PRESTO_USER, "testUser")
                        .put(PRESTO_SOURCE, "testSource")
                        .put(PRESTO_CATALOG, "testCatalog")
                        .put(PRESTO_SCHEMA, "testSchema")
                        .put(PRESTO_LANGUAGE, "zh-TW")
                        .put(PRESTO_TIME_ZONE, "Asia/Taipei")
                        .put(PRESTO_SESSION, QUERY_MAX_MEMORY + "=1GB")
                        .put(PRESTO_SESSION, DISTRIBUTED_JOIN + "=true," + HASH_PARTITION_COUNT + " = 43")
                        .build(),
                "testRemote");

        Session session = createSessionForRequest(request, new AllowAllAccessControl(), new SessionPropertyManager(), new QueryId("test_query_id"));

        assertEquals(session.getQueryId(), new QueryId("test_query_id"));
        assertEquals(session.getUser(), "testUser");
        assertEquals(session.getSource().get(), "testSource");
        assertEquals(session.getCatalog().get(), "testCatalog");
        assertEquals(session.getSchema().get(), "testSchema");
        assertEquals(session.getLocale(), Locale.TAIWAN);
        assertEquals(session.getTimeZoneKey(), getTimeZoneKey("Asia/Taipei"));
        assertEquals(session.getRemoteUserAddress().get(), "testRemote");
        assertEquals(session.getSystemProperties(), ImmutableMap.<String, String>builder()
                .put(QUERY_MAX_MEMORY, "1GB")
                .put(DISTRIBUTED_JOIN, "true")
                .put(HASH_PARTITION_COUNT, "43")
                .build());
    }
}
