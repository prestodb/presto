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

import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.security.SelectedRole;
import com.facebook.presto.sql.parser.IdentifierSymbol;
import com.facebook.presto.sql.parser.SqlParserOptions;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.WebApplicationException;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.EnumSet;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.HASH_PARTITION_COUNT;
import static com.facebook.presto.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static com.facebook.presto.SystemSessionProperties.QUERY_MAX_MEMORY;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_CATALOG;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_CLIENT_INFO;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_EXTRA_CREDENTIAL;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_LANGUAGE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_PREPARED_STATEMENT;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_ROLE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SCHEMA;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SESSION;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SOURCE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_TIME_ZONE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_USER;
import static org.testng.Assert.assertEquals;

public class TestHttpRequestSessionContext
{
    @Test
    public void testSessionContext()
    {
        HttpServletRequest request = new MockHttpServletRequest(
                ImmutableListMultimap.<String, String>builder()
                        .put(PRESTO_USER, "testUser")
                        .put(PRESTO_SOURCE, "testSource")
                        .put(PRESTO_CATALOG, "testCatalog")
                        .put(PRESTO_SCHEMA, "testSchema")
                        .put(PRESTO_LANGUAGE, "zh-TW")
                        .put(PRESTO_TIME_ZONE, "Asia/Taipei")
                        .put(PRESTO_CLIENT_INFO, "client-info")
                        .put(PRESTO_SESSION, QUERY_MAX_MEMORY + "=1GB")
                        .put(PRESTO_SESSION, JOIN_DISTRIBUTION_TYPE + "=partitioned," + HASH_PARTITION_COUNT + " = 43")
                        .put(PRESTO_PREPARED_STATEMENT, "query1=select * from foo,query2=select * from bar")
                        .put(PRESTO_ROLE, "foo_connector=ALL")
                        .put(PRESTO_ROLE, "bar_connector=NONE")
                        .put(PRESTO_ROLE, "foobar_connector=ROLE{role}")
                        .put(PRESTO_EXTRA_CREDENTIAL, "test.token.foo=bar")
                        .put(PRESTO_EXTRA_CREDENTIAL, "test.token.abc=xyz")
                        .build(),
                "testRemote");

        HttpRequestSessionContext context = new HttpRequestSessionContext(request, new SqlParserOptions());
        assertEquals(context.getSource(), "testSource");
        assertEquals(context.getCatalog(), "testCatalog");
        assertEquals(context.getSchema(), "testSchema");
        assertEquals(context.getIdentity(), new Identity("testUser", Optional.empty()));
        assertEquals(context.getClientInfo(), "client-info");
        assertEquals(context.getLanguage(), "zh-TW");
        assertEquals(context.getTimeZoneId(), "Asia/Taipei");
        assertEquals(context.getSystemProperties(), ImmutableMap.of(QUERY_MAX_MEMORY, "1GB", JOIN_DISTRIBUTION_TYPE, "partitioned", HASH_PARTITION_COUNT, "43"));
        assertEquals(context.getPreparedStatements(), ImmutableMap.of("query1", "select * from foo", "query2", "select * from bar"));
        assertEquals(context.getIdentity().getRoles(), ImmutableMap.of(
                "foo_connector", new SelectedRole(SelectedRole.Type.ALL, Optional.empty()),
                "bar_connector", new SelectedRole(SelectedRole.Type.NONE, Optional.empty()),
                "foobar_connector", new SelectedRole(SelectedRole.Type.ROLE, Optional.of("role"))));
        assertEquals(context.getIdentity().getExtraCredentials(), ImmutableMap.of("test.token.foo", "bar", "test.token.abc", "xyz"));
    }

    @Test(expectedExceptions = WebApplicationException.class)
    public void testPreparedStatementsHeaderDoesNotParse()
    {
        HttpServletRequest request = new MockHttpServletRequest(
                ImmutableListMultimap.<String, String>builder()
                        .put(PRESTO_USER, "testUser")
                        .put(PRESTO_SOURCE, "testSource")
                        .put(PRESTO_CATALOG, "testCatalog")
                        .put(PRESTO_SCHEMA, "testSchema")
                        .put(PRESTO_LANGUAGE, "zh-TW")
                        .put(PRESTO_TIME_ZONE, "Asia/Taipei")
                        .put(PRESTO_CLIENT_INFO, "null")
                        .put(PRESTO_PREPARED_STATEMENT, "query1=abcdefg")
                        .build(),
                "testRemote");
        new HttpRequestSessionContext(request, new SqlParserOptions());
    }

    @Test
    public void testPreparedStatementsSpecialCharacters()
    {
        HttpServletRequest request = new MockHttpServletRequest(
                ImmutableListMultimap.<String, String>builder()
                        .put(PRESTO_USER, "testUser")
                        .put(PRESTO_SOURCE, "testSource")
                        .put(PRESTO_CATALOG, "testCatalog")
                        .put(PRESTO_SCHEMA, "testSchema")
                        .put(PRESTO_LANGUAGE, "zh-TW")
                        .put(PRESTO_TIME_ZONE, "Asia/Taipei")
                        .put(PRESTO_CLIENT_INFO, "null")
                        .put(PRESTO_PREPARED_STATEMENT, "query1=select * from tbl:ns")
                        .build(),
                "testRemote");
        SqlParserOptions options = new SqlParserOptions();
        options.allowIdentifierSymbol(EnumSet.allOf(IdentifierSymbol.class));

        new HttpRequestSessionContext(request, options);
    }

    @Test
    public void testExtraCredentials()
    {
        HttpServletRequest request = new MockHttpServletRequest(
                ImmutableListMultimap.<String, String>builder()
                        .put(PRESTO_USER, "testUser")
                        .put(PRESTO_SOURCE, "testSource")
                        .put(PRESTO_CATALOG, "testCatalog")
                        .put(PRESTO_SCHEMA, "testSchema")
                        .put(PRESTO_LANGUAGE, "zh-TW")
                        .put(PRESTO_TIME_ZONE, "Asia/Taipei")
                        .put(PRESTO_CLIENT_INFO, "client-info")
                        .put(PRESTO_SESSION, QUERY_MAX_MEMORY + "=1GB")
                        .put(PRESTO_SESSION, JOIN_DISTRIBUTION_TYPE + "=partitioned," + HASH_PARTITION_COUNT + " = 43")
                        .put(PRESTO_PREPARED_STATEMENT, "query1=select * from foo,query2=select * from bar")
                        .put(PRESTO_ROLE, "foo_connector=ALL")
                        .put(PRESTO_ROLE, "bar_connector=NONE")
                        .put(PRESTO_ROLE, "foobar_connector=ROLE{role}")
                        .put(PRESTO_EXTRA_CREDENTIAL, "test.token.key1=" + urlEncode("bar=ab===,d"))
                        .put(PRESTO_EXTRA_CREDENTIAL, "test.token.key2=bar=ab===")
                        .put(PRESTO_EXTRA_CREDENTIAL, "test.json=" + urlEncode("{\"a\" : \"b\", \"c\" : \"d=\"}") + ", test.token.key3 = abc=cd")
                        .put(PRESTO_EXTRA_CREDENTIAL, "test.token.abc=xyz")
                        .build(),
                "testRemote");

        HttpRequestSessionContext context = new HttpRequestSessionContext(request, new SqlParserOptions());
        assertEquals(
                context.getIdentity().getExtraCredentials(),
                ImmutableMap.builder()
                        .put("test.token.key1", "bar=ab===,d")
                        .put("test.token.key2", "bar=ab===")
                        .put("test.token.key3", "abc=cd")
                        .put("test.json", "{\"a\" : \"b\", \"c\" : \"d=\"}")
                        .put("test.token.abc", "xyz")
                        .build());
    }

    private static String urlEncode(String value)
    {
        try {
            return URLEncoder.encode(value, "UTF-8");
        }
        catch (UnsupportedEncodingException e) {
            throw new AssertionError(e);
        }
    }
}
