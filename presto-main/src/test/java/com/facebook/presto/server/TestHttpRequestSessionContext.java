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

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.common.CatalogSchemaName;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.spi.function.Parameter;
import com.facebook.presto.spi.function.RoutineCharacteristics;
import com.facebook.presto.spi.function.SqlFunctionId;
import com.facebook.presto.spi.function.SqlInvokedFunction;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.security.SelectedRole;
import com.facebook.presto.sql.parser.IdentifierSymbol;
import com.facebook.presto.sql.parser.SqlParserOptions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.WebApplicationException;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.EnumSet;
import java.util.Optional;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
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
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SESSION_FUNCTION;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SOURCE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_TIME_ZONE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_USER;
import static com.facebook.presto.common.type.StandardTypes.INTEGER;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.spi.function.FunctionVersion.notVersioned;
import static com.facebook.presto.spi.function.RoutineCharacteristics.Determinism.DETERMINISTIC;
import static com.facebook.presto.spi.function.RoutineCharacteristics.NullCallClause.RETURNS_NULL_ON_NULL_INPUT;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;

public class TestHttpRequestSessionContext
{
    private static final JsonCodec<SqlFunctionId> SQL_FUNCTION_ID_JSON_CODEC = jsonCodec(SqlFunctionId.class);
    private static final JsonCodec<SqlInvokedFunction> SQL_INVOKED_FUNCTION_JSON_CODEC = jsonCodec(SqlInvokedFunction.class);

    private static final SqlFunctionId SQL_FUNCTION_ID_ADD = createSqlFunctionIdAdd();
    private static final SqlInvokedFunction SQL_FUNCTION_ADD = createFunctionAdd();

    private static final String SERIALIZED_SQL_FUNCTION_ID_ADD = SQL_FUNCTION_ID_JSON_CODEC.toJson(SQL_FUNCTION_ID_ADD);
    private static final String SERIALIZED_SQL_FUNCTION_ADD = SQL_INVOKED_FUNCTION_JSON_CODEC.toJson(SQL_FUNCTION_ADD);

    private static final SqlFunctionId SQL_FUNCTION_ID_ADD1_TO_INT_ARRAY = createSqlFunctionIdAdd1ToIntArray();
    private static final SqlInvokedFunction SQL_FUNCTION_ADD_1_TO_INT_ARRAY = createFunctionAdd1ToIntArray();

    private static final String SERIALIZED_SQL_FUNCTION_ID_ADD_1_TO_INT_ARRAY = SQL_FUNCTION_ID_JSON_CODEC.toJson(SQL_FUNCTION_ID_ADD1_TO_INT_ARRAY);
    private static final String SERIALIZED_SQL_FUNCTION_ADD_1_to_INT_ARRAY = SQL_INVOKED_FUNCTION_JSON_CODEC.toJson(SQL_FUNCTION_ADD_1_TO_INT_ARRAY);

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
                        .put(PRESTO_SESSION_FUNCTION, format(
                                "%s=%s,%s=%s",
                                urlEncode(SERIALIZED_SQL_FUNCTION_ID_ADD),
                                urlEncode(SERIALIZED_SQL_FUNCTION_ADD),
                                urlEncode(SERIALIZED_SQL_FUNCTION_ID_ADD_1_TO_INT_ARRAY),
                                urlEncode(SERIALIZED_SQL_FUNCTION_ADD_1_to_INT_ARRAY)))
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
        assertEquals(
                context.getSessionFunctions(),
                ImmutableMap.of(
                        SQL_FUNCTION_ID_ADD,
                        SQL_FUNCTION_ADD,
                        SQL_FUNCTION_ID_ADD1_TO_INT_ARRAY,
                        SQL_FUNCTION_ADD_1_TO_INT_ARRAY));
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

    protected static String urlEncode(String value)
    {
        try {
            return URLEncoder.encode(value, "UTF-8");
        }
        catch (UnsupportedEncodingException e) {
            throw new AssertionError(e);
        }
    }

    public static SqlFunctionId createSqlFunctionIdAdd()
    {
        return new SqlFunctionId(QualifiedObjectName.valueOf("presto.session.add"), ImmutableList.of(parseTypeSignature("integer"), parseTypeSignature("integer")));
    }

    public static SqlInvokedFunction createFunctionAdd()
    {
        return new SqlInvokedFunction(
                QualifiedObjectName.valueOf(new CatalogSchemaName("presto", "session"), "add"),
                ImmutableList.of(new Parameter("x", parseTypeSignature(INTEGER)), new Parameter("y", parseTypeSignature(INTEGER))),
                parseTypeSignature(INTEGER),
                "add",
                RoutineCharacteristics.builder()
                        .setDeterminism(DETERMINISTIC)
                        .setNullCallClause(RETURNS_NULL_ON_NULL_INPUT)
                        .build(),
                "RETURN x + y",
                notVersioned());
    }

    public static SqlFunctionId createSqlFunctionIdAdd1ToIntArray()
    {
        return new SqlFunctionId(QualifiedObjectName.valueOf("presto.session.add_1_int"), ImmutableList.of(parseTypeSignature("array(int)")));
    }

    public static SqlInvokedFunction createFunctionAdd1ToIntArray()
    {
        return new SqlInvokedFunction(
                QualifiedObjectName.valueOf(new CatalogSchemaName("presto", "session"), "add_1_int"),
                ImmutableList.of(new Parameter("x", parseTypeSignature("array(int)"))),
                parseTypeSignature("array(int)"),
                "add 1 to all elements of array",
                RoutineCharacteristics.builder()
                        .setDeterminism(DETERMINISTIC)
                        .setNullCallClause(RETURNS_NULL_ON_NULL_INPUT)
                        .build(),
                "RETURN transform(x, x -> x + 1)",
                notVersioned());
    }
}
