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
package com.facebook.presto.cli;

import com.facebook.presto.cli.ClientOptions.ClientSessionProperty;
import com.facebook.presto.client.ClientSession;
import com.facebook.presto.sql.parser.SqlParser;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.airlift.airline.SingleCommand.singleCommand;
import static org.testng.Assert.assertEquals;

public class TestClientOptions
{
    @Test
    public void testDefault()
    {
        ClientSession session = new ClientOptions().toClientSession();
        assertEquals(session.getServer().toString(), "http://localhost:8080");
        assertEquals(session.getSource(), "presto-cli");
    }

    @Test
    public void testSource()
    {
        ClientOptions options = new ClientOptions();
        options.source = "test";
        ClientSession session = options.toClientSession();
        assertEquals(session.getSource(), "test");
    }

    @Test
    public void testServerHostOnly()
    {
        ClientOptions options = new ClientOptions();
        options.server = "localhost";
        ClientSession session = options.toClientSession();
        assertEquals(session.getServer().toString(), "http://localhost:80");
    }

    @Test
    public void testServerHostPort()
    {
        ClientOptions options = new ClientOptions();
        options.server = "localhost:8888";
        ClientSession session = options.toClientSession();
        assertEquals(session.getServer().toString(), "http://localhost:8888");
    }

    @Test
    public void testServerHttpUri()
    {
        ClientOptions options = new ClientOptions();
        options.server = "http://localhost/foo";
        ClientSession session = options.toClientSession();
        assertEquals(session.getServer().toString(), "http://localhost/foo");
    }

    @Test
    public void testServerHttpsUri()
    {
        ClientOptions options = new ClientOptions();
        options.server = "https://localhost/foo";
        ClientSession session = options.toClientSession();
        assertEquals(session.getServer().toString(), "https://localhost/foo");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testInvalidServer()
    {
        ClientOptions options = new ClientOptions();
        options.server = "x:y";
        options.toClientSession();
    }

    @Test
    public void testSessionProperties()
    {
        Console console = singleCommand(Console.class).parse("--session", "system=system-value", "--session", "catalog.name=catalog-property");

        ClientOptions options = console.clientOptions;
        assertEquals(options.sessionProperties, ImmutableList.of(
                new ClientSessionProperty(Optional.empty(), "system", "system-value"),
                new ClientSessionProperty(Optional.of("catalog"), "name", "catalog-property")));

        // special characters are allowed in the value
        assertEquals(new ClientSessionProperty("foo=bar:=baz"), new ClientSessionProperty(Optional.empty(), "foo", "bar:=baz"));

        // empty values are allowed
        assertEquals(new ClientSessionProperty("foo="), new ClientSessionProperty(Optional.empty(), "foo", ""));
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testThreePartPropertyName()
    {
        new ClientSessionProperty("foo.bar.baz=value");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testEmptyPropertyName()
    {
        new ClientSessionProperty("=value");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testInvalidCharsetPropertyName()
    {
        new ClientSessionProperty("\u2603=value");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testInvalidCharsetPropertyValue()
    {
        new ClientSessionProperty("name=\u2603");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testEqualSignNoAllowedInPropertyCatalog()
    {
        new ClientSessionProperty(Optional.of("cat=alog"), "name", "value");
    }

    @Test
    public void testUpdateSessionParameters()
            throws Exception
    {
        ClientOptions options = new ClientOptions();
        ClientSession session = options.toClientSession();
        SqlParser sqlParser = new SqlParser();

        ImmutableMap<String, String> existingProperties = ImmutableMap.of("query_max_memory", "10GB", "distributed_join", "true");
        session = Console.processSessionParameterChange(sqlParser.createStatement("USE test_catalog.test_schema"), session, existingProperties);
        assertEquals(session.getCatalog(), "test_catalog");
        assertEquals(session.getSchema(), "test_schema");
        assertEquals(session.getProperties().get("query_max_memory"), "10GB");
        assertEquals(session.getProperties().get("distributed_join"), "true");

        session = Console.processSessionParameterChange(sqlParser.createStatement("USE test_schema_b"), session, existingProperties);
        assertEquals(session.getCatalog(), "test_catalog");
        assertEquals(session.getSchema(), "test_schema_b");
        assertEquals(session.getProperties().get("query_max_memory"), "10GB");
        assertEquals(session.getProperties().get("distributed_join"), "true");

        session = Console.processSessionParameterChange(sqlParser.createStatement("USE test_catalog_2.test_schema"), session, existingProperties);
        assertEquals(session.getCatalog(), "test_catalog_2");
        assertEquals(session.getSchema(), "test_schema");
        assertEquals(session.getProperties().get("query_max_memory"), "10GB");
        assertEquals(session.getProperties().get("distributed_join"), "true");
    }
}
