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

import com.facebook.presto.client.ClientSession;
import com.facebook.presto.sql.parser.SqlParser;
import org.testng.annotations.Test;

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
    public void testUpdateSessionParameters()
            throws Exception
    {
        ClientOptions options = new ClientOptions();
        ClientSession session = options.toClientSession();
        SqlParser sqlParser = new SqlParser();
        session = Console.processSessionParameterChange(sqlParser.createStatement("USE CATALOG test_catalog"), session);
        assertEquals(session.getCatalog(), "test_catalog");
        session = Console.processSessionParameterChange(sqlParser.createStatement("USE SCHEMA test_schema"), session);
        assertEquals(session.getSchema(), "test_schema");
    }
}
