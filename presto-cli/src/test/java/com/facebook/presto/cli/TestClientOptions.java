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

import com.facebook.presto.cli.ClientOptions.ClientResourceEstimate;
import com.facebook.presto.cli.ClientOptions.ClientSessionProperty;
import com.facebook.presto.client.ClientSession;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.airlift.airline.SingleCommand.singleCommand;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

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
    public void testResourceEstimates()
    {
        Console console = singleCommand(Console.class).parse("--resource-estimate", "resource1=1B", "--resource-estimate", "resource2=2.2h");

        ClientOptions options = console.clientOptions;
        assertEquals(options.resourceEstimates, ImmutableList.of(
                new ClientResourceEstimate("resource1", "1B"),
                new ClientResourceEstimate("resource2", "2.2h")));
    }

    @Test
    public void testExtraCredentials()
    {
        Console console = singleCommand(Console.class).parse("--extra-credential", "test.token.foo=foo", "--extra-credential", "test.token.bar=bar");
        ClientOptions options = console.clientOptions;
        assertEquals(options.extraCredentials, ImmutableList.of(
                new ClientOptions.ClientExtraCredential("test.token.foo", "foo"),
                new ClientOptions.ClientExtraCredential("test.token.bar", "bar")));
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

    @Test
    public void testDisableCompression()
    {
        Console console = singleCommand(Console.class).parse("--disable-compression");
        assertTrue(console.clientOptions.disableCompression);
        assertTrue(console.clientOptions.toClientSession().isCompressionDisabled());
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

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Multiple entries with same key: test.token.foo=bar and test.token.foo=foo")
    public void testDuplicateExtraCredentialKey()
    {
        Console console = singleCommand(Console.class).parse("--extra-credential", "test.token.foo=foo", "--extra-credential", "test.token.foo=bar");
        ClientOptions options = console.clientOptions;
        options.toClientSession();
    }
}
