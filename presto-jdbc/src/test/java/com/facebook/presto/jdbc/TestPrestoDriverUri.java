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
package com.facebook.presto.jdbc;

import org.testng.annotations.Test;

import java.net.URI;
import java.sql.SQLException;
import java.util.Properties;

import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.fail;

public class TestPrestoDriverUri
{
    @Test
    public void testInvalidUrls()
    {
        // missing port
        assertInvalid("jdbc:presto://localhost/", "No port number specified:");

        // extra path segments
        assertInvalid("jdbc:presto://localhost:8080/hive/default/abc", "Invalid path segments in URL:");

        // extra slash
        assertInvalid("jdbc:presto://localhost:8080//", "Catalog name is empty:");

        // has schema but is missing catalog
        assertInvalid("jdbc:presto://localhost:8080//default", "Catalog name is empty:");

        // has catalog but schema is missing
        assertInvalid("jdbc:presto://localhost:8080/a//", "Schema name is empty:");

        // unrecognized property
        assertInvalid("jdbc:presto://localhost:8080/hive/default?ShoeSize=13", "Unrecognized connection property 'ShoeSize'");

        // empty property
        assertInvalid("jdbc:presto://localhost:8080/hive/default?password=", "Connection property 'password' value is empty");
    }

    @Test
    public void testUrlWithSsl()
            throws SQLException
    {
        PrestoDriverUri parameters = createDriverUri("presto://some-ssl-server:443/blackhole");

        URI uri = parameters.getHttpUri();
        assertEquals(uri.getPort(), 443);
        assertEquals(uri.getScheme(), "https");
    }

    @Test
    public void testUriWithSecureMissing()
            throws SQLException
    {
        PrestoDriverUri parameters = createDriverUri("presto://localhost:8080/blackhole");

        URI uri = parameters.getHttpUri();
        assertEquals(uri.getPort(), 8080);
        assertEquals(uri.getScheme(), "http");
    }

    @Test
    public void testUriWithSecureTrue()
            throws SQLException
    {
        PrestoDriverUri parameters = createDriverUri("presto://localhost:8080/blackhole?secure=true");

        URI uri = parameters.getHttpUri();
        assertEquals(uri.getPort(), 8080);
        assertEquals(uri.getScheme(), "https");
    }

    @Test
    public void testUriWithSecureFalse()
            throws SQLException
    {
        PrestoDriverUri parameters = createDriverUri("presto://localhost:8080/blackhole?secure=false");

        URI uri = parameters.getHttpUri();
        assertEquals(uri.getPort(), 8080);
        assertEquals(uri.getScheme(), "http");
    }

    private static PrestoDriverUri createDriverUri(String url)
            throws SQLException
    {
        Properties properties = new Properties();
        properties.setProperty("user", "test");

        return new PrestoDriverUri(url, properties);
    }

    private static void assertInvalid(String url, String prefix)
    {
        try {
            createDriverUri(url);
            fail("expected exception");
        }
        catch (SQLException e) {
            assertNotNull(e.getMessage());
            if (!e.getMessage().startsWith(prefix)) {
                fail(format("expected:<%s> to start with <%s>", e.getMessage(), prefix));
            }
        }
    }
}
