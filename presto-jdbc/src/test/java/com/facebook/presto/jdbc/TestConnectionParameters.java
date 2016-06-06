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

import static java.lang.String.format;
import static org.testng.Assert.assertEquals;

public class TestConnectionParameters
{
    private static final String SERVER = "127.0.0.1:60429";

    @Test(expectedExceptions = SQLException.class, expectedExceptionsMessageRegExp = "Invalid path segments in URL: .*")
    public void testBadUrlExtraPathSegments()
            throws Exception
    {
        String url = format("jdbc:presto://%s/hive/default/bad_string", SERVER);
        new ConnectionParameters(url);
    }

    @Test(expectedExceptions = SQLException.class, expectedExceptionsMessageRegExp = "Catalog name is empty: .*")
    public void testBadUrlMissingCatalog()
            throws Exception
    {
        String url = format("jdbc:presto://%s//default", SERVER);
        new ConnectionParameters(url);
    }

    @Test(expectedExceptions = SQLException.class, expectedExceptionsMessageRegExp = "Catalog name is empty: .*")
    public void testBadUrlEndsInSlashes()
            throws Exception
    {
        String url = format("jdbc:presto://%s//", SERVER);
        new ConnectionParameters(url);
    }

    @Test(expectedExceptions = SQLException.class, expectedExceptionsMessageRegExp = "Schema name is empty: .*")
    public void testBadUrlMissingSchema()
            throws Exception
    {
        String url = format("jdbc:presto://%s/a//", SERVER);
        new ConnectionParameters(url);
    }

    @Test
    public void testURIWithSSL()
            throws SQLException
    {
        final ConnectionParameters parameters = new ConnectionParameters(URI.create("presto://some-ssl-server:443/blackhole"));

        final URI uri = parameters.getHttpUri();
        assertEquals(uri.getPort(), 443);
        assertEquals(uri.getScheme(), "https");
    }

    @Test
    public void testURIWithSSLmissing()
            throws SQLException
    {
        final ConnectionParameters parameters = new ConnectionParameters(URI.create("presto://localhost:8080/blackhole"));

        final URI uri = parameters.getHttpUri();
        assertEquals(uri.getPort(), 8080);
        assertEquals(uri.getScheme(), "http");
    }

    @Test
    public void testURIWithUseSSLTrue()
            throws SQLException
    {
        final ConnectionParameters parameters = new ConnectionParameters(URI.create("presto://localhost:8080/blackhole?useSSL=true"));

        final URI uri = parameters.getHttpUri();
        assertEquals(uri.getPort(), 8080);
        assertEquals(uri.getScheme(), "https");
    }

    @Test
    public void testURIWithUseSSLFalse()
            throws SQLException
    {
        final ConnectionParameters parameters = new ConnectionParameters(URI.create("presto://localhost:8080/blackhole?useSSL=false"));

        final URI uri = parameters.getHttpUri();
        assertEquals(uri.getPort(), 8080);
        assertEquals(uri.getScheme(), "http");
    }
}
