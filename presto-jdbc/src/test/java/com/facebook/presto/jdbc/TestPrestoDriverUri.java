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

import static com.facebook.presto.jdbc.ConnectionProperties.SSL_TRUST_STORE_PASSWORD;
import static com.facebook.presto.jdbc.ConnectionProperties.SSL_TRUST_STORE_PATH;
import static com.facebook.presto.jdbc.ConnectionProperties.SSL_TRUST_STORE_PWD;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;

public class TestPrestoDriverUri
{
    private static final String SERVER = "127.0.0.1:60429";
    private static final Properties minimalProperties = new Properties();

    public TestPrestoDriverUri()
    {
        minimalProperties.put("user", "BaltimoreJack");
    }

    @Test(expectedExceptions = SQLException.class, expectedExceptionsMessageRegExp = "Unrecognized connection property 'ShoeSize'")
    public void testUnrecognizedParameter()
            throws Exception
    {
        String url = format("jdbc:presto://%s/hive/default?ShoeSize=13", SERVER);
        new PrestoConnectionConfig(url, minimalProperties);
    }

    @Test(expectedExceptions = SQLException.class, expectedExceptionsMessageRegExp = "Connection property 'user' is required")
    public void testRequireUser()
            throws Exception
    {
        String url = format("jdbc:presto://%s", SERVER);
        new PrestoConnectionConfig(url, new Properties());
    }

    @Test(expectedExceptions = SQLException.class, expectedExceptionsMessageRegExp = "Invalid path segments in URL: .*")
    public void testBadUrlExtraPathSegments()
            throws Exception
    {
        String url = format("jdbc:presto://%s/hive/default/bad_string", SERVER);
        new PrestoConnectionConfig(url, minimalProperties);
    }

    @Test(expectedExceptions = SQLException.class, expectedExceptionsMessageRegExp = "Catalog name is empty: .*")
    public void testBadUrlMissingCatalog()
            throws Exception
    {
        String url = format("jdbc:presto://%s//default", SERVER);
        new PrestoConnectionConfig(url, minimalProperties);
    }

    @Test(expectedExceptions = SQLException.class, expectedExceptionsMessageRegExp = "Catalog name is empty: .*")
    public void testBadUrlEndsInSlashes()
            throws Exception
    {
        String url = format("jdbc:presto://%s//", SERVER);
        new PrestoConnectionConfig(url, minimalProperties);
    }

    @Test(expectedExceptions = SQLException.class, expectedExceptionsMessageRegExp = "Schema name is empty: .*")
    public void testBadUrlMissingSchema()
            throws Exception
    {
        String url = format("jdbc:presto://%s/a//", SERVER);
        new PrestoConnectionConfig(url, minimalProperties);
    }

    @Test(expectedExceptions = SQLException.class, expectedExceptionsMessageRegExp = "The value of SSL must be one of.*")
    public void testSslInvalidSslFlag2()
            throws Exception
    {
        String url = format("jdbc:presto://%s?SSL=2&SSLTrustStorePassword=password&SSLTrustStorePath=truststore.jks", SERVER);
        new PrestoConnectionConfig(url, minimalProperties);
    }

    @Test(expectedExceptions = SQLException.class, expectedExceptionsMessageRegExp = "Connection property 'SSL' value is invalid: RainbowStream")
    public void testSslInvalidSslFlagNotInteger()
            throws Exception
    {
        String url = format("jdbc:presto://%s?SSL=RainbowStream&SSLTrustStorePassword=password&SSLTrustStorePath=truststore.jks", SERVER);
        new PrestoConnectionConfig(url, minimalProperties);
    }

    @Test(expectedExceptions = SQLException.class, expectedExceptionsMessageRegExp = "Connection property 'SSLTrustStorePath' is required")
    public void testSslMissingTrustStorePath()
            throws Exception
    {
        String url = format("jdbc:presto://%s?SSL=1&SSLTrustStorePassword=password", SERVER);
        new PrestoConnectionConfig(url, minimalProperties);
    }

    @Test(expectedExceptions = SQLException.class, expectedExceptionsMessageRegExp = "Connection property 'SSLTrustStorePassword' is required")
    public void testSslMissingTrustStorePassword()
            throws Exception
    {
        String url = format("jdbc:presto://%s?SSL=1&SSLTrustStorePath=truststore.jks", SERVER);
        new PrestoConnectionConfig(url, minimalProperties);
    }

    @Test(expectedExceptions = SQLException.class, expectedExceptionsMessageRegExp = "Connection property 'SSLTrustStorePath' is not allowed")
    public void testNotAllowedTrustStorePath()
            throws Exception
    {
        String url = format("jdbc:presto://%s?SSLTrustStorePath=truststore.jks", SERVER);
        new PrestoConnectionConfig(url, minimalProperties);
    }

    @Test(expectedExceptions = SQLException.class, expectedExceptionsMessageRegExp = "Connection property 'SSLTrustStorePassword' is not allowed")
    public void testNotAllowedTrustStorePassword()
            throws Exception
    {
        String url = format("jdbc:presto://%s?SSLTrustStorePassword=password", SERVER);
        new PrestoConnectionConfig(url, minimalProperties);
    }

    @Test(expectedExceptions = SQLException.class, expectedExceptionsMessageRegExp = "Connection property 'SSLTrustStoreP.*d' is not allowed")
    public void testPasswordAndPwd()
            throws Exception
    {
        String url = format("presto://%s?SSL=1&SSLTrustStorePath=truststore.jks&SSLTrustStorePassword=password&SSLTrustStorePwd=pwd", SERVER);
        new PrestoConnectionConfig(url, minimalProperties);
    }

    @Test(expectedExceptions = SQLException.class, expectedExceptionsMessageRegExp = "Connection property 'SSLTrustStoreP.*d' is not allowed")
    public void testPwdAndPassword()
            throws Exception
    {
        String url = format("presto://%s?SSL=1&SSLTrustStorePath=truststore.jks&SSLTrustStorePwd=password&SSLTrustStorePassword=pwd", SERVER);
        new PrestoConnectionConfig(url, minimalProperties);
    }

    @Test
    public void testUriWithoutSsl()
            throws SQLException
    {
        PrestoConnectionConfig parameters = new PrestoConnectionConfig("presto://localhost:8080/blackhole", minimalProperties);

        URI uri = parameters.getHttpUri();
        assertEquals(uri.getPort(), 8080);
        assertEquals(uri.getScheme(), "http");
    }

    @Test
    public void testUriWithSslEnabledPassword()
            throws SQLException
    {
        PrestoConnectionConfig parameters = new PrestoConnectionConfig("presto://localhost:8080/blackhole?SSL=1&SSLTrustStorePath=truststore.jks&SSLTrustStorePassword=password", minimalProperties);

        URI uri = parameters.getHttpUri();
        assertEquals(uri.getPort(), 8080);
        assertEquals(uri.getScheme(), "https");

        Properties properties = parameters.getConnectionProperties();
        assertEquals(properties.getProperty(SSL_TRUST_STORE_PATH.getKey()), "truststore.jks");
        assertEquals(properties.getProperty(SSL_TRUST_STORE_PASSWORD.getKey()), "password");
    }

    @Test
    public void testUriWithSslEnabledPwd()
            throws SQLException
    {
        PrestoConnectionConfig parameters = new PrestoConnectionConfig("presto://localhost:8080/blackhole?SSL=1&SSLTrustStorePath=truststore.jks&SSLTrustStorePwd=password", minimalProperties);

        URI uri = parameters.getHttpUri();
        assertEquals(uri.getPort(), 8080);
        assertEquals(uri.getScheme(), "https");

        Properties properties = parameters.getConnectionProperties();
        assertEquals(properties.getProperty(SSL_TRUST_STORE_PATH.getKey()), "truststore.jks");
        assertEquals(properties.getProperty(SSL_TRUST_STORE_PWD.getKey()), "password");
    }

    @Test
    public void testUriWithSslDisabled()
            throws SQLException
    {
        PrestoConnectionConfig parameters = new PrestoConnectionConfig("presto://localhost:8080/blackhole?SSL=0", minimalProperties);

        URI uri = parameters.getHttpUri();
        assertEquals(uri.getPort(), 8080);
        assertEquals(uri.getScheme(), "http");
    }

    // This is to ensure consistency with the closed-source JDBC driver provided by Teradata.
    @Test
    public void testUriOverridesProperties()
            throws SQLException
    {
        PrestoConnectionConfig parameters = new PrestoConnectionConfig("presto://localhost:8080/blackhole?user=MissJanet", minimalProperties);
        Properties properties = parameters.getConnectionProperties();
        assertEquals(properties.getProperty("user"), "MissJanet");
    }
}
