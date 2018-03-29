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

import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.sql.parser.SqlParserOptions;
import com.facebook.presto.tpch.TpchPlugin;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logging;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Base64;
import java.util.Map;
import java.util.Properties;

import static com.facebook.presto.jdbc.TestPrestoDriver.closeQuietly;
import static com.facebook.presto.jdbc.TestPrestoDriver.waitForNodeRefresh;
import static com.google.common.io.Resources.getResource;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestPrestoDriverAuth
{
    private static final String TEST_CATALOG = "test_catalog";
    private static final String JWT_KEY = Base64.getEncoder().encodeToString("jwt-signing-key".getBytes(US_ASCII));
    private TestingPrestoServer server;

    @BeforeClass
    public void setup()
            throws Exception
    {
        Logging.initialize();

        server = new TestingPrestoServer(
                true,
                ImmutableMap.<String, String>builder()
                        .put("http-server.authentication.type", "JWT")
                        .put("http.authentication.jwt.key", JWT_KEY)
                        .put("http-server.https.enabled", "true")
                        .put("http-server.https.keystore.path", getResource("localhost.keystore").getPath())
                        .put("http-server.https.keystore.key", "changeit")
                        .build(),
                null,
                null,
                new SqlParserOptions(),
                ImmutableList.of());
        server.installPlugin(new TpchPlugin());
        server.createCatalog(TEST_CATALOG, "tpch");
        waitForNodeRefresh(server);
    }

    @AfterClass(alwaysRun = true)
    public void teardown()
    {
        closeQuietly(server);
    }

    @Test
    public void testSuccess()
            throws Exception
    {
        String accessToken = Jwts.builder()
                .setSubject("test")
                .signWith(SignatureAlgorithm.HS512, JWT_KEY)
                .compact();

        try (Connection connection = createConnection(ImmutableMap.of("accessToken", accessToken))) {
            try (Statement statement = connection.createStatement()) {
                assertTrue(statement.execute("SELECT 123"));
                ResultSet rs = statement.getResultSet();
                assertTrue(rs.next());
                assertEquals(rs.getLong(1), 123);
                assertFalse(rs.next());
            }
        }
    }

    @Test(expectedExceptions = SQLException.class, expectedExceptionsMessageRegExp = "Authentication failed: Unauthorized")
    public void testFailedNoToken()
            throws Exception
    {
        try (Connection connection = createConnection(ImmutableMap.of())) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("SELECT 123");
            }
        }
    }

    @Test(expectedExceptions = SQLException.class, expectedExceptionsMessageRegExp = "Authentication failed: JWT signature does not match.*")
    public void testFailedBadToken()
            throws Exception
    {
        String accessToken = Jwts.builder()
                .setSubject("test")
                .signWith(SignatureAlgorithm.HS512, Base64.getEncoder().encodeToString("bad-key".getBytes(US_ASCII)))
                .compact();

        try (Connection connection = createConnection(ImmutableMap.of("accessToken", accessToken))) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("SELECT 123");
            }
        }
    }

    private Connection createConnection(Map<String, String> additionalProperties)
            throws SQLException
    {
        String url = format("jdbc:presto://localhost:%s", server.getHttpsAddress().getPort());
        Properties properties = new Properties();
        properties.setProperty("user", "test");
        properties.setProperty("SSL", "true");
        properties.setProperty("SSLTrustStorePath", getResource("localhost.truststore").getPath());
        properties.setProperty("SSLTrustStorePassword", "changeit");
        properties.putAll(additionalProperties);
        return DriverManager.getConnection(url, properties);
    }
}
