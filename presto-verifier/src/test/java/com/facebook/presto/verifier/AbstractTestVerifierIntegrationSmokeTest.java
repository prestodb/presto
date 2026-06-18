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
package com.facebook.presto.verifier;

import com.facebook.presto.testing.mysql.TestingMySqlServer;
import com.facebook.presto.tests.StandaloneQueryRunner;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.CharSink;
import org.jdbi.v3.core.Handle;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Map;
import java.util.stream.Collectors;

import static com.facebook.airlift.testing.Closeables.closeQuietly;
import static com.facebook.presto.verifier.VerifierTestUtil.getHandle;
import static com.facebook.presto.verifier.VerifierTestUtil.insertSourceQuery;
import static com.facebook.presto.verifier.VerifierTestUtil.setupMySql;
import static com.facebook.presto.verifier.VerifierTestUtil.setupPresto;
import static com.google.common.io.Files.asCharSink;
import static com.google.common.io.Files.asCharSource;
import static com.google.common.io.Files.createTempDir;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.createFile;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public abstract class AbstractTestVerifierIntegrationSmokeTest
{
    private static final String XDB = "presto";
    private static final String TEST_ID = "test";
    private static final String SUITE = "integration_test";

    private static StandaloneQueryRunner queryRunner;
    private static TestingMySqlServer mySqlServer;
    private static Handle handle;

    private final Map<String, String> additionalConfigurationProperties;

    private File configDirectory;
    private File jsonLogFile;
    private File humanReadableLogFile;

    public AbstractTestVerifierIntegrationSmokeTest(Map<String, String> additionalConfigurationProperties)
    {
        this.additionalConfigurationProperties = ImmutableMap.copyOf(additionalConfigurationProperties);
    }

    protected abstract void runVerifier();

    @BeforeClass
    public void setup()
            throws Exception
    {
        queryRunner = setupPresto();
        queryRunner.execute("CREATE SCHEMA local");
        mySqlServer = setupMySql();
        handle = getHandle(mySqlServer);

        // Create test cases
        queryRunner.execute("CREATE TABLE table1 AS SELECT * FROM (VALUES (1, '2', 3.0), (2, '3', 4.0)) t(a, b, c)");
        queryRunner.execute("CREATE TABLE table2 AS SELECT * FROM (VALUES (1, '2', ARRAY[-3, -1]), (2, '3', ARRAY[-2, -1,-3])) t(a, b, c)");
        insertSourceQuery(handle, SUITE, "query_1", "SELECT * FROM table1");
        insertSourceQuery(handle, SUITE, "query_2", "SELECT * FROM table1 CROSS JOIN table2");
    }

    @AfterClass(alwaysRun = true)
    public void destroy()
    {
        closeQuietly(queryRunner, mySqlServer, handle);
    }

    @BeforeMethod
    public void generateConfigFile()
            throws IOException
    {
        configDirectory = createTempDir();
        File configFile = createFile(Paths.get(configDirectory.getAbsolutePath(), "config.properties")).toFile();
        CharSink sink = asCharSink(configFile, UTF_8);

        String host = queryRunner.getServer().getAddress().getHost();
        int port = queryRunner.getServer().getAddress().getPort();
        jsonLogFile = Paths.get(configDirectory.getAbsolutePath(), "json.log").toFile();
        humanReadableLogFile = Paths.get(configDirectory.getAbsolutePath(), "human-readable.log").toFile();
        Map<String, String> configurationProperties = ImmutableMap.<String, String>builder()
                .put("test-id", TEST_ID)
                .put("control.hosts", format("%s,%s", host, host))
                .put("control.jdbc-port", String.valueOf(port))
                .put("test.hosts", host)
                .put("test.jdbc-port", String.valueOf(port))
                .put("test.http-port", String.valueOf(port))
                .put("control.table-prefix", "local.tmp_verifier_c")
                .put("test.table-prefix", "local.tmp_verifier_t")
                .put("source-query.database", mySqlServer.getJdbcUrl(XDB))
                .put("source-query.suites", SUITE)
                .put("source-query.max-queries-per-suite", "100")
                .put("control.username-override", "verifier-test")
                .put("test.username-override", "verifier-test")
                .put("event-clients", "json,human-readable")
                .put("json.log-file", jsonLogFile.getAbsolutePath())
                .put("human-readable.log-file", humanReadableLogFile.getAbsolutePath())
                .put("max-concurrency", "50")
                .put("failure-resolver.enabled", "true")
                .putAll(additionalConfigurationProperties)
                .build();
        sink.write(configurationProperties.entrySet().stream()
                .map(entry -> format("%s=%s", entry.getKey(), entry.getValue()))
                .collect(Collectors.joining("\n")));
        System.setProperty("config", configFile.getAbsolutePath());
    }

    @AfterMethod
    public void cleanupFiles()
            throws IOException
    {
        deleteRecursively(configDirectory.toPath(), ALLOW_INSECURE);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testVerifier()
            throws IOException
    {
        assertFalse(humanReadableLogFile.exists());
        assertFalse(jsonLogFile.exists());

        runVerifier();

        assertTrue(humanReadableLogFile.exists());
        assertTrue(jsonLogFile.exists());

        ObjectMapper mapper = new ObjectMapper();
        for (String line : asCharSource(jsonLogFile, UTF_8).readLines()) {
            Map<String, Object> event = mapper.readValue(line, new TypeReference<Map<String, Object>>() {});
            assertEquals(((Map<String, Object>) event.get("data")).get("status"), "SUCCEEDED");
        }
    }
}
