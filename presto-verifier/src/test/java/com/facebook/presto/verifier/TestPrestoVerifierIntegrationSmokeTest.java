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

import com.facebook.presto.tests.StandaloneQueryRunner;
import com.google.common.io.CharSink;
import io.airlift.airline.Cli;
import io.airlift.airline.Help;
import io.airlift.testing.mysql.TestingMySqlServer;
import org.jdbi.v3.core.Handle;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;

import static com.facebook.presto.verifier.VerifierTestUtil.getHandle;
import static com.facebook.presto.verifier.VerifierTestUtil.insertSourceQuery;
import static com.facebook.presto.verifier.VerifierTestUtil.setupMySql;
import static com.facebook.presto.verifier.VerifierTestUtil.setupPresto;
import static com.google.common.io.Files.asCharSink;
import static com.google.common.io.Files.createTempDir;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.airlift.testing.Closeables.closeQuietly;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.createFile;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestPrestoVerifierIntegrationSmokeTest
{
    private static final String XDB = "presto";
    private static final String TEST_ID = "test";
    private static final String SUITE = "integration_test";

    private static StandaloneQueryRunner queryRunner;
    private static TestingMySqlServer mySqlServer;
    private static Handle handle;

    private File configDirectory;
    private File configFile;
    private File jsonLogFile;
    private File humanReadableLogFile;

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
        configFile = createFile(Paths.get(configDirectory.getAbsolutePath(), "config.properties")).toFile();
        CharSink sink = asCharSink(configFile, UTF_8);

        String gateway = queryRunner.getServer().getBaseUrl().toString().replace("http", "jdbc:presto");
        jsonLogFile = Paths.get(configDirectory.getAbsolutePath(), "json.log").toFile();
        humanReadableLogFile = Paths.get(configDirectory.getAbsolutePath(), "human-readable.log").toFile();
        sink.write(format(
                "test-id=%s\n" +
                        "control.jdbc-url=%s\n" +
                        "test.jdbc-url=%s\n" +
                        "control.table-prefix=local.tmp_verifier_c\n" +
                        "test.table-prefix=local.tmp_verifier_t\n" +
                        "source-query.database=%s\n" +
                        "source-query.suites=%s\n" +
                        "source-query.max-queries-per-suite=100\n" +
                        "event-clients=json,human-readable\n" +
                        "json.log-file=%s\n" +
                        "human-readable.log-file=%s\n" +
                        "max-concurrency=50\n",
                TEST_ID,
                gateway,
                gateway,
                mySqlServer.getJdbcUrl(XDB),
                SUITE,
                jsonLogFile.getAbsolutePath(),
                humanReadableLogFile.getAbsolutePath()));
    }

    @AfterMethod
    public void cleanupFiles()
            throws IOException
    {
        deleteRecursively(configDirectory.toPath(), ALLOW_INSECURE);
    }

    @Test
    public void testVerifier()
    {
        assertFalse(humanReadableLogFile.exists());
        assertFalse(jsonLogFile.exists());

        Cli<Runnable> verifierParser = Cli.<Runnable>builder("verifier")
                .withDescription("Presto Verifier")
                .withDefaultCommand(Help.class)
                .withCommand(Help.class)
                .withCommand(PrestoVerifyCommand.class)
                .build();
        verifierParser.parse("verify", configFile.getAbsolutePath()).run();

        assertTrue(humanReadableLogFile.exists());
        assertTrue(jsonLogFile.exists());
    }
}
