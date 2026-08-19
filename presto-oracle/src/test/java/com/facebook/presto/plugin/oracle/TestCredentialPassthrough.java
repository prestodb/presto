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
package com.facebook.presto.plugin.oracle;

import com.facebook.presto.Session;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;

import static com.facebook.airlift.testing.Closeables.closeAllRuntimeException;
import static com.facebook.airlift.testing.Closeables.closeAllSuppress;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;

/**
 * Regression test for the Oracle connector's connection factory: per-session extraCredentials
 * (user-credential-name / password-credential-name) must be honored, not silently ignored.
 * connection-user / connection-password are deliberately left unset in the catalog properties, so
 * the positive-path query below can only succeed via the session's extraCredentials.
 */
public class TestCredentialPassthrough
{
    private OracleServerTester oracleServer;
    private QueryRunner oracleQueryRunner;

    @BeforeClass
    public void createQueryRunner()
            throws Exception
    {
        oracleServer = new OracleServerTester();
        oracleQueryRunner = createQueryRunner(oracleServer);
    }

    @AfterClass(alwaysRun = true)
    public void destroy()
    {
        closeAllRuntimeException(oracleQueryRunner, oracleServer);
    }

    @Test
    public void testCredentialPassthrough()
    {
        Session session = getSession(true);
        oracleQueryRunner.execute(session, "CREATE TABLE test_create (a bigint)");
        oracleQueryRunner.execute(session, "DROP TABLE test_create");
    }

    @Test(expectedExceptions = RuntimeException.class)
    public void testCredentialPassthroughFailsWithoutExtraCredentials()
    {
        // Uses a separate catalog (registered fresh here, never previously authenticated) rather
        // than the "oracle" catalog the positive test already used, so this can't accidentally pass
        // by reusing an already-authenticated pooled connection instead of actually requiring
        // extraCredentials.
        oracleQueryRunner.createCatalog("oracle_negative", "oracle", catalogProperties(oracleServer));
        oracleQueryRunner.execute(getSession("oracle_negative", false), "CREATE TABLE test_create_negative (a bigint)");
    }

    private static QueryRunner createQueryRunner(OracleServerTester oracleServer)
            throws Exception
    {
        DistributedQueryRunner queryRunner = null;
        try {
            queryRunner = DistributedQueryRunner.builder(testSessionBuilder().build()).build();
            queryRunner.installPlugin(new OraclePlugin());
            queryRunner.createCatalog("oracle", "oracle", catalogProperties(oracleServer));

            return queryRunner;
        }
        catch (Exception e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    private static Map<String, String> catalogProperties(OracleServerTester oracleServer)
    {
        return ImmutableMap.<String, String>builder()
                .put("connection-url", oracleServer.getJdbcUrl())
                .put("user-credential-name", "oracle.user")
                .put("password-credential-name", "oracle.password")
                .put("allow-drop-table", "true")
                .build();
    }

    private static Session getSession(boolean withExtraCredentials)
    {
        return getSession("oracle", withExtraCredentials);
    }

    private static Session getSession(String catalog, boolean withExtraCredentials)
    {
        Map<String, String> extraCredentials = withExtraCredentials
                ? ImmutableMap.of("oracle.user", OracleServerTester.TEST_USER, "oracle.password", OracleServerTester.TEST_PASS)
                : ImmutableMap.of();
        return testSessionBuilder()
                .setCatalog(catalog)
                .setSchema(OracleServerTester.TEST_SCHEMA)
                .setIdentity(new Identity(
                        OracleServerTester.TEST_USER,
                        Optional.empty(),
                        ImmutableMap.of(),
                        extraCredentials,
                        ImmutableMap.of(),
                        Optional.empty(),
                        Optional.empty()))
                .build();
    }
}
