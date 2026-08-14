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
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;

import static com.facebook.airlift.testing.Closeables.closeAllSuppress;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;

/**
 * Regression test for the Oracle connector's connection factory: per-session extraCredentials
 * (user-credential-name / password-credential-name) must be honored, not silently ignored.
 * connection-user / connection-password are deliberately left unset in the catalog properties, so
 * the query below can only succeed via the session's extraCredentials.
 */
public class TestCredentialPassthrough
{
    private final OracleServerTester oracleServer;
    private final QueryRunner oracleQueryRunner;

    public TestCredentialPassthrough()
            throws Exception
    {
        oracleServer = new OracleServerTester();
        oracleQueryRunner = createQueryRunner(oracleServer);
    }

    @AfterClass(alwaysRun = true)
    public void destroy()
    {
        if (oracleServer != null) {
            oracleServer.close();
        }
    }

    @Test
    public void testCredentialPassthrough()
    {
        oracleQueryRunner.execute(getSession(), "CREATE TABLE test_create (a bigint)");
    }

    private static QueryRunner createQueryRunner(OracleServerTester oracleServer)
            throws Exception
    {
        DistributedQueryRunner queryRunner = null;
        try {
            queryRunner = DistributedQueryRunner.builder(testSessionBuilder().build()).build();
            queryRunner.installPlugin(new OraclePlugin());
            Map<String, String> properties = ImmutableMap.<String, String>builder()
                    .put("connection-url", oracleServer.getJdbcUrl())
                    .put("user-credential-name", "oracle.user")
                    .put("password-credential-name", "oracle.password")
                    .build();
            queryRunner.createCatalog("oracle", "oracle", properties);

            return queryRunner;
        }
        catch (Exception e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    private static Session getSession()
    {
        Map<String, String> extraCredentials = ImmutableMap.of(
                "oracle.user", OracleServerTester.TEST_USER,
                "oracle.password", OracleServerTester.TEST_PASS);
        return testSessionBuilder()
                .setCatalog("oracle")
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
