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
package com.facebook.presto.plugin.secretsManager;

import com.facebook.presto.Session;
import com.facebook.presto.plugin.mysql.MySqlPlugin;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.secretsManager.SecretsManager;
import com.facebook.presto.spi.secretsManager.SecretsManagerFactory;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.testing.mysql.TestingMySqlServer;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import static com.facebook.airlift.testing.Closeables.closeAllSuppress;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.String.format;
import static org.testng.Assert.assertThrows;

@Test
public class TestVaultSecretsManagerIntegrationTest
{
    protected static final boolean isEnabled = TestVaultSecretsManagerConfig.isEnabled();
    private SecretsManager secretsManager;

    private static final String TEST_SCHEMA = "test_database";
    private TestingMySqlServer mysqlServer;
    private QueryRunner queryRunner;

    TestVaultSecretsManagerIntegrationTest() throws Exception
    {
        if (isEnabled) {
            Plugin plugin = new VaultSecretsManagerPlugin();
            SecretsManagerFactory factory = getOnlyElement(plugin.getSecretsManagerFactories());
            secretsManager = factory.create();
            mysqlServer = new TestingMySqlServer("testuser", "testpass", TEST_SCHEMA);
            queryRunner = createQueryRunner(mysqlServer, true);
        }
    }

    @Test
    public void testCatalogWithSecrets()
    {
        if (isEnabled) {
            queryRunner.execute(getSession(mysqlServer), "CREATE TABLE test_create (a bigint, b double, c varchar)");
        }
    }

    @Test
    public void testCatalogWithoutSecrets() throws Exception
    {
        if (isEnabled) {
            assertThrows(RuntimeException.class, () ->
                    createQueryRunner(mysqlServer, false).execute(getSession(mysqlServer), "CREATE TABLE test_create (a bigint, b double, c varchar)"));
        }
    }

    private QueryRunner createQueryRunner(TestingMySqlServer mySqlServer, boolean appendSecrets)
            throws Exception
    {
        DistributedQueryRunner queryRunner = null;
        try {
            queryRunner = DistributedQueryRunner.builder(testSessionBuilder().build()).build();
            queryRunner.installPlugin(new MySqlPlugin());

            //Fetch secrets
            final Map<String, Map<String, String>> secrets = secretsManager.fetchSecrets();
            Map<String, String> properties = ImmutableMap.<String, String>builder()
                    .put("connection-url", getConnectionUrl(mySqlServer))
                    .putAll(appendSecrets ? getOnlyElement(secrets.values()) : Collections.emptyMap())
                    .build();
            queryRunner.createCatalog("mysql", "mysql", properties);

            return queryRunner;
        }
        catch (Exception e) {
            closeAllSuppress(e, queryRunner, mySqlServer);
            throw e;
        }
    }

    private static String getConnectionUrl(TestingMySqlServer mySqlServer)
    {
        return format("jdbc:mysql://localhost:%s?useSSL=false&allowPublicKeyRetrieval=true", mySqlServer.getPort());
    }

    private static Session getSession(TestingMySqlServer mySqlServer)
    {
        Map<String, String> extraCredentials = ImmutableMap.of("mysql.user", mySqlServer.getUser(), "mysql.password", mySqlServer.getPassword());
        return testSessionBuilder()
                .setCatalog("mysql")
                .setSchema(TEST_SCHEMA)
                .setIdentity(new Identity(
                        mySqlServer.getUser(),
                        Optional.empty(),
                        ImmutableMap.of(),
                        extraCredentials,
                        ImmutableMap.of(),
                        Optional.empty(),
                        Optional.empty()))
                .build();
    }
}
