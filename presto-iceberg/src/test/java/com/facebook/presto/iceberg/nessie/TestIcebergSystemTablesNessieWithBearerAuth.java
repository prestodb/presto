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
package com.facebook.presto.iceberg.nessie;

import com.facebook.presto.Session;
import com.facebook.presto.iceberg.IcebergConfig;
import com.facebook.presto.iceberg.IcebergPlugin;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.testing.containers.KeycloakContainer;
import com.facebook.presto.testing.containers.NessieContainer;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;
import org.testcontainers.containers.Network;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.nio.file.Path;
import java.util.Map;

import static com.facebook.presto.iceberg.CatalogType.NESSIE;
import static com.facebook.presto.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static com.facebook.presto.iceberg.IcebergQueryRunner.getIcebergDataDirectoryPath;
import static com.facebook.presto.iceberg.nessie.NessieTestUtil.nessieConnectorProperties;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;

public class TestIcebergSystemTablesNessieWithBearerAuth
        extends TestIcebergSystemTablesNessie
{
    private NessieContainer nessieContainer;
    private KeycloakContainer keycloakContainer;

    @BeforeClass
    @Override
    public void init()
            throws Exception
    {
        Map<String, String> envVars = ImmutableMap.<String, String>builder()
                .putAll(NessieContainer.DEFAULT_ENV_VARS)
                .put("QUARKUS_OIDC_AUTH_SERVER_URL", KeycloakContainer.SERVER_URL + "/realms/" + KeycloakContainer.MASTER_REALM)
                .put("QUARKUS_OIDC_CLIENT_ID", "nessie")
                .put("NESSIE_SERVER_AUTHENTICATION_ENABLED", "true")
                .buildOrThrow();

        Network network = Network.newNetwork();

        nessieContainer = NessieContainer.builder().withEnvVars(envVars).withNetwork(network).build();
        nessieContainer.start();
        keycloakContainer = KeycloakContainer.builder().withNetwork(network).build();
        keycloakContainer.start();

        super.init();
    }

    @AfterClass(alwaysRun = true)
    @Override
    public void tearDown()
    {
        super.tearDown();
        if (nessieContainer != null) {
            nessieContainer.stop();
        }
        if (keycloakContainer != null) {
            keycloakContainer.stop();
        }
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog(ICEBERG_CATALOG)
                .build();

        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session).build();

        Path dataDirectory = queryRunner.getCoordinator().getDataDirectory();
        Path catalogDirectory = getIcebergDataDirectoryPath(dataDirectory, "NESSIE", new IcebergConfig().getFileFormat(), false);

        queryRunner.installPlugin(new IcebergPlugin());
        Map<String, String> icebergProperties = ImmutableMap.<String, String>builder()
                .put("iceberg.catalog.type", String.valueOf(NESSIE))
                .putAll(nessieConnectorProperties(nessieContainer.getRestApiUri()))
                .put("iceberg.catalog.warehouse", catalogDirectory.getParent().toFile().toURI().toString())
                .put("iceberg.nessie.auth.type", "BEARER")
                .put("iceberg.nessie.auth.bearer.token", keycloakContainer.getAccessToken())
                .build();

        queryRunner.createCatalog(ICEBERG_CATALOG, "iceberg", icebergProperties);

        icebergProperties = ImmutableMap.<String, String>builder()
                .put("iceberg.catalog.type", String.valueOf(NESSIE))
                .putAll(nessieConnectorProperties(nessieContainer.getRestApiUri()))
                .put("iceberg.catalog.warehouse", catalogDirectory.getParent().toFile().toURI().toString())
                .put("iceberg.nessie.auth.type", "BEARER")
                .put("iceberg.nessie.auth.bearer.token", "invalid_token")
                .build();

        queryRunner.createCatalog("iceberg_invalid_credentials", "iceberg", icebergProperties);

        return queryRunner;
    }

    @Test
    public void testInvalidBearerToken()
    {
        assertQueryFails("CREATE SCHEMA iceberg_invalid_credentials.test_schema", "Unauthorized \\(HTTP/401\\).*", true);
    }
}
