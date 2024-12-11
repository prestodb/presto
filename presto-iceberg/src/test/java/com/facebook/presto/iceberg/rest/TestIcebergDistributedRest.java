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
package com.facebook.presto.iceberg.rest;

import com.facebook.airlift.http.server.testing.TestingHttpServer;
import com.facebook.presto.Session;
import com.facebook.presto.iceberg.IcebergDistributedTestBase;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.testing.QueryRunner;
import com.google.common.collect.ImmutableMap;
import org.assertj.core.util.Files;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import static com.facebook.presto.iceberg.CatalogType.REST;
import static com.facebook.presto.iceberg.FileFormat.PARQUET;
import static com.facebook.presto.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static com.facebook.presto.iceberg.IcebergQueryRunner.createIcebergQueryRunner;
import static com.facebook.presto.iceberg.rest.IcebergRestTestUtil.getRestServer;
import static com.facebook.presto.iceberg.rest.IcebergRestTestUtil.restConnectorProperties;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Test
public class TestIcebergDistributedRest
        extends IcebergDistributedTestBase
{
    private TestingHttpServer restServer;
    private String serverUri;
    private File warehouseLocation;

    protected TestIcebergDistributedRest()
    {
        super(REST);
    }

    @Override
    protected Map<String, String> getProperties()
    {
        return ImmutableMap.of("uri", serverUri);
    }

    @BeforeClass
    @Override
    public void init()
            throws Exception
    {
        warehouseLocation = Files.newTemporaryFolder();

        restServer = getRestServer(warehouseLocation.getAbsolutePath());
        restServer.start();

        serverUri = restServer.getBaseUrl().toString();
        super.init();
    }

    @AfterClass
    public void tearDown()
            throws Exception
    {
        if (restServer != null) {
            restServer.stop();
        }
        deleteRecursively(warehouseLocation.toPath(), ALLOW_INSECURE);
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Map<String, String> connectorProperties = ImmutableMap.<String, String>builder()
                .putAll(restConnectorProperties(serverUri))
                .put("iceberg.rest.session.type", SessionType.USER.name())
                .build();

        return createIcebergQueryRunner(
                ImmutableMap.of(),
                connectorProperties,
                PARQUET,
                true,
                false,
                OptionalInt.empty(),
                Optional.of(warehouseLocation.toPath()));
    }

    @Test
    public void testDeleteOnV1Table()
    {
        // v1 table create fails due to Iceberg REST catalog bug (see: https://github.com/apache/iceberg/issues/8756)
        assertThatThrownBy(super::testDeleteOnV1Table)
                .isInstanceOf(RuntimeException.class)
                .hasMessageMatching("Cannot downgrade v2 table to v1");
    }

    @Test
    public void testRestUserSessionAuthorization()
    {
        // Query with default user should succeed
        assertQuerySucceeds(getSession(), "SHOW SCHEMAS");

        String unauthorizedUser = "unauthorized_user";
        Session unauthorizedUserSession = testSessionBuilder()
                .setCatalog(ICEBERG_CATALOG)
                .setUserAgent(unauthorizedUser)
                .setIdentity(new Identity(
                        unauthorizedUser,
                        Optional.empty(),
                        ImmutableMap.of(),
                        ImmutableMap.of(),
                        ImmutableMap.of(),
                        Optional.of(unauthorizedUser),
                        Optional.empty()))
                .build();

        // Query with different user should fail
        assertQueryFails(unauthorizedUserSession, "SHOW SCHEMAS", "Forbidden: User not authorized");
    }
}
