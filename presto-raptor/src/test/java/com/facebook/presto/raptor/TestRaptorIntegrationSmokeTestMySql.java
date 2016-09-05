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
package com.facebook.presto.raptor;

import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tpch.TpchPlugin;
import com.facebook.presto.tpch.testing.SampledTpchPlugin;
import com.google.common.collect.ImmutableMap;
import io.airlift.testing.mysql.TestingMySqlServer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Map;

import static com.facebook.presto.raptor.RaptorQueryRunner.copyTables;
import static com.facebook.presto.raptor.RaptorQueryRunner.createSampledSession;
import static com.facebook.presto.raptor.RaptorQueryRunner.createSession;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.testing.Closeables.closeAllRuntimeException;
import static java.lang.String.format;

@Test
public class TestRaptorIntegrationSmokeTestMySql
        extends TestRaptorIntegrationSmokeTest
{
    private final TestingMySqlServer mysqlServer;

    public TestRaptorIntegrationSmokeTestMySql()
            throws Exception
    {
        this(new TestingMySqlServer("testuser", "testpass", "testdb"));
    }

    public TestRaptorIntegrationSmokeTestMySql(TestingMySqlServer mysqlServer)
            throws Exception
    {
        super(createRaptorMySqlQueryRunner(mysqlServer));
        this.mysqlServer = mysqlServer;
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
    {
        closeAllRuntimeException(mysqlServer);
    }

    private static DistributedQueryRunner createRaptorMySqlQueryRunner(TestingMySqlServer server)
            throws Exception
    {
        DistributedQueryRunner queryRunner = new DistributedQueryRunner(createSession("tpch"), 2);

        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch");

        queryRunner.installPlugin(new SampledTpchPlugin());
        queryRunner.createCatalog("tpch_sampled", "tpch_sampled");

        queryRunner.installPlugin(new RaptorPlugin());
        File baseDir = queryRunner.getCoordinator().getBaseDataDir().toFile();
        Map<String, String> raptorProperties = ImmutableMap.<String, String>builder()
                .put("metadata.db.type", "mysql")
                .put("metadata.db.url", jdbcUrl(server))
                .put("storage.data-directory", new File(baseDir, "data").getAbsolutePath())
                .put("storage.max-shard-rows", "2000")
                .put("backup.provider", "file")
                .put("backup.directory", new File(baseDir, "backup").getAbsolutePath())
                .build();

        queryRunner.createCatalog("raptor", "raptor", raptorProperties);

        copyTables(queryRunner, "tpch", createSession(), false);
        copyTables(queryRunner, "tpch_sampled", createSampledSession(), false);

        return queryRunner;
    }

    private static String jdbcUrl(TestingMySqlServer server)
    {
        return format("jdbc:mysql://localhost:%d/%s?user=%s&password=%s",
                server.getPort(),
                getOnlyElement(server.getDatabases()),
                server.getUser(),
                server.getPassword());
    }
}
