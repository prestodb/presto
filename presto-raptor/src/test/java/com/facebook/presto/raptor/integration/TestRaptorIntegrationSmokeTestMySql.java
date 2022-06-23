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
package com.facebook.presto.raptor.integration;

import com.facebook.presto.raptor.RaptorPlugin;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.testing.mysql.MySqlOptions;
import com.facebook.presto.testing.mysql.TestingMySqlServer;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tpch.TpchPlugin;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import static com.facebook.presto.raptor.RaptorQueryRunner.copyTables;
import static com.facebook.presto.raptor.RaptorQueryRunner.createSession;
import static java.util.concurrent.TimeUnit.SECONDS;

@Test(singleThreaded = true)
public class TestRaptorIntegrationSmokeTestMySql
        extends TestRaptorIntegrationSmokeTest
{
    private static final MySqlOptions MY_SQL_OPTIONS = MySqlOptions.builder()
            .setCommandTimeout(new Duration(90, SECONDS))
            .build();

    private final TestingMySqlServer mysqlServer;

    public TestRaptorIntegrationSmokeTestMySql()
            throws Exception
    {
        this.mysqlServer = new TestingMySqlServer("testuser", "testpass", ImmutableList.of("testdb"), MY_SQL_OPTIONS);
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createRaptorMySqlQueryRunner(mysqlServer.getJdbcUrl("testdb"));
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
            throws IOException
    {
        mysqlServer.close();
    }

    private static DistributedQueryRunner createRaptorMySqlQueryRunner(String mysqlUrl)
            throws Exception
    {
        DistributedQueryRunner queryRunner = new DistributedQueryRunner(createSession("tpch"), 2);

        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch");

        queryRunner.installPlugin(new RaptorPlugin());
        File baseDir = queryRunner.getCoordinator().getBaseDataDir().toFile();
        Map<String, String> raptorProperties = ImmutableMap.<String, String>builder()
                .put("metadata.db.type", "mysql")
                .put("metadata.db.url", mysqlUrl)
                .put("storage.data-directory", new File(baseDir, "data").toURI().toString())
                .put("storage.max-shard-rows", "2000")
                .put("backup.provider", "file")
                .put("raptor.startup-grace-period", "10s")
                .put("backup.directory", new File(baseDir, "backup").getAbsolutePath())
                .build();

        queryRunner.createCatalog("raptor", "raptor", raptorProperties);

        copyTables(queryRunner, "tpch", createSession(), false);

        return queryRunner;
    }
}
