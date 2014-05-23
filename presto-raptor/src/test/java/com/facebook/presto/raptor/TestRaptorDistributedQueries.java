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

import com.facebook.presto.metadata.QualifiedTableName;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestDistributedQueries;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tpch.TpchMetadata;
import com.facebook.presto.tpch.TpchPlugin;
import com.facebook.presto.tpch.testing.SampledTpchPlugin;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;

import java.io.File;
import java.util.Map;

import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.util.Types.checkType;
import static io.airlift.units.Duration.nanosSince;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestRaptorDistributedQueries
        extends AbstractTestDistributedQueries
{
    private static final Logger log = Logger.get("TestQueries");
    private static final String TPCH_SAMPLED_SCHEMA = "tpch_sampled";

    public TestRaptorDistributedQueries()
            throws Exception
    {
        super(createQueryRunner(), createSession(TPCH_SAMPLED_SCHEMA));
    }

    @AfterClass(alwaysRun = true)
    public void destroy()
    {
        queryRunner.close();
    }

    private static QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = new DistributedQueryRunner(createSession("tpch"), 4);

        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch");

        queryRunner.installPlugin(new SampledTpchPlugin());
        queryRunner.createCatalog("tpch_sampled", "tpch_sampled");

        queryRunner.installPlugin(new RaptorPlugin());
        File baseDir = queryRunner.getCoordinator().getBaseDataDir().toFile();
        Map<String, String> raptorProperties = ImmutableMap.<String, String>builder()
                .put("metadata.db.type", "h2")
                .put("metadata.db.filename", new File(baseDir, "db").getAbsolutePath())
                .put("storage.data-directory", new File(baseDir, "data").getAbsolutePath())
                .build();

        queryRunner.createCatalog("default", "raptor", raptorProperties);

        log.info("Loading data...");
        long startTime = System.nanoTime();
        distributeData(queryRunner, "tpch", TpchMetadata.TINY_SCHEMA_NAME, createSession("tpch"));
        distributeData(queryRunner, "tpch_sampled", TpchMetadata.TINY_SCHEMA_NAME, createSession(TPCH_SAMPLED_SCHEMA));
        log.info("Loading complete in %s", nanosSince(startTime).toString(SECONDS));

        return queryRunner;
    }

    private static void distributeData(QueryRunner queryRunner, String catalog, String schema, ConnectorSession session)
            throws Exception
    {
        for (QualifiedTableName table : queryRunner.listTables(session, catalog, schema)) {
            if (table.getTableName().equalsIgnoreCase("dual")) {
                continue;
            }
            log.info("Running import for %s", table.getTableName());
            @Language("SQL") String sql = format("CREATE TABLE %s AS SELECT * FROM %s", table.getTableName(), table);
            long rows = checkType(queryRunner.execute(session, sql).getMaterializedRows().get(0).getField(0), Long.class, "rows");
            log.info("Imported %s rows for %s", rows, table.getTableName());
        }
    }

    private static ConnectorSession createSession(String schema)
    {
        return new ConnectorSession("user", "test", "default", schema, UTC_KEY, ENGLISH, null, null);
    }
}
