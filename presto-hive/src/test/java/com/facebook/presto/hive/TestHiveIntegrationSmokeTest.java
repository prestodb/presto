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
package com.facebook.presto.hive;

import com.facebook.presto.hive.metastore.InMemoryHiveMetastore;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestIntegrationSmokeTest;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tpch.TpchMetadata;
import com.facebook.presto.tpch.TpchPlugin;
import com.facebook.presto.tpch.testing.SampledTpchPlugin;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import org.apache.hadoop.hive.metastore.api.Database;
import org.joda.time.DateTime;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.File;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Map;

import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.tests.QueryAssertions.copyTable;
import static io.airlift.units.Duration.nanosSince;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.joda.time.DateTimeZone.UTC;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class TestHiveIntegrationSmokeTest
        extends AbstractTestIntegrationSmokeTest
{
    private static final Logger log = Logger.get("TestQueries");
    private static final String TPCH_SAMPLED_SCHEMA = "tpch_sampled";

    public TestHiveIntegrationSmokeTest()
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

        try {
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");

            queryRunner.installPlugin(new SampledTpchPlugin());
            queryRunner.createCatalog("tpch_sampled", "tpch_sampled");

            File baseDir = queryRunner.getCoordinator().getBaseDataDir().toFile();
            InMemoryHiveMetastore metastore = new InMemoryHiveMetastore();
            metastore.createDatabase(new Database("tpch", null, new File(baseDir, "tpch").toURI().toString(), null));
            metastore.createDatabase(new Database("tpch_sampled", null, new File(baseDir, "tpch_sampled").toURI().toString(), null));

            queryRunner.installPlugin(new HivePlugin("hive", metastore));
            Map<String, String> hiveProperties = ImmutableMap.<String, String>builder()
                    .put("hive.metastore.uri", "thrift://localhost:8080")
                    .put("hive.allow-drop-table", "true")
                    .build();
            queryRunner.createCatalog("hive", "hive", hiveProperties);

            log.info("Loading data...");
            long startTime = System.nanoTime();
            copyTable(queryRunner, "tpch", TpchMetadata.TINY_SCHEMA_NAME, "orders", createSession("tpch"));
            copyTable(queryRunner, "tpch_sampled", TpchMetadata.TINY_SCHEMA_NAME, "orders", createSession(TPCH_SAMPLED_SCHEMA));
            log.info("Loading complete in %s", nanosSince(startTime).toString(SECONDS));

            return queryRunner;
        }
        catch (Exception e) {
            queryRunner.close();
            throw e;
        }
    }

    private static ConnectorSession createSession(String schema)
    {
        return new ConnectorSession("user", "test", "hive", schema, UTC_KEY, ENGLISH, null, null);
    }

    @Test
    public void createTableWithEveryType()
            throws Exception
    {
        String query = "" +
                "CREATE TABLE test_types_table AS " +
                "SELECT" +
                " 'foo' _varchar" +
                ", cast('bar' as varbinary) _varbinary" +
                ", 1 _bigint" +
                ", 3.14 _double" +
                ", true _boolean" +
                ", DATE '1980-05-07' _date" +
                ", TIMESTAMP '1980-05-07 11:22:33.456' _timestamp";

        assertQuery(query, "SELECT 1");

        MaterializedResult results = queryRunner.execute(getSession(), "SELECT * FROM test_types_table").toJdbcTypes();
        assertEquals(results.getRowCount(), 1);
        MaterializedRow row = results.getMaterializedRows().get(0);
        assertEquals(row.getField(0), "foo");
        assertEquals(row.getField(1), "bar".getBytes(UTF_8));
        assertEquals(row.getField(2), 1L);
        assertEquals(row.getField(3), 3.14);
        assertEquals(row.getField(4), true);
        assertEquals(row.getField(5), new Date(new DateTime(1980, 5, 7, 0, 0, 0, UTC).getMillis()));
        assertEquals(row.getField(6), new Timestamp(new DateTime(1980, 5, 7, 11, 22, 33, 456, UTC).getMillis()));
        assertQueryTrue("DROP TABLE test_types_table");

        assertFalse(queryRunner.tableExists(getSession(), "test_types_table"));
    }
}
