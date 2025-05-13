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
package com.facebook.presto.delta;

import com.facebook.presto.Session;
import com.facebook.presto.common.type.TimeZoneKey;
import com.facebook.presto.hive.HivePlugin;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tpch.TpchPlugin;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.nio.file.Path;
import java.util.Map;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static java.util.Locale.US;
import static org.testng.Assert.assertEquals;

public class TestUppercasePartitionColumns
        extends AbstractDeltaDistributedQueryTestBase
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createDeltaQueryRunner(ImmutableMap.of(
                "experimental.pushdown-subfields-enabled", "true",
                "experimental.pushdown-dereference-enabled", "true"));
    }

    private static DistributedQueryRunner createDeltaQueryRunner(Map<String, String> extraProperties)
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog(DELTA_CATALOG)
                .setSchema(DELTA_SCHEMA.toLowerCase(US))
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey("Europe/Madrid"))
                .build();

        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session)
                .setExtraProperties(extraProperties)
                .build();

        // Install the TPCH plugin for test data (not in Delta format)
        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch");

        Path dataDirectory = queryRunner.getCoordinator().getDataDirectory().resolve("delta_metadata");
        Path catalogDirectory = dataDirectory.getParent().resolve("catalog");

        // Install a Delta connector catalog
        queryRunner.installPlugin(new DeltaPlugin());
        Map<String, String> deltaProperties = ImmutableMap.<String, String>builder()
                .put("hive.metastore", "file")
                .put("hive.metastore.catalog.dir", catalogDirectory.toFile().toURI().toString())
                .put("delta.case-sensitive-partitions-enabled", "true")
                .build();
        queryRunner.createCatalog(DELTA_CATALOG, "delta", deltaProperties);

        // Install a Hive connector catalog that uses the same metastore as Delta
        // This catalog will be used to create tables in metastore as the Delta connector doesn't
        // support creating tables yet.
        queryRunner.installPlugin(new HivePlugin("hive"));
        Map<String, String> hiveProperties = ImmutableMap.<String, String>builder()
                .put("hive.metastore", "file")
                .put("hive.metastore.catalog.dir", catalogDirectory.toFile().toURI().toString())
                .put("hive.allow-drop-table", "true")
                .put("hive.security", "legacy")
                .build();
        queryRunner.createCatalog(HIVE_CATALOG, "hive", hiveProperties);
        queryRunner.execute(format("CREATE SCHEMA %s.%s", HIVE_CATALOG, DELTA_SCHEMA));

        return queryRunner;
    }

    @Test(dataProvider = "deltaReaderVersions")
    public void readDeltaTableWithUpperCaseRegularColumnTest(String version)
    {
        String controlTableQuery = format("select * from \"%s\".\"%s\" order by id asc",
                PATH_SCHEMA, goldenTablePathWithPrefix(version, "test-lowercase"));
        String testTableQuery = format("select * from \"%s\".\"%s\" order by id asc",
                PATH_SCHEMA, goldenTablePathWithPrefix(version, "test-uppercase"));
        MaterializedResult expectedResult = getQueryRunner().execute(controlTableQuery);
        MaterializedResult testResult = getQueryRunner().execute(testTableQuery);
        assertEquals(testResult.getMaterializedRows(), expectedResult.getMaterializedRows());
    }

    @Test(dataProvider = "deltaReaderVersions")
    public void readDeltaTableWithUpperCasePartitionValuesTest(String version)
    {
        String controlTableQuery = format("select * from \"%s\".\"%s\" order by id asc",
                PATH_SCHEMA, goldenTablePathWithPrefix(version, "test-partitions-lowercase"));
        String testTableQuery = format("select * from \"%s\".\"%s\" order by id asc", PATH_SCHEMA,
                goldenTablePathWithPrefix(version, "test-partitions-uppercase"));
        MaterializedResult expectedResult = getQueryRunner().execute(controlTableQuery);
        MaterializedResult testResult = getQueryRunner().execute(testTableQuery);
        assertEquals(testResult.getMaterializedRows(), expectedResult.getMaterializedRows());
    }
}
