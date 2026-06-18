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

import com.facebook.presto.Session;
import com.facebook.presto.common.type.TimeZoneKey;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;

@Test
public class TestHiveAnalyze
        extends AbstractTestQueryFramework
{
    private static final String CATALOG = "hive";
    private static final String SCHEMA = "test_analyze_schema";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session session = testSessionBuilder().setCatalog(CATALOG).setSchema(SCHEMA).setTimeZoneKey(TimeZoneKey.UTC_KEY).build();
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session).setExtraProperties(ImmutableMap.<String, String>builder().build()).build();

        queryRunner.installPlugin(new HivePlugin(CATALOG));
        Path catalogDirectory = queryRunner.getCoordinator().getDataDirectory().resolve("hive_data").getParent().resolve("catalog");
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("hive.metastore", "file")
                .put("hive.metastore.catalog.dir", catalogDirectory.toFile().toURI().toString())
                .put("hive.allow-drop-table", "true")
                .put("hive.non-managed-table-writes-enabled", "true")
                .put("hive.parquet.use-column-names", "true")
                .build();

        queryRunner.createCatalog(CATALOG, CATALOG, properties);
        queryRunner.execute(format("CREATE SCHEMA %s.%s", CATALOG, SCHEMA));

        return queryRunner;
    }

    @Test
    public void testAnalyzePartitionedTableWithNonCanonicalValues()
            throws IOException
    {
        String tableName = "test_analyze_table_canonicalization";
        assertUpdate(format("CREATE TABLE %s (a_varchar varchar, month varchar) WITH (partitioned_by = ARRAY['month'], external_location='%s')", tableName, com.google.common.io.Files.createTempDir().getPath()));

        assertUpdate(format("INSERT INTO %s VALUES ('A', '01'), ('B', '01'), ('C', '02'), ('D', '03')", tableName), 4);

        String tableLocation = (String) computeActual(format("SELECT DISTINCT regexp_replace(\"$path\", '/[^/]*/[^/]*$', '') FROM %s", tableName)).getOnlyValue();

        String externalTableName = "external_" + tableName;
        assertUpdate(format(
                "CREATE TABLE %s (a_varchar varchar, month integer) WITH (partitioned_by = ARRAY['month'], external_location='%s')", externalTableName, tableLocation));

        assertUpdate(format("CALL system.sync_partition_metadata('%s', '%s', 'ADD')", SCHEMA, externalTableName));
        assertQuery(format("SELECT * FROM \"%s$partitions\"", externalTableName), "SELECT * FROM VALUES 1, 2, 3");
        assertUpdate(format("ANALYZE %s", externalTableName), 4);
        assertQuery(format("SHOW STATS FOR %s", externalTableName),
                "SELECT * FROM VALUES " +
                        "('a_varchar', 4.0, 2.0, 0.0, null, null, null, null), " +
                        "('month', null, 3.0, 0.0, null, 1, 3, null), " +
                        "(null, null, null, null, 4.0, null, null, null)");

        assertUpdate(format("INSERT INTO %s VALUES ('E', '04')", tableName), 1);
        assertUpdate(format("CALL system.sync_partition_metadata('%s', '%s', 'ADD')", SCHEMA, externalTableName));
        assertQuery(format("SELECT * FROM \"%s$partitions\"", externalTableName), "SELECT * FROM VALUES 1, 2, 3, 4");
        assertUpdate(format("ANALYZE %s WITH (partitions = ARRAY[ARRAY['04']])", externalTableName), 1);
        assertQuery(format("SHOW STATS FOR %s", externalTableName),
                "SELECT * FROM VALUES " +
                        "('a_varchar', 5.0, 2.0, 0.0, null, null, null, null), " +
                        "('month', null, 4.0, 0.0, null, 1, 4, null), " +
                        "(null, null, null, null, 5.0, null, null, null)");
        // TODO fix selective ANALYZE for table with non-canonical partition values
        assertQueryFails(format("ANALYZE %s WITH (partitions = ARRAY[ARRAY['4']])", externalTableName),
                format("Partition no longer exists: %s.%s/month=4", SCHEMA, externalTableName));

        assertUpdate(format("DROP TABLE %s", tableName));
        assertUpdate(format("DROP TABLE %s", externalTableName));
    }

    @Test
    public void testAnalyzePartitionedTableWithNonCanonicalValuesUnsupportedScenario()
            throws IOException
    {
        String tableName = "test_analyze_table_canonicalization_unsupported";
        assertUpdate(format("CREATE TABLE %s (a_varchar varchar, month varchar) WITH (partitioned_by = ARRAY['month'], external_location='%s')", tableName, com.google.common.io.Files.createTempDir().getPath()));

        assertUpdate(format("INSERT INTO %s VALUES ('A', '1'), ('B', '01'), ('C', '001'), ('D', '02')", tableName), 4);

        String tableLocation = (String) computeActual(format("SELECT DISTINCT regexp_replace(\"$path\", '/[^/]*/[^/]*$', '') FROM %s", tableName)).getOnlyValue();

        String externalTableName = "external_" + tableName;
        assertUpdate(format(
                "CREATE TABLE %s (a_varchar varchar, month integer) WITH (partitioned_by = ARRAY['month'], external_location='%s')", externalTableName, tableLocation));

        assertUpdate(format("CALL system.sync_partition_metadata('%s', '%s', 'ADD')", SCHEMA, externalTableName));
        assertQuery(format("SELECT * FROM \"%s$partitions\"", externalTableName), "SELECT * FROM VALUES 1, 1, 1, 2");

        assertQueryFails(format("ANALYZE %s", externalTableName),
                "There are multiple variants of the same partition, e.g. p=1, p=01, p=001. All partitions must follow the same key=value representation");

        assertUpdate(format("DROP TABLE %s", tableName));
        assertUpdate(format("DROP TABLE %s", externalTableName));
    }
}
