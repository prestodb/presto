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
package com.facebook.presto.iceberg;

import com.facebook.presto.Session;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.nio.file.Path;
import java.util.Map;

import static com.facebook.presto.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;

public class TestIcebergTableVersion
        extends AbstractTestQueryFramework
{
    public static final String schemaName = "test_tt_schema";
    public static final String tab1 = "test_table_version_tab1";
    public static final String tab2 = "test_table_version_tab2";
    public static final String view1 = "test_table_version_view1";
    public static final String view2 = "test_table_version_view2";
    private static final String tableName1 = schemaName + "." + tab1;
    private static final String tableName2 = schemaName + "." + tab2;
    private static final String tableName3 = schemaName + "." + "tab1_version1";
    private static final String tableName4 = schemaName + "." + "tab2_version1";
    private static final String viewName1 = schemaName + "." + view1;
    private static final String viewName2 = schemaName + "." + view2;

    private static long tab1VersionId1;
    private static long tab1VersionId2;
    private static long tab1VersionId3;
    private static long tab2VersionId1;
    private static long tab2VersionId2;
    private static long tab2VersionId3;
    private static String tab1Timestamp1;
    private static String tab1Timestamp2;
    private static String tab1Timestamp3;
    private static String tab2Timestamp1;
    private static String tab2Timestamp2;
    private static String tab2Timestamp3;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog(ICEBERG_CATALOG)
                .build();
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session).build();

        Path catalogDirectory = queryRunner.getCoordinator().getDataDirectory().resolve("iceberg_data").resolve("catalog");

        queryRunner.installPlugin(new IcebergPlugin());
        Map<String, String> icebergProperties = ImmutableMap.<String, String>builder()
                .put("hive.metastore", "file")
                .put("hive.metastore.catalog.dir", catalogDirectory.toFile().toURI().toString())
                .build();

        queryRunner.createCatalog(ICEBERG_CATALOG, "iceberg", icebergProperties);

        return queryRunner;
    }

    @BeforeClass
    public void setUp()
    {
        assertQuerySucceeds("CREATE SCHEMA " + ICEBERG_CATALOG + "." + schemaName);

        assertUpdate("CREATE TABLE " + tableName1 + " (id integer, desc varchar) WITH(partitioning = ARRAY['id'])");
        assertUpdate("INSERT INTO " + tableName1 + " VALUES (1,'aaa')", 1);
        tab1VersionId1 = getLatestSnapshotId(tab1);
        tab1Timestamp1 = getLatestTimestampId(tab1);
        assertUpdate("INSERT INTO " + tableName1 + " VALUES (2,'bbb')", 1);
        tab1VersionId2 = getLatestSnapshotId(tab1);
        tab1Timestamp2 = getLatestTimestampId(tab1);
        assertUpdate("INSERT INTO " + tableName1 + " VALUES (3,'ccc')", 1);
        tab1VersionId3 = getLatestSnapshotId(tab1);
        tab1Timestamp3 = getLatestTimestampId(tab1);

        assertUpdate("CREATE TABLE " + tableName2 + " (id integer, desc varchar) WITH(partitioning = ARRAY['id'])");
        assertUpdate("INSERT INTO " + tableName2 + " VALUES (1,'xxx')", 1);
        tab2VersionId1 = getLatestSnapshotId(tab2);
        tab2Timestamp1 = getLatestTimestampId(tab2);
        assertUpdate("INSERT INTO " + tableName2 + " VALUES (2,'yyy')", 1);
        tab2VersionId2 = getLatestSnapshotId(tab2);
        tab2Timestamp2 = getLatestTimestampId(tab2);
        assertUpdate("INSERT INTO " + tableName2 + " VALUES (3,'zzz')", 1);
        tab2VersionId3 = getLatestSnapshotId(tab2);
        tab2Timestamp3 = getLatestTimestampId(tab2);
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        assertQuerySucceeds("DROP VIEW IF EXISTS " + ICEBERG_CATALOG + "." + viewName1);
        assertQuerySucceeds("DROP VIEW IF EXISTS " + ICEBERG_CATALOG + "." + viewName2);
        assertQuerySucceeds("DROP TABLE IF EXISTS " + ICEBERG_CATALOG + "." + tableName1);
        assertQuerySucceeds("DROP TABLE IF EXISTS " + ICEBERG_CATALOG + "." + tableName2);
        assertQuerySucceeds("DROP TABLE IF EXISTS " + ICEBERG_CATALOG + "." + tableName3);
        assertQuerySucceeds("DROP TABLE IF EXISTS " + ICEBERG_CATALOG + "." + tableName4);
        assertQuerySucceeds("DROP SCHEMA IF EXISTS " + ICEBERG_CATALOG + "." + schemaName);
    }

    private long getLatestSnapshotId(String tableName)
    {
        return (long) computeActual("SELECT snapshot_id FROM " + schemaName + "." + "\"" + tableName + "$snapshots\" ORDER BY committed_at DESC LIMIT 1")
                .getOnlyValue();
    }
    private String getLatestTimestampId(String tableName)
    {
        return (String) computeActual("SELECT cast(made_current_at as varchar) FROM " + schemaName + "." + "\"" + tableName + "$history\" ORDER BY made_current_at DESC LIMIT 1")
                .getOnlyValue();
    }

    @Test
    public void testTableVersionBasic()
    {
        assertQuery("SELECT desc FROM " + tableName1 + " FOR VERSION AS OF " + tab1VersionId1 + " ORDER BY 1", "VALUES 'aaa'");
        assertQuery("SELECT desc FROM " + tableName1 + " FOR VERSION AS OF " + tab1VersionId2 + " ORDER BY 1", "VALUES ('aaa'),('bbb')");
        assertQuery("SELECT desc FROM " + tableName1 + " FOR VERSION AS OF " + tab1VersionId3 + " ORDER BY 1", "VALUES ('aaa'), ('bbb'), ('ccc')");
        assertQuery("SELECT desc FROM " + tableName1 + " FOR TIMESTAMP AS OF TIMESTAMP " + "'" + tab1Timestamp1 + "'" + " ORDER BY 1", "VALUES 'aaa'");
        assertQuery("SELECT desc FROM " + tableName1 + " FOR TIMESTAMP AS OF TIMESTAMP " + "'" + tab1Timestamp2 + "'" + " ORDER BY 1", "VALUES ('aaa'),('bbb')");
        assertQuery("SELECT desc FROM " + tableName1 + " FOR TIMESTAMP AS OF TIMESTAMP " + "'" + tab1Timestamp3 + "'" + " ORDER BY 1", "VALUES ('aaa'), ('bbb'), ('ccc')");

        assertQuery("SELECT desc FROM " + tableName1 + " FOR TIMESTAMP AS OF TIMESTAMP " + "'" + tab1Timestamp2 + "'" + " ORDER BY 1", "VALUES ('aaa'),('bbb')");
        assertQuery("SELECT desc FROM " + tableName1 + " FOR TIMESTAMP AS OF CURRENT_TIMESTAMP ORDER BY 1", "VALUES ('aaa'), ('bbb'), ('ccc')");
        assertQuery("SELECT desc FROM " + tableName1 + " FOR TIMESTAMP AS OF NOW() ORDER BY 1", "VALUES ('aaa'), ('bbb'), ('ccc')");
        assertQuery("SELECT desc FROM " + tableName1 + " FOR TIMESTAMP AS OF CAST ('" + tab1Timestamp3 + "' AS TIMESTAMP WITH TIME ZONE) ORDER BY 1", "VALUES ('aaa'), ('bbb'), ('ccc')");

        assertQuery("SELECT desc FROM " + tableName1 + " FOR VERSION AS OF " + tab1VersionId2 + " WHERE id = 2 ORDER BY 1", "VALUES 'bbb'");
        assertQuery("SELECT desc FROM " + tableName1 + " FOR TIMESTAMP AS OF TIMESTAMP " + "'" + tab1Timestamp2 + "'" + " WHERE id = 2 ORDER BY 1", "VALUES 'bbb'");
        assertQuery("SELECT desc FROM " + tableName1 + " FOR VERSION AS OF " + tab1VersionId2 + " AS tab1_v2 WHERE id = 2", "VALUES 'bbb'");
        assertQuery("SELECT desc FROM " + tableName1 + " FOR TIMESTAMP AS OF TIMESTAMP " + "'" + tab1Timestamp2 + "'" + " AS tab1_v2 WHERE id = 2", "VALUES 'bbb'");

        assertQuery("SELECT SUM(id) FROM " + tableName1 + " FOR VERSION AS OF " + tab1VersionId2, "VALUES 3");
        assertQuery("SELECT desc, SUM(id) FROM " + tableName1 + " FOR VERSION AS OF " + tab1VersionId2 + " GROUP BY desc ORDER BY 2", "VALUES ('aaa', 1), ('bbb', 2)");
        assertQuery("SELECT MAX(id) FROM " + tableName1 + " FOR TIMESTAMP AS OF TIMESTAMP " + "'" + tab1Timestamp2 + "'", "VALUES 2");
        assertQuery("SELECT desc, MAX(id) FROM " + tableName1 + " FOR TIMESTAMP AS OF TIMESTAMP " + "'" + tab1Timestamp2 + "' GROUP BY desc ORDER BY 2", "VALUES ('aaa', 1), ('bbb', 2)");
    }

    @Test
    public void testTableVersionMisc()
    {
        // Alias cases - SYSTEM_TIME and SYSTEM_VERSION
        assertQuery("SELECT desc FROM " + tableName1 + " FOR SYSTEM_VERSION AS OF " + tab1VersionId1 + " ORDER BY 1", "VALUES 'aaa'");
        assertQuery("SELECT desc FROM " + tableName1 + " FOR SYSTEM_TIME AS OF TIMESTAMP " + "'" + tab1Timestamp1 + "'" + " ORDER BY 1", "VALUES 'aaa'");
        assertQuery("SELECT desc FROM " + tableName1 + " FOR SYSTEM_TIME AS OF CURRENT_TIMESTAMP ORDER BY 1", "VALUES ('aaa'), ('bbb'), ('ccc')");
        assertQuery("SELECT SUM(id) FROM " + tableName1 + " FOR SYSTEM_VERSION AS OF " + tab1VersionId2, "VALUES 3");
        assertQuery("SELECT desc, MAX(id) FROM " + tableName1 + " FOR SYSTEM_TIME AS OF TIMESTAMP " + "'" + tab1Timestamp2 + "' GROUP BY desc ORDER BY 2", "VALUES ('aaa', 1), ('bbb', 2)");
        assertQuery("SELECT count(*) FROM " + tableName1 + " FOR SYSTEM_VERSION AS OF " + tab1VersionId2 + " , " +
                tableName2 + " FOR SYSTEM_VERSION AS OF " + tab2VersionId2, "VALUES 4");
        assertQuery("SELECT count(*) FROM " + tableName1 + " FOR SYSTEM_TIME AS OF TIMESTAMP " + "'" + tab1Timestamp2 + "' , " +
                tableName2 + " FOR SYSTEM_TIME AS OF TIMESTAMP " + "'" + tab2Timestamp3 + "'" + " WHERE " + tableName1 + ".id = " + tableName2 + ".id", "VALUES 2");

        // Joins, CTE, create table as, union/intersect/except, subquery, view
        assertQuery("SELECT count(*) FROM " + tableName1 + " FOR VERSION AS OF " + tab1VersionId2 + " , " +
                tableName2 + " FOR VERSION AS OF " + tab2VersionId2, "VALUES 4");
        assertQuery("SELECT count(*) FROM " + tableName1 + " FOR VERSION AS OF " + tab1VersionId2 + " AS tab1_v2 INNER JOIN " + tableName2 +
                " FOR VERSION AS OF " + tab2VersionId2 + " AS tab2_v2 ON tab1_v2.id = tab2_v2.id", "VALUES 2");
        assertQuery("SELECT count(*) FROM " + tableName1 + " FOR TIMESTAMP AS OF TIMESTAMP " + "'" + tab1Timestamp2 + "' , " +
                tableName2 + " FOR TIMESTAMP AS OF TIMESTAMP " + "'" + tab2Timestamp3 + "'" + " WHERE " + tableName1 + ".id = " + tableName2 + ".id", "VALUES 2");
        assertQuery("SELECT count(*) FROM " + tableName1 + " FOR TIMESTAMP AS OF TIMESTAMP " + "'" + tab1Timestamp2 + "'" + " AS tab1_v2 INNER JOIN " +
                tableName2 + " FOR TIMESTAMP AS OF TIMESTAMP " + "'" + tab2Timestamp2 + "'" + " AS tab2_v2 ON tab1_v2.id = tab2_v2.id", "VALUES 2");

        assertQuery("WITH CTE1 AS (SELECT id, desc FROM " + tableName2 + " FOR VERSION AS OF " + tab2VersionId2 + ") SELECT desc FROM CTE1", "VALUES ('xxx'), ('yyy')");
        assertQuery("WITH CTE2 AS (SELECT id, desc FROM " + tableName2 + " FOR TIMESTAMP AS OF TIMESTAMP " + "'" + tab2Timestamp2 + "' ) SELECT desc FROM CTE2", "VALUES ('xxx'), ('yyy')");

        assertUpdate("CREATE TABLE " + tableName3 + " AS SELECT * FROM " + tableName1 + " FOR VERSION AS OF " + tab1VersionId2, 2);
        assertQuery("SELECT COUNT(*) FROM " + tableName3, "VALUES 2");
        assertUpdate("CREATE TABLE " + tableName4 + " AS SELECT * FROM " + tableName2 + " FOR TIMESTAMP AS OF TIMESTAMP " + "'" + tab2Timestamp2 + "'", 2);
        assertQuery("SELECT COUNT(*) FROM " + tableName4, "VALUES 2");

        assertQuery("SELECT desc FROM " + tableName1 + " FOR VERSION AS OF " + tab1VersionId1 + " UNION " +
                " SELECT desc FROM " + tableName1 + " FOR VERSION AS OF " + tab1VersionId2 + " ORDER BY desc", "VALUES ('aaa'), ('bbb')");
        assertQuery("SELECT desc FROM " + tableName1 + " FOR TIMESTAMP AS OF TIMESTAMP " + "'" + tab1Timestamp1 + "'" + " UNION ALL " +
                " SELECT desc FROM " + tableName1 + " FOR TIMESTAMP AS OF TIMESTAMP " + "'" + tab1Timestamp2 + "'" + " ORDER BY desc", "VALUES ('aaa'), ('aaa'), ('bbb')");
        assertQuery("SELECT desc FROM " + tableName1 + " FOR VERSION AS OF " + tab1VersionId1 + " INTERSECT " +
                " SELECT desc FROM " + tableName1 + " FOR VERSION AS OF " + tab1VersionId2 + " ORDER BY desc", "VALUES ('aaa')");
        assertQuery("SELECT desc FROM " + tableName1 + " FOR TIMESTAMP AS OF TIMESTAMP " + "'" + tab1Timestamp1 + "'" + " INTERSECT " +
                " SELECT desc FROM " + tableName1 + " FOR TIMESTAMP AS OF TIMESTAMP " + "'" + tab1Timestamp2 + "'" + " ORDER BY desc", "VALUES ('aaa')");
        assertQuery("SELECT desc FROM " + tableName1 + " FOR VERSION AS OF " + tab1VersionId2 + " EXCEPT " +
                " SELECT desc FROM " + tableName1 + " FOR VERSION AS OF " + tab1VersionId1 + " ORDER BY desc", "VALUES ('bbb')");
        assertQuery("SELECT desc FROM " + tableName1 + " FOR TIMESTAMP AS OF TIMESTAMP " + "'" + tab1Timestamp2 + "'" + " EXCEPT " +
                " SELECT desc FROM " + tableName1 + " FOR TIMESTAMP AS OF TIMESTAMP " + "'" + tab1Timestamp1 + "'" + " ORDER BY desc", "VALUES ('bbb')");

        assertQuery("SELECT desc FROM " + tableName1 + " FOR VERSION AS OF " + tab1VersionId2 + " WHERE id IN " +
                "(SELECT id FROM " + tableName2 + " FOR VERSION AS OF " + tab2VersionId2 + ") ORDER BY 1", "VALUES ('aaa'), ('bbb')");
        assertQuery("SELECT desc FROM " + tableName1 + " FOR TIMESTAMP AS OF TIMESTAMP " + "'" + tab1Timestamp2 + "' WHERE id IN " +
                "(SELECT id FROM " + tableName2 + " FOR TIMESTAMP AS OF TIMESTAMP " + "'" + tab2Timestamp2 + "' ) ORDER BY 1", "VALUES ('aaa'), ('bbb')");
        assertQuery("SELECT desc FROM " + tableName1 + " FOR VERSION AS OF " + tab1VersionId2 + " WHERE id IN " +
                "(SELECT id FROM " + tableName2 + " FOR TIMESTAMP AS OF TIMESTAMP " + "'" + tab2Timestamp2 + "' ) ORDER BY 1", "VALUES ('aaa'), ('bbb')");

        assertUpdate("CREATE VIEW " + viewName1 + " AS SELECT id, desc FROM " + tableName1 + " FOR VERSION AS OF " + tab1VersionId2);
        assertQuery("SELECT desc FROM " + viewName1 + " ORDER BY 1", "VALUES ('aaa'),('bbb')");
        assertUpdate("CREATE VIEW " + viewName2 + " AS SELECT id, desc FROM " + tableName1 + " FOR TIMESTAMP AS OF TIMESTAMP " + "'" + tab1Timestamp2 + "'");
        assertQuery("SELECT desc FROM " + viewName2 + " ORDER BY 1", "VALUES ('aaa'),('bbb')");
        assertQuery("SELECT count(*) FROM " + viewName1 + " INNER JOIN " + viewName2 + " ON " + viewName1 + ".id = " + viewName2 + ".id", "VALUES 2");
    }

    @Test
    public void testTableVersionErrors()
    {
        assertQueryFails("SELECT desc FROM " + tableName2 + " FOR VERSION AS OF 100", ".* Type integer is invalid. Supported table version AS OF expression type is BIGINT");
        assertQueryFails("SELECT desc FROM " + tableName2 + " FOR VERSION AS OF 'bad'", ".* Type varchar\\(3\\) is invalid. Supported table version AS OF expression type is BIGINT");
        assertQueryFails("SELECT desc FROM " + tableName2 + " FOR VERSION AS OF CURRENT_DATE", ".* Type date is invalid. Supported table version AS OF expression type is BIGINT");
        assertQueryFails("SELECT desc FROM " + tableName2 + " FOR VERSION AS OF CURRENT_TIMESTAMP", ".* Type timestamp with time zone is invalid. Supported table version AS OF expression type is BIGINT");
        assertQueryFails("SELECT desc FROM " + tableName2 + " FOR VERSION AS OF id", ".* cannot be resolved");
        assertQueryFails("SELECT desc FROM " + tableName2 + " FOR VERSION AS OF (SELECT 10000000)", ".* Constant expression cannot contain a subquery");
        assertQueryFails("SELECT desc FROM " + tableName2 + " FOR VERSION AS OF NULL", "Table version AS OF expression cannot be NULL for .*");
        assertQueryFails("SELECT desc FROM " + tableName2 + " FOR VERSION AS OF " + tab2VersionId1 + " - " + tab2VersionId1, "Iceberg snapshot ID does not exists: 0");
        assertQueryFails("SELECT desc FROM " + tableName2 + " FOR VERSION AS OF CAST (100 AS BIGINT)", "Iceberg snapshot ID does not exists: 100");

        assertQueryFails("SELECT desc FROM " + tableName2 + " FOR TIMESTAMP AS OF 100", ".* Type integer is invalid. Supported table version AS OF expression type is Timestamp with Time Zone.");
        assertQueryFails("SELECT desc FROM " + tableName2 + " FOR TIMESTAMP AS OF 'bad'", ".* Type varchar\\(3\\) is invalid. Supported table version AS OF expression type is Timestamp with Time Zone.");
        assertQueryFails("SELECT desc FROM " + tableName2 + " FOR TIMESTAMP AS OF id", ".* cannot be resolved");
        assertQueryFails("SELECT desc FROM " + tableName2 + " FOR TIMESTAMP AS OF (SELECT CURRENT_TIMESTAMP)", ".* Constant expression cannot contain a subquery");
        assertQueryFails("SELECT desc FROM " + tableName2 + " FOR TIMESTAMP AS OF NULL", "Table version AS OF expression cannot be NULL for .*");
        assertQueryFails("SELECT desc FROM " + tableName2 + " FOR TIMESTAMP AS OF TIMESTAMP " + "'" + tab2Timestamp1 + "' - INTERVAL '1' MONTH", "No history found based on timestamp for table \"test_tt_schema\".\"test_table_version_tab2\"");
        assertQueryFails("SELECT desc FROM " + tableName2 + " FOR TIMESTAMP AS OF CAST ('2023-01-01' AS TIMESTAMP WITH TIME ZONE)", "No history found based on timestamp for table \"test_tt_schema\".\"test_table_version_tab2\"");
        assertQueryFails("SELECT desc FROM " + tableName2 + " FOR TIMESTAMP AS OF CAST ('2023-01-01' AS TIMESTAMP)", ".* Type timestamp is invalid. Supported table version AS OF expression type is Timestamp with Time Zone.");
        assertQueryFails("SELECT desc FROM " + tableName2 + " FOR TIMESTAMP AS OF CAST ('2023-01-01' AS DATE)", ".* Type date is invalid. Supported table version AS OF expression type is Timestamp with Time Zone.");
        assertQueryFails("SELECT desc FROM " + tableName2 + " FOR TIMESTAMP AS OF CURRENT_DATE", ".* Type date is invalid. Supported table version AS OF expression type is Timestamp with Time Zone.");
        assertQueryFails("SELECT desc FROM " + tableName2 + " FOR TIMESTAMP AS OF TIMESTAMP '2023-01-01 00:00:00.000'", ".* Type timestamp is invalid. Supported table version AS OF expression type is Timestamp with Time Zone.");
    }
}
