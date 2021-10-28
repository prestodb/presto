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
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestDistributedQueries;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.ENABLE_STATS_COLLECTION_FOR_TEMPORARY_TABLE;
import static com.facebook.presto.hive.BucketFunctionType.HIVE_COMPATIBLE;
import static com.facebook.presto.hive.BucketFunctionType.PRESTO_NATIVE;
import static com.facebook.presto.hive.HiveQueryRunner.createMaterializingQueryRunner;
import static com.facebook.presto.hive.HiveStorageFormat.ORC;
import static com.facebook.presto.hive.HiveStorageFormat.PAGEFILE;
import static com.facebook.presto.hive.TestHiveIntegrationSmokeTest.assertRemoteMaterializedExchangesCount;
import static com.facebook.presto.sql.tree.ExplainType.Type.LOGICAL;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.tpch.TpchTable.getTables;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

public class TestHiveDistributedQueriesWithExchangeMaterialization
        extends AbstractTestDistributedQueries
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createMaterializingQueryRunner(getTables());
    }

    @Test
    public void testMaterializedExchangesEnabled()
    {
        assertQuery(getSession(), "SELECT orderkey, COUNT(*) lines FROM lineitem GROUP BY orderkey", assertRemoteMaterializedExchangesCount(1));
    }

    @Test
    public void testMaterializeHiveUnsupportedTypeForTemporaryTable()
    {
        testMaterializeHiveUnsupportedTypeForTemporaryTable(ORC, true);
        testMaterializeHiveUnsupportedTypeForTemporaryTable(PAGEFILE, false);
        assertThrows(RuntimeException.class, () -> testMaterializeHiveUnsupportedTypeForTemporaryTable(ORC, false));
    }

    private void testMaterializeHiveUnsupportedTypeForTemporaryTable(
            HiveStorageFormat storageFormat,
            boolean usePageFileForHiveUnsupportedType)
    {
        Session session = Session.builder(getSession())
                .setCatalogSessionProperty("hive", "temporary_table_storage_format", storageFormat.name())
                .setCatalogSessionProperty("hive", "use_pagefile_for_hive_unsupported_type", String.valueOf(usePageFileForHiveUnsupportedType))
                .setSystemProperty(ENABLE_STATS_COLLECTION_FOR_TEMPORARY_TABLE, "true")
                .build();

        assertUpdate(session, "CREATE TABLE test_materialize_non_hive_types AS\n" +
                        "WITH t1 AS (\n" +
                        "    SELECT\n" +
                        "        CAST('192.168.0.0' AS IPADDRESS) address,\n" +
                        "        nationkey\n" +
                        "    FROM nation\n" +
                        "),\n" +
                        "t2 AS (\n" +
                        "    SELECT\n" +
                        "        FROM_ISO8601_TIMESTAMP('2020-02-25') time,\n" +
                        "        nationkey\n" +
                        "    FROM nation\n" +
                        ")\n" +
                        "SELECT\n" +
                        "    t1.nationkey,\n" +
                        "    CAST(t1.address AS VARCHAR) address,\n" +
                        "    CAST(t2.time AS VARCHAR) time\n" +
                        "FROM t1\n" +
                        "JOIN t2\n" +
                        "    ON t1.nationkey = t2.nationkey",
                25,
                assertRemoteMaterializedExchangesCount(2));

        assertUpdate("DROP TABLE IF EXISTS test_materialize_non_hive_types");

        assertUpdate(session, "CREATE TABLE test_materialize_non_hive_types AS\n" +
                        "WITH t1 AS (\n" +
                        "    SELECT\n" +
                        "        CAST('2000-01-01' AS DATE) date,\n" +
                        "        nationkey\n" +
                        "    FROM nation\n" +
                        "),\n" +
                        "t2 AS (\n" +
                        "    SELECT\n" +
                        "        FROM_ISO8601_TIMESTAMP('2020-02-25') time,\n" +
                        "        nationkey\n" +
                        "    FROM nation\n" +
                        ")\n" +
                        "SELECT\n" +
                        "    t1.nationkey,\n" +
                        "    t1.date,\n" +
                        "    CAST(t2.time AS VARCHAR) time\n" +
                        "FROM t1\n" +
                        "JOIN t2\n" +
                        "    ON t1.nationkey = t2.nationkey",
                25);

        assertUpdate("DROP TABLE IF EXISTS test_materialize_non_hive_types");
    }

    @Test
    public void testBucketedByHiveUnsupportedTypeForTemporaryTable()
    {
        testBucketedByHiveUnsupportedTypeForTemporaryTable(ORC, HIVE_COMPATIBLE, true);
        testBucketedByHiveUnsupportedTypeForTemporaryTable(ORC, PRESTO_NATIVE, true);
        testBucketedByHiveUnsupportedTypeForTemporaryTable(PAGEFILE, HIVE_COMPATIBLE, true);
        testBucketedByHiveUnsupportedTypeForTemporaryTable(PAGEFILE, PRESTO_NATIVE, false);
        assertThrows(RuntimeException.class, () -> testBucketedByHiveUnsupportedTypeForTemporaryTable(ORC, HIVE_COMPATIBLE, false));
        assertThrows(RuntimeException.class, () -> testBucketedByHiveUnsupportedTypeForTemporaryTable(ORC, PRESTO_NATIVE, false));
        assertThrows(RuntimeException.class, () -> testBucketedByHiveUnsupportedTypeForTemporaryTable(PAGEFILE, HIVE_COMPATIBLE, false));
    }

    private void testBucketedByHiveUnsupportedTypeForTemporaryTable(
            HiveStorageFormat storageFormat,
            BucketFunctionType bucketFunctionType,
            boolean usePageFileForHiveUnsupportedType)
    {
        Session session = Session.builder(getSession())
                .setCatalogSessionProperty("hive", "temporary_table_storage_format", storageFormat.name())
                .setCatalogSessionProperty("hive", "bucket_function_type_for_exchange", bucketFunctionType.name())
                .setCatalogSessionProperty("hive", "use_pagefile_for_hive_unsupported_type", String.valueOf(usePageFileForHiveUnsupportedType))
                .build();

        assertUpdate(session, "CREATE TABLE test_materialize_bucket_by_non_hive_types AS\n" +
                        "WITH t1 AS (\n" +
                        "    SELECT\n" +
                        "        CAST('192.168.0.0' AS IPADDRESS) address,\n" +
                        "        nationkey\n" +
                        "    FROM nation\n" +
                        "    GROUP BY\n" +
                        "        nationkey,\n" +
                        "        CAST('192.168.0.0' AS IPADDRESS)\n" +
                        "),\n" +
                        "t2 AS (\n" +
                        "    SELECT\n" +
                        "        FROM_ISO8601_TIMESTAMP('2020-02-25') time,\n" +
                        "        nationkey\n" +
                        "    FROM nation\n" +
                        "    GROUP BY\n" +
                        "        nationkey,\n" +
                        "        FROM_ISO8601_TIMESTAMP('2020-02-25')\n" +
                        ")\n" +
                        "SELECT\n" +
                        "    t1.nationkey,\n" +
                        "    CAST(t1.address AS VARCHAR) address,\n" +
                        "    CAST(t2.time AS VARCHAR) time\n" +
                        "FROM t1\n" +
                        "JOIN t2\n" +
                        "    ON t1.nationkey = t2.nationkey",
                25,
                assertRemoteMaterializedExchangesCount(3));

        assertUpdate("DROP TABLE IF EXISTS test_materialize_bucket_by_non_hive_types");
    }

    @Override
    public void testDelete()
    {
        // Hive connector currently does not support row-by-row delete
    }

    @Override
    public void testExcept()
    {
        // decimal type is not supported by the Hive hash code function
    }

    @Override
    public void testIntersect()
    {
        // decimal type is not supported by the Hive hash code function
    }

    @Override
    public void testQuantifiedComparison()
    {
        // decimal type is not supported by the Hive hash code function
    }

    public void testSemiJoin()
    {
        // decimal type is not supported by the Hive hash code function
    }

    @Override
    public void testUnionRequiringCoercion()
    {
        // decimal type is not supported by the Hive hash code function
    }

    @Override
    public void testValues()
    {
        // decimal type is not supported by the Hive hash code function
    }

    @Test
    public void testExchangeMaterializationWithConstantFolding()
    {
        try {
            assertUpdate(
                    // bucket count has to be different from materialized bucket number
                    "CREATE TABLE test_constant_folding_lineitem_bucketed\n" +
                            "WITH (bucket_count = 17, bucketed_by = ARRAY['partkey_mod_9', 'partkey', 'suppkey', 'suppkey_varchar']) AS\n" +
                            "SELECT partkey % 9 partkey_mod_9, partkey, suppkey, CAST(suppkey AS VARCHAR) suppkey_varchar, comment FROM lineitem",
                    "SELECT count(*) from lineitem");
            assertUpdate(
                    "CREATE TABLE test_constant_folding_partsupp_unbucketed AS\n" +
                            "SELECT partkey % 9 partkey_mod_9, partkey, suppkey, CAST(suppkey AS VARCHAR) suppkey_varchar, comment FROM partsupp",
                    "SELECT count(*) from partsupp");

            // one constant, third position (suppkey BIGINT)
            assertQuery(
                    getSession(),
                    "SELECT lineitem.partkey, lineitem.suppkey, lineitem.comment lineitem_comment, partsupp.comment partsupp_comment\n" +
                            "FROM test_constant_folding_lineitem_bucketed lineitem JOIN test_constant_folding_partsupp_unbucketed partsupp\n" +
                            "ON\n" +
                            "  lineitem.partkey = partsupp.partkey AND\n" +
                            "  lineitem.partkey_mod_9 = partsupp.partkey_mod_9 AND\n" +
                            "  lineitem.suppkey = partsupp.suppkey AND\n" +
                            "  lineitem.suppkey_varchar = partsupp.suppkey_varchar\n" +
                            "WHERE lineitem.suppkey = 42",
                    "SELECT lineitem.partkey, lineitem.suppkey, lineitem.comment lineitem_comment, partsupp.comment partsupp_comment\n" +
                            "FROM lineitem JOIN partsupp\n" +
                            "ON lineitem.partkey = partsupp.partkey AND\n" +
                            "lineitem.suppkey = partsupp.suppkey\n" +
                            "WHERE lineitem.suppkey = 42",
                    assertRemoteMaterializedExchangesCount(1));

            // one constant, fourth position (suppkey_varchar VARCHAR)
            assertQuery(
                    getSession(),
                    "SELECT lineitem.partkey, lineitem.suppkey, lineitem.comment lineitem_comment, partsupp.comment partsupp_comment\n" +
                            "FROM test_constant_folding_lineitem_bucketed lineitem JOIN test_constant_folding_partsupp_unbucketed partsupp\n" +
                            "ON\n" +
                            "  lineitem.partkey = partsupp.partkey AND\n" +
                            "  lineitem.partkey_mod_9 = partsupp.partkey_mod_9 AND\n" +
                            "  lineitem.suppkey = partsupp.suppkey AND\n" +
                            "  lineitem.suppkey_varchar = partsupp.suppkey_varchar\n" +
                            "WHERE lineitem.suppkey_varchar = '42'",
                    "SELECT lineitem.partkey, lineitem.suppkey, lineitem.comment lineitem_comment, partsupp.comment partsupp_comment\n" +
                            "FROM lineitem JOIN partsupp\n" +
                            "ON lineitem.partkey = partsupp.partkey AND\n" +
                            "lineitem.suppkey = partsupp.suppkey\n" +
                            "WHERE lineitem.suppkey = 42",
                    assertRemoteMaterializedExchangesCount(1));

            // two constants, first and third position (partkey_mod_9 BIGINT, suppkey BIGINT)
            assertQuery(
                    getSession(),
                    "SELECT lineitem.partkey, lineitem.suppkey, lineitem.comment lineitem_comment, partsupp.comment partsupp_comment\n" +
                            "FROM test_constant_folding_lineitem_bucketed lineitem JOIN test_constant_folding_partsupp_unbucketed partsupp\n" +
                            "ON\n" +
                            "  lineitem.partkey = partsupp.partkey AND\n" +
                            "  lineitem.partkey_mod_9 = partsupp.partkey_mod_9 AND\n" +
                            "  lineitem.suppkey = partsupp.suppkey AND\n" +
                            "  lineitem.suppkey_varchar = partsupp.suppkey_varchar\n" +
                            "WHERE lineitem.partkey_mod_9 = 7 AND lineitem.suppkey = 42",
                    "SELECT lineitem.partkey, lineitem.suppkey, lineitem.comment lineitem_comment, partsupp.comment partsupp_comment\n" +
                            "FROM lineitem JOIN partsupp\n" +
                            "ON lineitem.partkey = partsupp.partkey AND\n" +
                            "lineitem.suppkey = partsupp.suppkey\n" +
                            "WHERE lineitem.partkey % 9 = 7 AND lineitem.suppkey = 42",
                    assertRemoteMaterializedExchangesCount(1));

            // two constants, first and forth position (partkey_mod_9 BIGINT, suppkey_varchar VARCHAR)
            assertQuery(
                    getSession(),
                    "SELECT lineitem.partkey, lineitem.suppkey, lineitem.comment lineitem_comment, partsupp.comment partsupp_comment\n" +
                            "FROM test_constant_folding_lineitem_bucketed lineitem JOIN test_constant_folding_partsupp_unbucketed partsupp\n" +
                            "ON\n" +
                            "  lineitem.partkey = partsupp.partkey AND\n" +
                            "  lineitem.partkey_mod_9 = partsupp.partkey_mod_9 AND\n" +
                            "  lineitem.suppkey = partsupp.suppkey AND\n" +
                            "  lineitem.suppkey_varchar = partsupp.suppkey_varchar\n" +
                            "WHERE lineitem.partkey_mod_9 = 7 AND lineitem.suppkey_varchar = '42'",
                    "SELECT lineitem.partkey, lineitem.suppkey, lineitem.comment lineitem_comment, partsupp.comment partsupp_comment\n" +
                            "FROM lineitem JOIN partsupp\n" +
                            "ON lineitem.partkey = partsupp.partkey AND\n" +
                            "lineitem.suppkey = partsupp.suppkey\n" +
                            "WHERE lineitem.partkey % 9 = 7 AND lineitem.suppkey = 42",
                    assertRemoteMaterializedExchangesCount(1));
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS test_constant_folding_lineitem_bucketed");
            assertUpdate("DROP TABLE IF EXISTS test_constant_folding_partsupp_unbucketed");
        }
    }

    @Test
    public void testIgnoreTableBucketingWhenTableBucketCountIsSmall()
    {
        try {
            assertUpdate(
                    "CREATE TABLE partitioned_nation\n" +
                            "WITH (\n" +
                            "    bucket_count = 17,\n" +
                            "    bucketed_by = ARRAY['nationkey']\n" +
                            ") AS\n" +
                            "SELECT\n" +
                            "    *\n" +
                            "FROM nation",
                    25);

            // test default : not ignore table bucketing
            assertQuery(
                    getSession(),
                    "SELECT\n" +
                            "    *\n" +
                            "FROM partitioned_nation t1\n" +
                            "JOIN nation t2\n" +
                            "    ON t1.nationkey = t2.nationkey",
                    "SELECT\n" +
                            "    *\n" +
                            "FROM nation t1\n" +
                            "JOIN nation t2\n" +
                            "    ON t1.nationkey = t2.nationkey",
                    assertRemoteMaterializedExchangesCount(1));

            // test ignore table bucketing
            Session testSession = Session.builder(getSession())
                    .setCatalogSessionProperty("hive", "min_bucket_count_to_not_ignore_table_bucketing", "20")
                    .build();

            assertQuery(
                    testSession,
                    "SELECT\n" +
                            "    *\n" +
                            "FROM partitioned_nation t1\n" +
                            "JOIN nation t2\n" +
                            "    ON t1.nationkey = t2.nationkey",
                    "SELECT\n" +
                            "    *\n" +
                            "FROM nation t1\n" +
                            "JOIN nation t2\n" +
                            "    ON t1.nationkey = t2.nationkey",
                    assertRemoteMaterializedExchangesCount(2));

            // do not ignore table bucketing if $bucket column is referenced
            assertQuery(
                    testSession,
                    "SELECT\n" +
                            "    *\n" +
                            "FROM partitioned_nation t1\n" +
                            "JOIN nation t2\n" +
                            "    ON t1.nationkey = t2.nationkey\n" +
                            "WHERE\n" +
                            "    \"$bucket\" < 20",
                    "SELECT\n" +
                            "    *\n" +
                            "FROM nation t1\n" +
                            "JOIN nation t2\n" +
                            "    ON t1.nationkey = t2.nationkey",
                    assertRemoteMaterializedExchangesCount(1));
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS partitioned_nation");
        }
    }

    @Test
    public void testExplainOfCreateTableAs()
    {
        String query = "CREATE TABLE copy_orders AS SELECT * FROM orders";
        MaterializedResult result = computeActual("EXPLAIN " + query);
        assertEquals(getOnlyElement(result.getOnlyColumnAsSet()), getExplainPlan(query, LOGICAL));
    }

    @Override
    protected boolean supportsNotNullColumns()
    {
        return false;
    }

    @Test
    public void testEmptyBucketedTemporaryTable()
    {
        assertQuery("SELECT COUNT(DISTINCT linenumber), COUNT(*) from lineitem where linenumber < 0");
    }

    @Test
    public void testBucketedTemporaryTableWithMissingFiles()
    {
        testBucketedTemporaryTableWithMissingFiles(true);
        testBucketedTemporaryTableWithMissingFiles(false);
    }

    private void testBucketedTemporaryTableWithMissingFiles(boolean isFileRenameEnabled)
    {
        Session session = Session.builder(getSession())
                .setCatalogSessionProperty("hive", "file_renaming_enabled", String.valueOf(isFileRenameEnabled))
                .build();
        assertQuery(session, "SELECT COUNT(DISTINCT linenumber), COUNT(*) from (SELECT * from lineitem LIMIT 1)");
    }
    // Hive specific tests should normally go in TestHiveIntegrationSmokeTest
}
