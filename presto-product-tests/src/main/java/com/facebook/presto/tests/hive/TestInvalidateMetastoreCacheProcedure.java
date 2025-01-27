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
package com.facebook.presto.tests.hive;

import io.prestodb.tempto.AfterTestWithContext;
import io.prestodb.tempto.BeforeTestWithContext;
import io.prestodb.tempto.ProductTest;
import org.testng.annotations.Test;

import static com.facebook.presto.tests.TestGroups.INVALIDATE_METASTORE_CACHE;
import static com.facebook.presto.tests.utils.QueryExecutors.onHive;
import static com.facebook.presto.tests.utils.QueryExecutors.onPresto;
import static io.prestodb.tempto.assertions.QueryAssert.Row.row;
import static io.prestodb.tempto.assertions.QueryAssert.assertThat;

public class TestInvalidateMetastoreCacheProcedure
        extends ProductTest
{
    @BeforeTestWithContext
    public void setUp()
    {
        // CREATE SCHEMAS
        onPresto().executeQuery("CREATE SCHEMA IF NOT EXISTS hive_with_metastore_cache.test_invalidate_metastore_cache_schema1");
        onPresto().executeQuery("CREATE SCHEMA IF NOT EXISTS hive_with_metastore_cache.test_invalidate_metastore_cache_schema2");

        // CREATE TABLES IN SCHEMA 1
        onPresto().executeQuery("CREATE TABLE IF NOT EXISTS hive_with_metastore_cache.test_invalidate_metastore_cache_schema1.nation AS SELECT * FROM tpch.tiny.nation");
        onPresto().executeQuery("CREATE TABLE IF NOT EXISTS hive_with_metastore_cache.test_invalidate_metastore_cache_schema1.region AS SELECT * FROM tpch.tiny.region");

        // CREATE TABLES IN SCHEMA 2
        onPresto().executeQuery("CREATE TABLE IF NOT EXISTS hive_with_metastore_cache.test_invalidate_metastore_cache_schema2.nation AS SELECT * FROM tpch.tiny.nation");
        onPresto().executeQuery("CREATE TABLE IF NOT EXISTS hive_with_metastore_cache.test_invalidate_metastore_cache_schema2.region AS SELECT * FROM tpch.tiny.region");

        // CREATE A PARTITIONED TABLE
        onPresto().executeQuery(
                "CREATE TABLE IF NOT EXISTS hive_with_metastore_cache.test_invalidate_metastore_cache_schema1.nation_partitioned(" +
                        "nationkey bigint," +
                        " name varchar(25)," +
                        " regionkey bigint)" +
                        " WITH (partitioned_by = ARRAY['regionkey'])");

        onPresto().executeQuery(
                "INSERT INTO hive_with_metastore_cache.test_invalidate_metastore_cache_schema1.nation_partitioned" +
                        " SELECT nationkey, name, regionkey FROM tpch.tiny.nation");

        onPresto().executeQuery("CALL hive_with_metastore_cache.system.invalidate_metastore_cache()");
    }

    @Test(groups = {INVALIDATE_METASTORE_CACHE})
    public void testInvalidateFullMetastoreCache()
    {
        assertThat(onPresto().executeQuery("SELECT nationkey FROM hive_with_metastore_cache.test_invalidate_metastore_cache_schema1.nation"))
                .hasRowsCount(25);
        assertThat(onPresto().executeQuery("select tablecachesize from jmx.current.\"com.facebook.presto.hive.metastore:name=hive_with_metastore_cache,type=metastorecachestats\""))
                .contains(row(0), row(1));

        // Rename Column outside Presto
        onHive().executeQuery("ALTER TABLE test_invalidate_metastore_cache_schema1.nation CHANGE nationkey key bigint");

        // This should fail as Presto has old metadata cached
        assertThat(() -> onPresto().executeQuery("SELECT key FROM hive_with_metastore_cache.test_invalidate_metastore_cache_schema1.nation"))
                .failsWithMessageMatching(".*Column 'key' cannot be resolved");

        // This should pass after invalidating Metastore Cache
        onPresto().executeQuery("CALL hive_with_metastore_cache.system.invalidate_metastore_cache()");
        assertThat(onPresto().executeQuery("SELECT key FROM hive_with_metastore_cache.test_invalidate_metastore_cache_schema1.nation"))
                .hasRowsCount(25);
    }

    @Test(groups = {INVALIDATE_METASTORE_CACHE}, dependsOnMethods = "testInvalidateFullMetastoreCache")
    public void testInvalidateMetastoreCacheBySchema()
    {
        assertThat(onPresto().executeQuery("SELECT nationkey FROM hive_with_metastore_cache.test_invalidate_metastore_cache_schema1.nation"))
                .hasRowsCount(25);
        assertThat(onPresto().executeQuery("SELECT nationkey FROM hive_with_metastore_cache.test_invalidate_metastore_cache_schema2.nation"))
                .hasRowsCount(25);
        assertThat(onPresto().executeQuery("select tablecachesize from jmx.current.\"com.facebook.presto.hive.metastore:name=hive_with_metastore_cache,type=metastorecachestats\""))
                .contains(row(0), row(2));

        // Rename Column outside Presto
        onHive().executeQuery("ALTER TABLE test_invalidate_metastore_cache_schema2.nation CHANGE nationkey key bigint");

        // This should fail as Presto has old metadata cached
        assertThat(() -> onPresto().executeQuery("SELECT key FROM hive_with_metastore_cache.test_invalidate_metastore_cache_schema2.nation"))
                .failsWithMessageMatching(".*Column 'key' cannot be resolved");

        // This should still fail after invalidating Metastore Cache only for test_invalidate_metastore_cache_schema1
        onPresto().executeQuery("CALL hive_with_metastore_cache.system.invalidate_metastore_cache('test_invalidate_metastore_cache_schema1')");
        assertThat(() -> onPresto().executeQuery("SELECT key FROM hive_with_metastore_cache.test_invalidate_metastore_cache_schema2.nation"))
                .failsWithMessageMatching(".*Column 'key' cannot be resolved");

        // This should pass after invalidating Metastore Cache for test_invalidate_metastore_cache_schema2
        onPresto().executeQuery("CALL hive_with_metastore_cache.system.invalidate_metastore_cache('test_invalidate_metastore_cache_schema2')");
        assertThat(onPresto().executeQuery("SELECT key FROM hive_with_metastore_cache.test_invalidate_metastore_cache_schema2.nation"))
                .hasRowsCount(25);
    }

    @Test(groups = {INVALIDATE_METASTORE_CACHE}, dependsOnMethods = "testInvalidateMetastoreCacheBySchema")
    public void testInvalidateMetastoreCacheByTable()
    {
        assertThat(onPresto().executeQuery("SELECT nationkey FROM hive_with_metastore_cache.test_invalidate_metastore_cache_schema1.nation"))
                .hasRowsCount(25);
        assertThat(onPresto().executeQuery("SELECT * FROM hive_with_metastore_cache.test_invalidate_metastore_cache_schema1.region"))
                .hasRowsCount(5);
        assertThat(onPresto().executeQuery("select tablecachesize from jmx.current.\"com.facebook.presto.hive.metastore:name=hive_with_metastore_cache,type=metastorecachestats\""))
                .contains(row(0), row(2));

        // Rename Column outside Presto
        onHive().executeQuery("ALTER TABLE test_invalidate_metastore_cache_schema1.nation CHANGE nationkey key bigint");

        // This should fail as Presto has old metadata cached
        assertThat(() -> onPresto().executeQuery("SELECT key FROM hive_with_metastore_cache.test_invalidate_metastore_cache_schema1.nation"))
                .failsWithMessageMatching(".*Column 'key' cannot be resolved");

        // This should still fail after invalidating Metastore Cache only for test_invalidate_metastore_cache_schema1.region
        onPresto().executeQuery("CALL hive_with_metastore_cache.system.invalidate_metastore_cache('test_invalidate_metastore_cache_schema1', 'region')");
        assertThat(() -> onPresto().executeQuery("SELECT key FROM hive_with_metastore_cache.test_invalidate_metastore_cache_schema1.nation"))
                .failsWithMessageMatching(".*Column 'key' cannot be resolved");

        // This should pass after invalidating Metastore Cache for test_invalidate_metastore_cache_schema2
        onPresto().executeQuery("CALL hive_with_metastore_cache.system.invalidate_metastore_cache('test_invalidate_metastore_cache_schema1', 'nation')");
        assertThat(onPresto().executeQuery("SELECT key FROM hive_with_metastore_cache.test_invalidate_metastore_cache_schema1.nation"))
                .hasRowsCount(25);
    }

    @Test(groups = {INVALIDATE_METASTORE_CACHE}, dependsOnMethods = "testInvalidateMetastoreCacheByTable")
    public void testInvalidateMetastoreCacheByPartition()
    {
        assertThat(onPresto().executeQuery("SELECT * FROM hive_with_metastore_cache.test_invalidate_metastore_cache_schema1.\"nation_partitioned$partitions\""))
                .hasRowsCount(5);
        assertThat(onPresto().executeQuery("SELECT DISTINCT regionkey FROM hive_with_metastore_cache.test_invalidate_metastore_cache_schema1.nation_partitioned"))
                .hasRowsCount(5);
        assertThat(onPresto().executeQuery("select tablecachesize, partitioncachesize, partitionnamescachesize from jmx.current.\"com.facebook.presto.hive.metastore:name=hive_with_metastore_cache,type=metastorecachestats\""))
                .contains(row(1, 5, 1));

        // Add new partition outside Presto
        onHive().executeQuery("INSERT INTO test_invalidate_metastore_cache_schema1.nation_partitioned PARTITION(regionkey=5) VALUES(26, 'new_nation')");

        // This should not be able to show the new partition
        assertThat(onPresto().executeQuery("SELECT * FROM hive_with_metastore_cache.test_invalidate_metastore_cache_schema1.\"nation_partitioned$partitions\""))
                .hasRowsCount(5);
        assertThat(onPresto().executeQuery("SELECT DISTINCT regionkey FROM hive_with_metastore_cache.test_invalidate_metastore_cache_schema1.nation_partitioned"))
                .hasRowsCount(5);
        assertThat(onPresto().executeQuery("select tablecachesize, partitioncachesize, partitionnamescachesize from jmx.current.\"com.facebook.presto.hive.metastore:name=hive_with_metastore_cache,type=metastorecachestats\""))
                .contains(row(1, 5, 1));

        // After invalidating the cache, we should be able to see the new partition once we invalidate any partition in the table
        onPresto().executeQuery("CALL hive_with_metastore_cache.system.invalidate_metastore_cache('test_invalidate_metastore_cache_schema1', 'nation_partitioned', ARRAY['regionkey'], ARRAY['0'])");
        assertThat(onPresto().executeQuery("select tablecachesize, partitioncachesize, partitionnamescachesize from jmx.current.\"com.facebook.presto.hive.metastore:name=hive_with_metastore_cache,type=metastorecachestats\""))
                .contains(row(1, 4, 0));
        assertThat(onPresto().executeQuery("SELECT * FROM hive_with_metastore_cache.test_invalidate_metastore_cache_schema1.\"nation_partitioned$partitions\""))
                .hasRowsCount(6);
        assertThat(onPresto().executeQuery("SELECT DISTINCT regionkey FROM hive_with_metastore_cache.test_invalidate_metastore_cache_schema1.nation_partitioned"))
                .hasRowsCount(6)
                .contains(row(5));
        assertThat(onPresto().executeQuery("select tablecachesize, partitioncachesize, partitionnamescachesize from jmx.current.\"com.facebook.presto.hive.metastore:name=hive_with_metastore_cache,type=metastorecachestats\""))
                .contains(row(1, 6, 1));
    }

    @AfterTestWithContext
    public void tearDown()
    {
        onPresto().executeQuery("DROP TABLE IF EXISTS hive_with_metastore_cache.test_invalidate_metastore_cache_schema1.nation");
        onPresto().executeQuery("DROP TABLE IF EXISTS hive_with_metastore_cache.test_invalidate_metastore_cache_schema1.region");
        onPresto().executeQuery("DROP TABLE IF EXISTS hive_with_metastore_cache.test_invalidate_metastore_cache_schema1.nation_partitioned");
        onPresto().executeQuery("DROP SCHEMA IF EXISTS hive_with_metastore_cache.test_invalidate_metastore_cache_schema1");

        onPresto().executeQuery("DROP TABLE IF EXISTS hive_with_metastore_cache.test_invalidate_metastore_cache_schema2.nation");
        onPresto().executeQuery("DROP TABLE IF EXISTS hive_with_metastore_cache.test_invalidate_metastore_cache_schema2.region");
        onPresto().executeQuery("DROP SCHEMA IF EXISTS hive_with_metastore_cache.test_invalidate_metastore_cache_schema2");
    }
}
