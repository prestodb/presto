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

import com.google.common.primitives.Longs;
import io.prestodb.tempto.ProductTest;
import io.prestodb.tempto.Requires;
import io.prestodb.tempto.fulfillment.table.hive.tpch.ImmutableTpchTablesRequirements.ImmutableNationTable;
import io.prestodb.tempto.query.QueryExecutor;
import io.prestodb.tempto.query.QueryResult;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import static com.facebook.presto.tests.TestGroups.HIVE_TABLE_STATISTICS;
import static com.facebook.presto.tests.utils.QueryExecutors.onHive;
import static com.facebook.presto.tests.utils.QueryExecutors.onPresto;
import static com.google.common.base.Verify.verify;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Requires(ImmutableNationTable.class)
public class TestHiveBasicTableStatistics
        extends ProductTest
{
    @Test(groups = {HIVE_TABLE_STATISTICS})
    public void testCreateUnpartitioned()
    {
        String tableName = "test_basic_statistics_unpartitioned_ctas_presto";

        onPresto().executeQuery(format("DROP TABLE IF EXISTS %s", tableName));
        onPresto().executeQuery(format("CREATE TABLE %s AS SELECT * FROM nation", tableName));

        try {
            BasicStatistics statistics = getBasicStatisticsForTable(onHive(), tableName);
            assertThatStatisticsAreNonZero(statistics);
            assertThat(statistics.getNumRows().getAsLong()).isEqualTo(25);
        }
        finally {
            onPresto().executeQuery(format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    @Test(groups = {HIVE_TABLE_STATISTICS})
    public void testCreateTableWithNoData()
    {
        String tableName = "test_basic_statistics_unpartitioned_ctas_presto_with_no_data";

        onPresto().executeQuery(format("DROP TABLE IF EXISTS %s", tableName));
        onPresto().executeQuery(format("CREATE TABLE %s AS SELECT * FROM nation WITH NO DATA", tableName));

        try {
            BasicStatistics statistics = getBasicStatisticsForTable(onHive(), tableName);
            assertThatStatisticsAreZero(statistics);
        }
        finally {
            onPresto().executeQuery(format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    @Test(groups = {HIVE_TABLE_STATISTICS})
    public void testInsertUnpartitioned()
    {
        String tableName = "test_basic_statistics_unpartitioned_insert_presto";

        onPresto().executeQuery(format("DROP TABLE IF EXISTS %s", tableName));
        onPresto().executeQuery(format("" +
                "CREATE TABLE %s (" +
                "   n_nationkey bigint, " +
                "   n_regionkey bigint, " +
                "   n_name varchar(25), " +
                "   n_comment varchar(152)" +
                ")", tableName));

        try {
            BasicStatistics statisticsEmpty = getBasicStatisticsForTable(onHive(), tableName);
            assertThatStatisticsAreZero(statisticsEmpty);

            insertNationData(onPresto(), tableName);

            BasicStatistics statisticsFirstInsert = getBasicStatisticsForTable(onHive(), tableName);
            assertThatStatisticsAreNonZero(statisticsFirstInsert);
            assertThat(statisticsFirstInsert.getNumRows().getAsLong()).isEqualTo(25);

            insertNationData(onPresto(), tableName);

            BasicStatistics statisticsSecondInsert = getBasicStatisticsForTable(onHive(), tableName);
            assertThatStatisticsAreNonZero(statisticsSecondInsert);
            assertThatStatisticsValuesHaveIncreased(statisticsFirstInsert, statisticsSecondInsert);
            assertThat(statisticsSecondInsert.getNumRows().getAsLong()).isEqualTo(50);
        }
        finally {
            onPresto().executeQuery(format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    @Test(groups = {HIVE_TABLE_STATISTICS})
    public void testCreatePartitioned()
    {
        String tableName = "test_basic_statistics_partitioned_ctas_presto";

        onPresto().executeQuery(format("DROP TABLE IF EXISTS %s", tableName));
        onPresto().executeQuery(format("" +
                "CREATE TABLE %s " +
                "WITH (" +
                "   partitioned_by = ARRAY['n_regionkey'] " +
                ") " +
                "AS " +
                "SELECT n_nationkey, n_name, n_comment, n_regionkey " +
                "FROM nation " +
                // turns out there are exactly 5 countries in each region
                // let's change records count for one of the regions to verify statistics are different
                "WHERE n_nationkey <> 23", tableName));

        try {
            BasicStatistics tableStatistics = getBasicStatisticsForTable(onHive(), tableName);
            assertThatStatisticsAreNotPresent(tableStatistics);

            BasicStatistics firstPartitionStatistics = getBasicStatisticsForPartition(onHive(), tableName, "n_regionkey=1");
            assertThatStatisticsAreNonZero(firstPartitionStatistics);
            assertThat(firstPartitionStatistics.getNumRows().getAsLong()).isEqualTo(5);

            BasicStatistics secondPartitionStatistics = getBasicStatisticsForPartition(onHive(), tableName, "n_regionkey=3");
            assertThatStatisticsAreNonZero(secondPartitionStatistics);
            assertThat(secondPartitionStatistics.getNumRows().getAsLong()).isEqualTo(4);
        }
        finally {
            onPresto().executeQuery(format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    @Test(groups = {HIVE_TABLE_STATISTICS})
    public void testAnalyzePartitioned()
    {
        String tableName = "test_basic_statistics_analyze_partitioned";

        onPresto().executeQuery("DROP TABLE IF EXISTS " + tableName);
        onPresto().executeQuery(format("" +
                "CREATE TABLE %s " +
                "WITH ( " +
                "   partitioned_by = ARRAY['n_regionkey'], " +
                "   bucketed_by = ARRAY['n_nationkey'], " +
                "   bucket_count = 10 " +
                ") " +
                "AS " +
                "SELECT n_nationkey, n_name, n_comment, n_regionkey " +
                "FROM nation " +
                "WHERE n_regionkey = 1", tableName));

        try {
            BasicStatistics tableStatistics = getBasicStatisticsForTable(onHive(), tableName);
            assertThatStatisticsAreNotPresent(tableStatistics);

            BasicStatistics partitionStatisticsBefore = getBasicStatisticsForPartition(onHive(), tableName, "n_regionkey=1");
            assertThatStatisticsArePresent(partitionStatisticsBefore);

            // run ANALYZE
            onPresto().executeQuery(format("ANALYZE %s", tableName));
            BasicStatistics partitionStatisticsAfter = getBasicStatisticsForPartition(onHive(), tableName, "n_regionkey=1");
            assertThatStatisticsArePresent(partitionStatisticsAfter);

            // ANALYZE must not change the basic stats
            assertThat(partitionStatisticsBefore.getNumRows().getAsLong()).isEqualTo(partitionStatisticsAfter.getNumRows().getAsLong());
            assertThat(partitionStatisticsBefore.getNumFiles().getAsLong()).isEqualTo(partitionStatisticsAfter.getNumFiles().getAsLong());
            assertThat(partitionStatisticsBefore.getRawDataSize().getAsLong()).isEqualTo(partitionStatisticsAfter.getRawDataSize().getAsLong());
            assertThat(partitionStatisticsBefore.getTotalSize().getAsLong()).isEqualTo(partitionStatisticsAfter.getTotalSize().getAsLong());
        }
        finally {
            onPresto().executeQuery(format("DROP TABLE %s", tableName));
        }
    }

    @Test(groups = {HIVE_TABLE_STATISTICS})
    public void testAnalyzeUnpartitioned()
    {
        String tableName = "test_basic_statistics_analyze_unpartitioned";

        onPresto().executeQuery("DROP TABLE IF EXISTS " + tableName);
        onPresto().executeQuery(format("" +
                "CREATE TABLE %s " +
                "AS " +
                "SELECT n_nationkey, n_name, n_comment, n_regionkey " +
                "FROM nation " +
                "WHERE n_regionkey = 1", tableName));

        try {
            BasicStatistics tableStatisticsBefore = getBasicStatisticsForTable(onHive(), tableName);
            assertThatStatisticsArePresent(tableStatisticsBefore);

            // run ANALYZE
            onPresto().executeQuery(format("ANALYZE %s", tableName));
            BasicStatistics tableStatisticsAfter = getBasicStatisticsForTable(onHive(), tableName);
            assertThatStatisticsArePresent(tableStatisticsAfter);

            // ANALYZE must not change the basic stats
            assertThat(tableStatisticsBefore.getNumRows()).isEqualTo(tableStatisticsAfter.getNumRows());
            assertThat(tableStatisticsBefore.getNumFiles()).isEqualTo(tableStatisticsAfter.getNumFiles());
            assertThat(tableStatisticsBefore.getRawDataSize()).isEqualTo(tableStatisticsAfter.getRawDataSize());
            assertThat(tableStatisticsBefore.getTotalSize()).isEqualTo(tableStatisticsAfter.getTotalSize());
        }
        finally {
            onPresto().executeQuery(format("DROP TABLE %s", tableName));
        }
    }

    @Test(groups = {HIVE_TABLE_STATISTICS})
    public void testInsertPartitioned()
    {
        String tableName = "test_basic_statistics_partitioned_insert_presto";

        onPresto().executeQuery(format("DROP TABLE IF EXISTS %s", tableName));
        onPresto().executeQuery(format("" +
                "CREATE TABLE %s (" +
                "   n_nationkey bigint, " +
                "   n_name varchar(25), " +
                "   n_comment varchar(152), " +
                "   n_regionkey bigint " +
                ")" +
                "WITH (" +
                "   partitioned_by = ARRAY['n_regionkey'] " +
                ") ", tableName));

        try {
            BasicStatistics tableStatisticsAfterCreate = getBasicStatisticsForTable(onHive(), tableName);
            assertThatStatisticsAreNotPresent(tableStatisticsAfterCreate);

            insertNationData(onPresto(), tableName);

            BasicStatistics partitionStatisticsFirstInsert = getBasicStatisticsForPartition(onHive(), tableName, "n_regionkey=3");
            assertThatStatisticsAreNonZero(partitionStatisticsFirstInsert);
            assertThat(partitionStatisticsFirstInsert.getNumRows().getAsLong()).isEqualTo(5);

            insertNationData(onPresto(), tableName);

            BasicStatistics statisticsSecondInsert = getBasicStatisticsForPartition(onHive(), tableName, "n_regionkey=3");
            assertThat(statisticsSecondInsert.getNumRows().getAsLong()).isEqualTo(10);
            assertThatStatisticsValuesHaveIncreased(partitionStatisticsFirstInsert, statisticsSecondInsert);
        }
        finally {
            onPresto().executeQuery(format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    @Test(groups = {HIVE_TABLE_STATISTICS})
    public void testInsertBucketed()
    {
        String tableName = "test_basic_statistics_bucketed_insert_presto";

        onPresto().executeQuery(format("DROP TABLE IF EXISTS %s", tableName));
        onPresto().executeQuery(format("" +
                "CREATE TABLE %s " +
                "WITH ( " +
                "   bucketed_by = ARRAY['n_nationkey'], " +
                "   bucket_count = 50 " +
                ") " +
                "AS " +
                "SELECT n_nationkey, n_name, n_comment, n_regionkey " +
                "FROM nation", tableName));

        try {
            BasicStatistics statisticsAfterCreate = getBasicStatisticsForTable(onHive(), tableName);
            assertThatStatisticsAreNonZero(statisticsAfterCreate);
            assertThat(statisticsAfterCreate.getNumRows().getAsLong()).isEqualTo(25);
            assertThat(statisticsAfterCreate.getNumFiles().getAsLong()).isEqualTo(50);

            // Insert into bucketed unpartitioned table is unsupported
            assertThatThrownBy(() -> insertNationData(onPresto(), tableName))
                    .hasMessageContaining("Cannot insert into bucketed unpartitioned Hive table");

            BasicStatistics statisticsAfterInsert = getBasicStatisticsForTable(onHive(), tableName);
            assertThat(statisticsAfterInsert.getNumRows().getAsLong()).isEqualTo(25);
            assertThat(statisticsAfterCreate.getNumFiles().getAsLong()).isEqualTo(50);
        }
        finally {
            onPresto().executeQuery(format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    @Test(groups = {HIVE_TABLE_STATISTICS})
    public void testInsertBucketedPartitioned()
    {
        String tableName = "test_basic_statistics_bucketed_partitioned_insert_presto";

        onPresto().executeQuery(format("DROP TABLE IF EXISTS %s", tableName));
        onPresto().executeQuery(format("" +
                "CREATE TABLE %s " +
                "WITH ( " +
                "   partitioned_by = ARRAY['n_regionkey'], " +
                "   bucketed_by = ARRAY['n_nationkey'], " +
                "   bucket_count = 10 " +
                ") " +
                "AS " +
                "SELECT n_nationkey, n_name, n_comment, n_regionkey " +
                "FROM nation " +
                "WHERE n_regionkey = 1", tableName));

        try {
            BasicStatistics tableStatistics = getBasicStatisticsForTable(onHive(), tableName);
            assertThatStatisticsAreNotPresent(tableStatistics);

            BasicStatistics firstPartitionStatistics = getBasicStatisticsForPartition(onHive(), tableName, "n_regionkey=1");
            assertThatStatisticsAreNonZero(firstPartitionStatistics);
            assertThat(firstPartitionStatistics.getNumRows().getAsLong()).isEqualTo(5);
            assertThat(firstPartitionStatistics.getNumFiles().getAsLong()).isEqualTo(10);

            onPresto().executeQuery(format("" +
                    "INSERT INTO %s (n_nationkey, n_regionkey, n_name, n_comment) " +
                    "SELECT n_nationkey, n_regionkey, n_name, n_comment " +
                    "FROM nation " +
                    "WHERE n_regionkey = 2", tableName));

            BasicStatistics secondPartitionStatistics = getBasicStatisticsForPartition(onHive(), tableName, "n_regionkey=2");
            assertThat(secondPartitionStatistics.getNumRows().getAsLong()).isEqualTo(5);
            assertThat(secondPartitionStatistics.getNumFiles().getAsLong()).isEqualTo(10);

            // Insert into existing bucketed partition is not supported
            assertThatThrownBy(() -> insertNationData(onPresto(), tableName))
                    .hasMessageContaining("Cannot insert into existing partition of bucketed Hive table");

            BasicStatistics secondPartitionUpdatedStatistics = getBasicStatisticsForPartition(onHive(), tableName, "n_regionkey=2");
            assertThat(secondPartitionUpdatedStatistics.getNumRows().getAsLong()).isEqualTo(5);
            assertThat(secondPartitionUpdatedStatistics.getNumFiles().getAsLong()).isEqualTo(10);
        }
        finally {
            onPresto().executeQuery(format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    private static void insertNationData(QueryExecutor executor, String tableName)
    {
        executor.executeQuery(format("" +
                "INSERT INTO %s (n_nationkey, n_regionkey, n_name, n_comment) " +
                "SELECT n_nationkey, n_regionkey, n_name, n_comment FROM nation", tableName));
    }

    private static void assertThatStatisticsAreNonZero(BasicStatistics statistics)
    {
        assertThatStatisticsArePresent(statistics);
        assertThat(statistics.getNumRows().getAsLong()).isGreaterThan(0);
        assertThat(statistics.getNumFiles().getAsLong()).isGreaterThan(0);
        assertThat(statistics.getRawDataSize().getAsLong()).isGreaterThan(0);
        assertThat(statistics.getTotalSize().getAsLong()).isGreaterThan(0);
    }

    private static void assertThatStatisticsAreZero(BasicStatistics statistics)
    {
        assertThatStatisticsArePresent(statistics);
        assertThat(statistics.getNumRows().getAsLong()).isEqualTo(0);
        assertThat(statistics.getNumFiles().getAsLong()).isEqualTo(0);
        assertThat(statistics.getRawDataSize().getAsLong()).isEqualTo(0);
        assertThat(statistics.getTotalSize().getAsLong()).isEqualTo(0);
    }

    private static void assertThatStatisticsArePresent(BasicStatistics statistics)
    {
        assertThat(statistics.getNumRows()).isPresent();
        assertThat(statistics.getNumFiles()).isPresent();
        assertThat(statistics.getRawDataSize()).isPresent();
        assertThat(statistics.getTotalSize()).isPresent();
    }

    private static void assertThatStatisticsAreNotPresent(BasicStatistics statistics)
    {
        assertThat(statistics.getNumRows()).isNotPresent();
        assertThat(statistics.getNumFiles()).isNotPresent();
        assertThat(statistics.getRawDataSize()).isNotPresent();
        assertThat(statistics.getTotalSize()).isNotPresent();
    }

    private static void assertThatStatisticsValuesHaveIncreased(BasicStatistics first, BasicStatistics second)
    {
        assertThat(second.getNumRows().getAsLong()).isGreaterThan(first.getNumRows().getAsLong());
        assertThat(second.getNumFiles().getAsLong()).isGreaterThan(first.getNumFiles().getAsLong());
        assertThat(second.getTotalSize().getAsLong()).isGreaterThan(first.getTotalSize().getAsLong());
        assertThat(second.getRawDataSize().getAsLong()).isGreaterThan(first.getRawDataSize().getAsLong());
    }

    private static BasicStatistics getBasicStatisticsForTable(QueryExecutor executor, String table)
    {
        QueryResult result = executor.executeQuery(format("DESCRIBE FORMATTED %s", table));
        return basicStatisticsFromDescribeResult(result);
    }

    private static BasicStatistics getBasicStatisticsForPartition(QueryExecutor executor, String table, String partition)
    {
        QueryResult result = executor.executeQuery(format("DESCRIBE FORMATTED %s partition (%s)", table, partition));
        return basicStatisticsFromDescribeResult(result);
    }

    private static BasicStatistics basicStatisticsFromDescribeResult(QueryResult result)
    {
        OptionalLong numFiles = getTableParameterValue(result, "numFiles");
        OptionalLong numRows = getTableParameterValue(result, "numRows");
        OptionalLong rawDataSize = getTableParameterValue(result, "rawDataSize");
        OptionalLong totalSize = getTableParameterValue(result, "totalSize");
        return new BasicStatistics(numFiles, numRows, rawDataSize, totalSize);
    }

    private static OptionalLong getTableParameterValue(QueryResult describeResult, String key)
    {
        verify(describeResult.getColumnsCount() == 3, "describe result is expected to have 3 columns");
        for (List<Object> row : describeResult.rows()) {
            Optional<String> parameterKey = Optional.ofNullable(row.get(1))
                    .map(Object::toString)
                    .map(String::trim);

            if (parameterKey.isPresent() && key.equals(parameterKey.get())) {
                return Optional.ofNullable(row.get(2))
                        .map(Object::toString)
                        .map(String::trim)
                        .map(TestHiveBasicTableStatistics::tryParse)
                        .get();
            }
        }
        return OptionalLong.empty();
    }

    private static OptionalLong tryParse(String value)
    {
        Long number = Longs.tryParse(value);
        if (number != null) {
            return OptionalLong.of(number);
        }
        return OptionalLong.empty();
    }

    private static class BasicStatistics
    {
        private final OptionalLong numFiles;
        private final OptionalLong numRows;
        private final OptionalLong rawDataSize;
        private final OptionalLong totalSize;

        public BasicStatistics(OptionalLong numFiles, OptionalLong numRows, OptionalLong rawDataSize, OptionalLong totalSize)
        {
            this.numFiles = requireNonNull(numFiles, "numFiles is null");
            this.numRows = requireNonNull(numRows, "numRows is null");
            this.rawDataSize = requireNonNull(rawDataSize, "rawDataSize is null");
            this.totalSize = requireNonNull(totalSize, "totalSize is null");
        }

        public OptionalLong getNumFiles()
        {
            return numFiles;
        }

        public OptionalLong getNumRows()
        {
            return numRows;
        }

        public OptionalLong getRawDataSize()
        {
            return rawDataSize;
        }

        public OptionalLong getTotalSize()
        {
            return totalSize;
        }
    }
}
