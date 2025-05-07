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

import com.google.common.collect.ImmutableList;
import io.prestodb.tempto.ProductTest;
import io.prestodb.tempto.Requirement;
import io.prestodb.tempto.Requirements;
import io.prestodb.tempto.RequirementsProvider;
import io.prestodb.tempto.Requires;
import io.prestodb.tempto.assertions.QueryAssert.Row;
import io.prestodb.tempto.configuration.Configuration;
import io.prestodb.tempto.fulfillment.table.MutableTableRequirement;
import io.prestodb.tempto.fulfillment.table.hive.HiveTableDefinition;
import io.prestodb.tempto.fulfillment.table.hive.InlineDataSource;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.tests.TestGroups.SKIP_ON_CDH;
import static com.facebook.presto.tests.hive.AllSimpleTypesTableDefinitions.ALL_HIVE_SIMPLE_TYPES_TEXTFILE;
import static com.facebook.presto.tests.hive.HiveTableDefinitions.NATION_PARTITIONED_BY_BIGINT_REGIONKEY;
import static com.facebook.presto.tests.hive.HiveTableDefinitions.NATION_PARTITIONED_BY_VARCHAR_REGIONKEY;
import static com.facebook.presto.tests.utils.QueryExecutors.onHive;
import static io.prestodb.tempto.assertions.QueryAssert.Row.row;
import static io.prestodb.tempto.assertions.QueryAssert.anyOf;
import static io.prestodb.tempto.assertions.QueryAssert.assertThat;
import static io.prestodb.tempto.fulfillment.table.MutableTablesState.mutableTablesState;
import static io.prestodb.tempto.fulfillment.table.TableRequirements.mutableTable;
import static io.prestodb.tempto.fulfillment.table.hive.tpch.TpchTableDefinitions.NATION;
import static io.prestodb.tempto.query.QueryExecutor.query;
import static java.lang.String.format;

public class TestHiveTableStatistics
        extends ProductTest
{
    private static class UnpartitionedNationTable
            implements RequirementsProvider
    {
        @Override
        public Requirement getRequirements(Configuration configuration)
        {
            return mutableTable(
                    HiveTableDefinition.from(NATION)
                            .injectStats(false)
                            .build());
        }
    }

    private static class NationPartitionedByBigintTable
            implements RequirementsProvider
    {
        @Override
        public Requirement getRequirements(Configuration configuration)
        {
            return mutableTable(
                    HiveTableDefinition.from(NATION_PARTITIONED_BY_BIGINT_REGIONKEY)
                            .injectStats(false)
                            .build());
        }
    }

    private static class NationPartitionedByVarcharTable
            implements RequirementsProvider
    {
        @Override
        public Requirement getRequirements(Configuration configuration)
        {
            return mutableTable(
                    HiveTableDefinition.from(NATION_PARTITIONED_BY_VARCHAR_REGIONKEY)
                            .injectStats(false)
                            .build());
        }
    }

    private static final String ALL_TYPES_TABLE_NAME = "all_types";
    private static final String EMPTY_ALL_TYPES_TABLE_NAME = "empty_all_types";
    private static final String ALL_TYPES_ALL_NULL_TABLE_NAME = "all_types_all_null";

    private static final HiveTableDefinition ALL_TYPES_TABLE = HiveTableDefinition.like(ALL_HIVE_SIMPLE_TYPES_TEXTFILE)
            .setDataSource(InlineDataSource.createStringDataSource(
                    "all_analyzable_types",
                    "121|32761|2147483641|9223372036854775801|123.341|234.561|344.671|345.671|2015-05-10 12:15:31.123456|2015-05-09|ela ma kota|ela ma kot|ela ma    |false|cGllcyBiaW5hcm55|\n" +
                            "127|32767|2147483647|9223372036854775807|123.345|235.567|345.678|345.678|2015-05-10 12:15:35.123456|2015-06-10|ala ma kota|ala ma kot|ala ma    |true|a290IGJpbmFybnk=|\n"))
            .build();

    private static final HiveTableDefinition ALL_TYPES_ALL_NULL_TABLE = HiveTableDefinition.like(ALL_HIVE_SIMPLE_TYPES_TEXTFILE)
            .setDataSource(InlineDataSource.createStringDataSource(
                    "all_analyzable_types_all_null",
                    "\\N|\\N|\\N|\\N|\\N|\\N|\\N|\\N|\\N|\\N|\\N|\\N|\\N|\\N|\\N|\n"))
            .build();

    private static final List<Row> ALL_TYPES_TABLE_STATISTICS = ImmutableList.of(
            row("c_tinyint", null, 2.0, 0.0, null, "121", "127", null),
            row("c_smallint", null, 2.0, 0.0, null, "32761", "32767", null),
            row("c_int", null, 2.0, 0.0, null, "2147483641", "2147483647", null),
            row("c_bigint", null, 2.0, 0.0, null, "9223372036854775807", "9223372036854775807", null),
            row("c_float", null, 2.0, 0.0, null, "123.341", "123.345", null),
            row("c_double", null, 2.0, 0.0, null, "234.561", "235.567", null),
            row("c_decimal", null, 2.0, 0.0, null, "345.0", "346.0", null),
            row("c_decimal_w_params", null, 2.0, 0.0, null, "345.671", "345.678", null),
            row("c_timestamp", null, 2.0, 0.0, null, null, null, null),
            row("c_date", null, 2.0, 0.0, null, "2015-05-09", "2015-06-10", null),
            row("c_string", 22.0, 2.0, 0.0, null, null, null, null),
            row("c_varchar", 20.0, 2.0, 0.0, null, null, null, null),
            row("c_char", 12.0, 2.0, 0.0, null, null, null, null),
            row("c_boolean", null, 2.0, 0.0, null, null, null, null),
            row("c_binary", 23.0, null, 0.0, null, null, null, null),
            row(null, null, null, null, 2.0, null, null, null));

    private static final List<Row> ALL_TYPES_ALL_NULL_TABLE_STATISTICS = ImmutableList.of(
            row("c_tinyint", null, 0.0, 1.0, null, null, null, null),
            row("c_smallint", null, 0.0, 1.0, null, null, null, null),
            row("c_int", null, 0.0, 1.0, null, null, null, null),
            row("c_bigint", null, 0.0, 1.0, null, null, null, null),
            row("c_float", null, 0.0, 1.0, null, null, null, null),
            row("c_double", null, 0.0, 1.0, null, null, null, null),
            row("c_decimal", null, 0.0, 1.0, null, null, null, null),
            row("c_decimal_w_params", null, 0.0, 1.0, null, null, null, null),
            row("c_timestamp", null, 0.0, 1.0, null, null, null, null),
            row("c_date", null, 0.0, 1.0, null, null, null, null),
            row("c_string", 0.0, 0.0, 1.0, null, null, null, null),
            row("c_varchar", 0.0, 0.0, 1.0, null, null, null, null),
            row("c_char", 0.0, 0.0, 1.0, null, null, null, null),
            row("c_boolean", null, 0.0, 1.0, null, null, null, null),
            row("c_binary", 0.0, null, 1.0, null, null, null, null),
            row(null, null, null, null, 1.0, null, null, null));

    private static final List<Row> ALL_TYPES_EMPTY_TABLE_STATISTICS = ImmutableList.of(
            row("c_tinyint", null, 0.0, 0.0, null, null, null, null),
            row("c_smallint", null, 0.0, 0.0, null, null, null, null),
            row("c_int", null, 0.0, 0.0, null, null, null, null),
            row("c_bigint", null, 0.0, 0.0, null, null, null, null),
            row("c_float", null, 0.0, 0.0, null, null, null, null),
            row("c_double", null, 0.0, 0.0, null, null, null, null),
            row("c_decimal", null, 0.0, 0.0, null, null, null, null),
            row("c_decimal_w_params", null, 0.0, 0.0, null, null, null, null),
            row("c_timestamp", null, 0.0, 0.0, null, null, null, null),
            row("c_date", null, 0.0, 0.0, null, null, null, null),
            row("c_string", 0.0, 0.0, 0.0, null, null, null, null),
            row("c_varchar", 0.0, 0.0, 0.0, null, null, null, null),
            row("c_char", 0.0, 0.0, 0.0, null, null, null, null),
            row("c_boolean", null, 0.0, 0.0, null, null, null, null),
            row("c_binary", 0.0, null, 0.0, null, null, null, null),
            row(null, null, null, null, 0.0, null, null, null));

    private static final class AllTypesTable
            implements RequirementsProvider
    {
        @Override
        public Requirement getRequirements(Configuration configuration)
        {
            return Requirements.compose(
                    mutableTable(ALL_TYPES_TABLE, ALL_TYPES_TABLE_NAME, MutableTableRequirement.State.LOADED),
                    mutableTable(ALL_TYPES_ALL_NULL_TABLE, ALL_TYPES_ALL_NULL_TABLE_NAME, MutableTableRequirement.State.LOADED),
                    mutableTable(ALL_TYPES_TABLE, EMPTY_ALL_TYPES_TABLE_NAME, MutableTableRequirement.State.CREATED));
        }
    }

    @Test
    @Requires(UnpartitionedNationTable.class)
    public void testStatisticsForUnpartitionedTable()
    {
        String tableNameInDatabase = mutableTablesState().get(NATION.getName()).getNameInDatabase();

        String showStatsWholeTable = "SHOW STATS FOR " + tableNameInDatabase;

        // table not analyzed

        assertThat(query(showStatsWholeTable)).containsOnly(
                row("n_nationkey", null, null, anyOf(null, 0.0), null, null, null, null),
                row("n_name", null, null, anyOf(null, 0.0), null, null, null, null),
                row("n_regionkey", null, null, anyOf(null, 0.0), null, null, null, null),
                row("n_comment", null, null, anyOf(null, 0.0), null, null, null, null),
                row(null, null, null, null, anyOf(null, 0.0), null, null, null)); // anyOf because of different behaviour on HDP (hive 1.2) and CDH (hive 1.1)

        // basic analysis

        onHive().executeQuery("ANALYZE TABLE " + tableNameInDatabase + " COMPUTE STATISTICS");

        assertThat(query(showStatsWholeTable)).containsOnly(
                row("n_nationkey", null, null, null, null, null, null, null),
                row("n_name", null, null, null, null, null, null, null),
                row("n_regionkey", null, null, null, null, null, null, null),
                row("n_comment", null, null, null, null, null, null, null),
                row(null, null, null, null, 25.0, null, null, null));

        // column analysis

        onHive().executeQuery("ANALYZE TABLE " + tableNameInDatabase + " COMPUTE STATISTICS FOR COLUMNS");

        assertThat(query(showStatsWholeTable)).containsOnly(
                row("n_nationkey", null, 25.0, 0.0, null, "0", "24", null),
                row("n_name", 177.0, 25.0, 0.0, null, null, null, null),
                row("n_regionkey", null, 5.0, 0.0, null, "0", "4", null),
                row("n_comment", 1857.0, 25.0, 0.0, null, null, null, null),
                row(null, null, null, null, 25.0, null, null, null));
    }

    @Test
    @Requires(NationPartitionedByBigintTable.class)
    public void testStatisticsForTablePartitionedByBigint()
    {
        String tableNameInDatabase = mutableTablesState().get(NATION_PARTITIONED_BY_BIGINT_REGIONKEY.getName()).getNameInDatabase();

        String showStatsWholeTable = "SHOW STATS FOR " + tableNameInDatabase;
        String showStatsPartitionOne = "SHOW STATS FOR (SELECT * FROM " + tableNameInDatabase + " WHERE p_regionkey = 1)";
        String showStatsPartitionTwo = "SHOW STATS FOR (SELECT * FROM " + tableNameInDatabase + " WHERE p_regionkey = 2)";

        // table not analyzed

        assertThat(query(showStatsWholeTable)).containsOnly(
                row("p_nationkey", null, null, null, null, null, null, null),
                row("p_name", null, null, null, null, null, null, null),
                row("p_regionkey", null, null, null, null, null, null, null),
                row("p_comment", null, null, null, null, null, null, null),
                row(null, null, null, null, null, null, null, null));

        assertThat(query(showStatsPartitionOne)).containsOnly(
                row("p_nationkey", null, null, null, null, null, null, null),
                row("p_name", null, null, null, null, null, null, null),
                row("p_regionkey", null, null, null, null, null, null, null),
                row("p_comment", null, null, null, null, null, null, null),
                row(null, null, null, null, null, null, null, null));

        // basic analysis for single partition

        onHive().executeQuery("ANALYZE TABLE " + tableNameInDatabase + " PARTITION (p_regionkey = \"1\") COMPUTE STATISTICS");

        assertThat(query(showStatsWholeTable)).containsOnly(
                row("p_nationkey", null, null, null, null, null, null, null),
                row("p_name", null, null, null, null, null, null, null),
                row("p_regionkey", null, 3.0, 0.0, null, "1", "3", null),
                row("p_comment", null, null, null, null, null, null, null),
                row(null, null, null, null, 15.0, null, null, null));

        assertThat(query(showStatsPartitionOne)).containsOnly(
                row("p_nationkey", null, null, null, null, null, null, null),
                row("p_name", null, null, null, null, null, null, null),
                row("p_regionkey", null, 1.0, 0.0, null, "1", "1", null),
                row("p_comment", null, null, null, null, null, null, null),
                row(null, null, null, null, 5.0, null, null, null));

        assertThat(query(showStatsPartitionTwo)).containsOnly(
                row("p_nationkey", null, null, null, null, null, null, null),
                row("p_name", null, null, null, null, null, null, null),
                row("p_regionkey", null, null, null, null, null, null, null),
                row("p_comment", null, null, null, null, null, null, null),
                row(null, null, null, null, null, null, null, null));

        // basic analysis for all partitions

        onHive().executeQuery("ANALYZE TABLE " + tableNameInDatabase + " PARTITION (p_regionkey) COMPUTE STATISTICS");

        assertThat(query(showStatsWholeTable)).containsOnly(
                row("p_nationkey", null, null, null, null, null, null, null),
                row("p_name", null, null, null, null, null, null, null),
                row("p_regionkey", null, 3.0, 0.0, null, "1", "3", null),
                row("p_comment", null, null, null, null, null, null, null),
                row(null, null, null, null, 15.0, null, null, null));

        assertThat(query(showStatsPartitionOne)).containsOnly(
                row("p_nationkey", null, null, null, null, null, null, null),
                row("p_name", null, null, null, null, null, null, null),
                row("p_regionkey", null, 1.0, 0.0, null, "1", "1", null),
                row("p_comment", null, null, null, null, null, null, null),
                row(null, null, null, null, 5.0, null, null, null));

        assertThat(query(showStatsPartitionTwo)).containsOnly(
                row("p_nationkey", null, null, null, null, null, null, null),
                row("p_name", null, null, null, null, null, null, null),
                row("p_regionkey", null, 1.0, 0.0, null, "2", "2", null),
                row("p_comment", null, null, null, null, null, null, null),
                row(null, null, null, null, 5.0, null, null, null));

        // column analysis for single partition

        onHive().executeQuery("ANALYZE TABLE " + tableNameInDatabase + " PARTITION (p_regionkey = \"1\") COMPUTE STATISTICS FOR COLUMNS");

        assertThat(query(showStatsWholeTable)).containsOnly(
                row("p_nationkey", null, 5.0, 0.0, null, "1", "24", null),
                row("p_name", 114.0, 5.0, 0.0, null, null, null, null),
                row("p_regionkey", null, 3.0, 0.0, null, "1", "3", null),
                row("p_comment", 1497.0, 5.0, 0.0, null, null, null, null),
                row(null, null, null, null, 15.0, null, null, null));

        assertThat(query(showStatsPartitionOne)).containsOnly(
                row("p_nationkey", null, 5.0, 0.0, null, "1", "24", null),
                row("p_name", 38.0, 5.0, 0.0, null, null, null, null),
                row("p_regionkey", null, 1.0, 0.0, null, "1", "1", null),
                row("p_comment", 499.0, 5.0, 0.0, null, null, null, null),
                row(null, null, null, null, 5.0, null, null, null));

        assertThat(query(showStatsPartitionTwo)).containsOnly(
                row("p_nationkey", null, null, null, null, null, null, null),
                row("p_name", null, null, null, null, null, null, null),
                row("p_regionkey", null, 1.0, 0.0, null, "2", "2", null),
                row("p_comment", null, null, null, null, null, null, null),
                row(null, null, null, null, 5.0, null, null, null));

        // column analysis for all partitions

        onHive().executeQuery("ANALYZE TABLE " + tableNameInDatabase + " PARTITION (p_regionkey) COMPUTE STATISTICS FOR COLUMNS");

        assertThat(query(showStatsWholeTable)).containsOnly(
                row("p_nationkey", null, 5.0, 0.0, null, "1", "24", null),
                row("p_name", 109.0, 5.0, 0.0, null, null, null, null),
                row("p_regionkey", null, 3.0, 0.0, null, "1", "3", null),
                row("p_comment", 1197.0, 5.0, 0.0, null, null, null, null),
                row(null, null, null, null, 15.0, null, null, null));

        assertThat(query(showStatsPartitionOne)).containsOnly(
                row("p_nationkey", null, 5.0, 0.0, null, "1", "24", null),
                row("p_name", 38.0, 5.0, 0.0, null, null, null, null),
                row("p_regionkey", null, 1.0, 0.0, null, "1", "1", null),
                row("p_comment", 499.0, 5.0, 0.0, null, null, null, null),
                row(null, null, null, null, 5.0, null, null, null));

        assertThat(query(showStatsPartitionTwo)).containsOnly(
                row("p_nationkey", null, 5.0, 0.0, null, "8", "21", null),
                row("p_name", 31.0, 5.0, 0.0, null, null, null, null),
                row("p_regionkey", null, 1.0, 0.0, null, "2", "2", null),
                row("p_comment", 351.0, 5.0, 0.0, null, null, null, null),
                row(null, null, null, null, 5.0, null, null, null));
    }

    @Test
    @Requires(NationPartitionedByVarcharTable.class)
    public void testStatisticsForTablePartitionedByVarchar()
    {
        String tableNameInDatabase = mutableTablesState().get(NATION_PARTITIONED_BY_VARCHAR_REGIONKEY.getName()).getNameInDatabase();

        String showStatsWholeTable = "SHOW STATS FOR " + tableNameInDatabase;
        String showStatsPartitionOne = "SHOW STATS FOR (SELECT * FROM " + tableNameInDatabase + " WHERE p_regionkey = 'AMERICA')";
        String showStatsPartitionTwo = "SHOW STATS FOR (SELECT * FROM " + tableNameInDatabase + " WHERE p_regionkey = 'ASIA')";

        // table not analyzed

        assertThat(query(showStatsWholeTable)).containsOnly(
                row("p_nationkey", null, null, null, null, null, null, null),
                row("p_name", null, null, null, null, null, null, null),
                row("p_regionkey", null, null, null, null, null, null, null),
                row("p_comment", null, null, null, null, null, null, null),
                row(null, null, null, null, null, null, null, null));

        assertThat(query(showStatsPartitionOne)).containsOnly(
                row("p_nationkey", null, null, null, null, null, null, null),
                row("p_name", null, null, null, null, null, null, null),
                row("p_regionkey", null, null, null, null, null, null, null),
                row("p_comment", null, null, null, null, null, null, null),
                row(null, null, null, null, null, null, null, null));

        // basic analysis for single partition

        onHive().executeQuery("ANALYZE TABLE " + tableNameInDatabase + " PARTITION (p_regionkey = \"AMERICA\") COMPUTE STATISTICS");

        assertThat(query(showStatsWholeTable)).containsOnly(
                row("p_nationkey", null, null, null, null, null, null, null),
                row("p_name", null, null, null, null, null, null, null),
                row("p_regionkey", 85.0, 3.0, 0.0, null, null, null, null),
                row("p_comment", null, null, null, null, null, null, null),
                row(null, null, null, null, 15.0, null, null, null));

        assertThat(query(showStatsPartitionOne)).containsOnly(
                row("p_nationkey", null, null, null, null, null, null, null),
                row("p_name", null, null, null, null, null, null, null),
                row("p_regionkey", 35.0, 1.0, 0.0, null, null, null, null),
                row("p_comment", null, null, null, null, null, null, null),
                row(null, null, null, null, 5.0, null, null, null));

        assertThat(query(showStatsPartitionTwo)).containsOnly(
                row("p_nationkey", null, null, null, null, null, null, null),
                row("p_name", null, null, null, null, null, null, null),
                row("p_regionkey", null, null, null, null, null, null, null),
                row("p_comment", null, null, null, null, null, null, null),
                row(null, null, null, null, null, null, null, null));

        // basic analysis for all partitions

        onHive().executeQuery("ANALYZE TABLE " + tableNameInDatabase + " PARTITION (p_regionkey) COMPUTE STATISTICS");

        assertThat(query(showStatsWholeTable)).containsOnly(
                row("p_nationkey", null, null, null, null, null, null, null),
                row("p_name", null, null, null, null, null, null, null),
                row("p_regionkey", 85.0, 3.0, 0.0, null, null, null, null),
                row("p_comment", null, null, null, null, null, null, null),
                row(null, null, null, null, 15.0, null, null, null));

        assertThat(query(showStatsPartitionOne)).containsOnly(
                row("p_nationkey", null, null, null, null, null, null, null),
                row("p_name", null, null, null, null, null, null, null),
                row("p_regionkey", 35.0, 1.0, 0.0, null, null, null, null),
                row("p_comment", null, null, null, null, null, null, null),
                row(null, null, null, null, 5.0, null, null, null));

        assertThat(query(showStatsPartitionTwo)).containsOnly(
                row("p_nationkey", null, null, null, null, null, null, null),
                row("p_name", null, null, null, null, null, null, null),
                row("p_regionkey", 20.0, 1.0, 0.0, null, null, null, null),
                row("p_comment", null, null, null, null, null, null, null),
                row(null, null, null, null, 5.0, null, null, null));

        // column analysis for single partition

        onHive().executeQuery("ANALYZE TABLE " + tableNameInDatabase + " PARTITION (p_regionkey = \"AMERICA\") COMPUTE STATISTICS FOR COLUMNS");

        assertThat(query(showStatsWholeTable)).containsOnly(
                row("p_nationkey", null, 5.0, 0.0, null, "1", "24", null),
                row("p_name", 114.0, 5.0, 0.0, null, null, null, null),
                row("p_regionkey", 85.0, 3.0, 0.0, null, null, null, null),
                row("p_comment", 1497.0, 5.0, 0.0, null, null, null, null),
                row(null, null, null, null, 15.0, null, null, null));

        assertThat(query(showStatsPartitionOne)).containsOnly(
                row("p_nationkey", null, 5.0, 0.0, null, "1", "24", null),
                row("p_name", 38.0, 5.0, 0.0, null, null, null, null),
                row("p_regionkey", 35.0, 1.0, 0.0, null, null, null, null),
                row("p_comment", 499.0, 5.0, 0.0, null, null, null, null),
                row(null, null, null, null, 5.0, null, null, null));

        assertThat(query(showStatsPartitionTwo)).containsOnly(
                row("p_nationkey", null, null, null, null, null, null, null),
                row("p_name", null, null, null, null, null, null, null),
                row("p_regionkey", 20.0, 1.0, 0.0, null, null, null, null),
                row("p_comment", null, null, null, null, null, null, null),
                row(null, null, null, null, 5.0, null, null, null));

        // column analysis for all partitions

        onHive().executeQuery("ANALYZE TABLE " + tableNameInDatabase + " PARTITION (p_regionkey) COMPUTE STATISTICS FOR COLUMNS");

        assertThat(query(showStatsWholeTable)).containsOnly(
                row("p_nationkey", null, 5.0, 0.0, null, "1", "24", null),
                row("p_name", 109.0, 5.0, 0.0, null, null, null, null),
                row("p_regionkey", 85.0, 3.0, 0.0, null, null, null, null),
                row("p_comment", 1197.0, 5.0, 0.0, null, null, null, null),
                row(null, null, null, null, 15.0, null, null, null));

        assertThat(query(showStatsPartitionOne)).containsOnly(
                row("p_nationkey", null, 5.0, 0.0, null, "1", "24", null),
                row("p_name", 38.0, 5.0, 0.0, null, null, null, null),
                row("p_regionkey", 35.0, 1.0, 0.0, null, null, null, null),
                row("p_comment", 499.0, 5.0, 0.0, null, null, null, null),
                row(null, null, null, null, 5.0, null, null, null));

        assertThat(query(showStatsPartitionTwo)).containsOnly(
                row("p_nationkey", null, 5.0, 0.0, null, "8", "21", null),
                row("p_name", 31.0, 5.0, 0.0, null, null, null, null),
                row("p_regionkey", 20.0, 1.0, 0.0, null, null, null, null),
                row("p_comment", 351.0, 5.0, 0.0, null, null, null, null),
                row(null, null, null, null, 5.0, null, null, null));
    }

    // This covers also stats calculation for unpartitioned table
    @Test(groups = {SKIP_ON_CDH}) // skip on cdh due to no support for date column and stats
    @Requires(AllTypesTable.class)
    public void testStatisticsForAllDataTypes()
    {
        String tableNameInDatabase = mutableTablesState().get(ALL_TYPES_TABLE_NAME).getNameInDatabase();

        onHive().executeQuery("ANALYZE TABLE " + tableNameInDatabase + " COMPUTE STATISTICS");

        assertThat(query("SHOW STATS FOR " + tableNameInDatabase)).containsOnly(
                row("c_tinyint", null, null, null, null, null, null, null),
                row("c_smallint", null, null, null, null, null, null, null),
                row("c_int", null, null, null, null, null, null, null),
                row("c_bigint", null, null, null, null, null, null, null),
                row("c_float", null, null, null, null, null, null, null),
                row("c_double", null, null, null, null, null, null, null),
                row("c_decimal", null, null, null, null, null, null, null),
                row("c_decimal_w_params", null, null, null, null, null, null, null),
                row("c_timestamp", null, null, null, null, null, null, null),
                row("c_date", null, null, null, null, null, null, null),
                row("c_string", null, null, null, null, null, null, null),
                row("c_varchar", null, null, null, null, null, null, null),
                row("c_char", null, null, null, null, null, null, null),
                row("c_boolean", null, null, null, null, null, null, null),
                row("c_binary", null, null, null, null, null, null, null),
                row(null, null, null, null, 2.0, null, null, null));

        onHive().executeQuery("ANALYZE TABLE " + tableNameInDatabase + " COMPUTE STATISTICS FOR COLUMNS");

        // SHOW STATS FORMAT: column_name, data_size, distinct_values_count, nulls_fraction, row_count
        assertThat(query("SHOW STATS FOR " + tableNameInDatabase)).containsOnly(
                row("c_tinyint", null, 2.0, 0.0, null, "121", "127", null),
                row("c_smallint", null, 2.0, 0.0, null, "32761", "32767", null),
                row("c_int", null, 2.0, 0.0, null, "2147483641", "2147483647", null),
                row("c_bigint", null, 2.0, 0.0, null, "9223372036854775807", "9223372036854775807", null),
                row("c_float", null, 2.0, 0.0, null, "123.341", "123.345", null),
                row("c_double", null, 2.0, 0.0, null, "234.561", "235.567", null),
                row("c_decimal", null, 2.0, 0.0, null, "345.0", "346.0", null),
                row("c_decimal_w_params", null, 2.0, 0.0, null, "345.671", "345.678", null),
                row("c_timestamp", null, 2.0, 0.0, null, null, null, null), // timestamp is shifted by hive.time-zone on read
                row("c_date", null, 2.0, 0.0, null, "2015-05-09", "2015-06-10", null),
                row("c_string", 22.0, 2.0, 0.0, null, null, null, null),
                row("c_varchar", 20.0, 2.0, 0.0, null, null, null, null),
                row("c_char", 12.0, 2.0, 0.0, null, null, null, null),
                row("c_boolean", null, 2.0, 0.0, null, null, null, null),
                row("c_binary", 23.0, null, 0.0, null, null, null, null),
                row(null, null, null, null, 2.0, null, null, null));
    }

    @Test(groups = {SKIP_ON_CDH}) // skip on cdh due to no support for date column and stats
    @Requires(AllTypesTable.class)
    public void testStatisticsForAllDataTypesNoData()
    {
        String tableNameInDatabase = mutableTablesState().get(EMPTY_ALL_TYPES_TABLE_NAME).getNameInDatabase();

        onHive().executeQuery("ANALYZE TABLE " + tableNameInDatabase + " COMPUTE STATISTICS");

        assertThat(query("SHOW STATS FOR " + tableNameInDatabase)).containsOnly(
                row("c_tinyint", null, null, null, null, null, null, null),
                row("c_smallint", null, null, null, null, null, null, null),
                row("c_int", null, null, null, null, null, null, null),
                row("c_bigint", null, null, null, null, null, null, null),
                row("c_float", null, null, null, null, null, null, null),
                row("c_double", null, null, null, null, null, null, null),
                row("c_decimal", null, null, null, null, null, null, null),
                row("c_decimal_w_params", null, null, null, null, null, null, null),
                row("c_timestamp", null, null, null, null, null, null, null),
                row("c_date", null, null, null, null, null, null, null),
                row("c_string", null, null, null, null, null, null, null),
                row("c_varchar", null, null, null, null, null, null, null),
                row("c_char", null, null, null, null, null, null, null),
                row("c_boolean", null, null, null, null, null, null, null),
                row("c_binary", null, null, null, null, null, null, null),
                row(null, null, null, null, 0.0, null, null, null));

        onHive().executeQuery("ANALYZE TABLE " + tableNameInDatabase + " COMPUTE STATISTICS FOR COLUMNS");

        assertThat(query("SHOW STATS FOR " + tableNameInDatabase)).containsOnly(
                row("c_tinyint", null, 0.0, 0.0, null, null, null, null),
                row("c_smallint", null, 0.0, 0.0, null, null, null, null),
                row("c_int", null, 0.0, 0.0, null, null, null, null),
                row("c_bigint", null, 0.0, 0.0, null, null, null, null),
                row("c_float", null, 0.0, 0.0, null, null, null, null),
                row("c_double", null, 0.0, 0.0, null, null, null, null),
                row("c_decimal", null, 0.0, 0.0, null, null, null, null),
                row("c_decimal_w_params", null, 0.0, 0.0, null, null, null, null),
                row("c_timestamp", null, 0.0, 0.0, null, null, null, null),
                row("c_date", null, 0.0, 0.0, null, null, null, null),
                row("c_string", 0.0, 0.0, 0.0, null, null, null, null),
                row("c_varchar", 0.0, 0.0, 0.0, null, null, null, null),
                row("c_char", 0.0, 0.0, 0.0, null, null, null, null),
                row("c_boolean", null, 0.0, 0.0, null, null, null, null),
                row("c_binary", 0.0, null, 0.0, null, null, null, null),
                row(null, null, null, null, 0.0, null, null, null));
    }

    @Test(groups = {SKIP_ON_CDH}) // skip on cdh due to no support for date column and stats
    @Requires(AllTypesTable.class)
    public void testStatisticsForAllDataTypesOnlyNulls()
    {
        String tableNameInDatabase = mutableTablesState().get(EMPTY_ALL_TYPES_TABLE_NAME).getNameInDatabase();
        onHive().executeQuery("INSERT INTO TABLE " + tableNameInDatabase + " VALUES(null, null, null, null, null, null, null, null, null, null, null, null, null, null, null)");

        assertThat(query("SHOW STATS FOR " + tableNameInDatabase)).containsOnly(
                row("c_tinyint", null, 0.0, 1.0, null, null, null, null),
                row("c_smallint", null, 0.0, 1.0, null, null, null, null),
                row("c_int", null, 0.0, 1.0, null, null, null, null),
                row("c_bigint", null, 0.0, 1.0, null, null, null, null),
                row("c_float", null, 0.0, 1.0, null, null, null, null),
                row("c_double", null, 0.0, 1.0, null, null, null, null),
                row("c_decimal", null, 0.0, 1.0, null, null, null, null),
                row("c_decimal_w_params", null, 0.0, 1.0, null, null, null, null),
                row("c_timestamp", null, 0.0, 1.0, null, null, null, null),
                row("c_date", null, 0.0, 1.0, null, null, null, null),
                row("c_string", 0.0, 0.0, 1.0, null, null, null, null),
                row("c_varchar", 0.0, 0.0, 1.0, null, null, null, null),
                row("c_char", 0.0, 0.0, 1.0, null, null, null, null),
                row("c_boolean", null, 0.0, 1.0, null, null, null, null),
                row("c_binary", 0.0, null, 1.0, null, null, null, null),
                row(null, null, null, null, 1.0, null, null, null));
    }

    @Test
    @Requires(UnpartitionedNationTable.class)
    public void testStatisticsForSkewedTable()
    {
        String tableName = "test_hive_skewed_table_statistics";
        onHive().executeQuery("DROP TABLE IF EXISTS " + tableName);
        onHive().executeQuery("CREATE TABLE " + tableName + " (c_string STRING, c_int INT) SKEWED BY (c_string) ON ('c1')");
        onHive().executeQuery("INSERT INTO TABLE " + tableName + " VALUES ('c1', 1), ('c1', 2)");

        assertThat(query("SHOW STATS FOR " + tableName)).containsOnly(
                row("c_string", 4.0, 1.0, 0.0, null, null, null, null),
                row("c_int", null, 2.0, 0.0, null, "1", "2", null),
                row(null, null, null, null, 2.0, null, null, null));
    }

    @Test
    @Requires(UnpartitionedNationTable.class)
    public void testAnalyzesForSkewedTable()
    {
        String tableName = "test_analyze_skewed_table";
        onHive().executeQuery("DROP TABLE IF EXISTS " + tableName);
        onHive().executeQuery("CREATE TABLE " + tableName + " (c_string STRING, c_int INT) SKEWED BY (c_string) ON ('c1')");
        onHive().executeQuery("INSERT INTO TABLE " + tableName + " VALUES ('c1', 1), ('c1', 2)");

        assertThat(query("SHOW STATS FOR " + tableName)).containsOnly(
                row("c_string", 4.0, 1.0, 0.0, null, null, null, null),
                row("c_int", null, 2.0, 0.0, null, "1", "2", null),
                row(null, null, null, null, 2.0, null, null, null));
    }

    @Test
    @Requires(UnpartitionedNationTable.class)
    public void testAnalyzeForUnpartitionedTable()
    {
        String tableNameInDatabase = mutableTablesState().get(NATION.getName()).getNameInDatabase();

        String showStatsWholeTable = "SHOW STATS FOR " + tableNameInDatabase;

        // table not analyzed
        assertThat(query(showStatsWholeTable)).containsOnly(
                row("n_nationkey", null, null, anyOf(null, 0.0), null, null, null, null),
                row("n_name", null, null, anyOf(null, 0.0), null, null, null, null),
                row("n_regionkey", null, null, anyOf(null, 0.0), null, null, null, null),
                row("n_comment", null, null, anyOf(null, 0.0), null, null, null, null),
                row(null, null, null, null, anyOf(null, 0.0), null, null, null)); // anyOf because of different behaviour on HDP (hive 1.2) and CDH (hive 1.1)

        assertThat(query("ANALYZE " + tableNameInDatabase)).containsExactly(row(25));

        assertThat(query(showStatsWholeTable)).containsOnly(
                row("n_nationkey", null, 25.0, 0.0, null, "0", "24", null),
                row("n_name", 177.0, 25.0, 0.0, null, null, null, null),
                row("n_regionkey", null, 5.0, 0.0, null, "0", "4", null),
                row("n_comment", 1857.0, 25.0, 0.0, null, null, null, null),
                row(null, null, null, null, 25.0, null, null, null));
    }

    @Test
    public void testAnalyzeForTableWithNonPrimitiveTypes()
    {
        String tableName = "test_analyze_table_complex_types";
        String showStatsTable = "SHOW STATS FOR " + tableName;

        query("DROP TABLE IF EXISTS " + tableName);
        query("CREATE TABLE " + tableName + "(c_row row(c1 int, c2 int), c_char char, c_int int)");
        query("INSERT INTO " + tableName + " VALUES (row(1,2), 'a', 3)");

        assertThat(query("ANALYZE " + tableName)).containsExactly(row(1));
        assertThat(query(showStatsTable)).containsOnly(
                row("c_row", null, null, null, null, null, null, null),
                row("c_char", 1.0, 1.0, 0.0, null, null, null, null),
                row("c_int", null, 1.0, 0.0, null, "3", "3", null),
                row(null, null, null, null, 1.0, null, null, null));
    }

    @Test
    public void testAnalyzeForPartitionedTableWithNonPrimitiveTypes()
    {
        String tableName = "test_analyze_partitioned_table_complex_types";
        String showStatsTable = "SHOW STATS FOR " + tableName;

        query("DROP TABLE IF EXISTS " + tableName);
        query("CREATE TABLE " + tableName +
                "(c_row row(c1 int, c2 int), c_char char, c_int int, c_part char) " +
                "WITH (partitioned_by = ARRAY['c_part'])");
        query("INSERT INTO " + tableName + " VALUES (row(1,2), 'a', 3, '1')");
        query("INSERT INTO " + tableName + " VALUES (row(4,5), 'b', 5, '2')");
        query("INSERT INTO " + tableName + " VALUES (row(6,7), 'c', 5, '2')");

        assertThat(query("ANALYZE " + tableName)).containsExactly(row(3));
        assertThat(query(showStatsTable)).containsOnly(
                row("c_row", null, null, null, null, null, null, null),
                row("c_char", 3.0, 2.0, 0.0, null, null, null, null),
                row("c_int", null, 1.0, 0.0, null, "3", "5", null),
                row("c_part", 3.0, 2.0, 0.0, null, null, null, null),
                row(null, null, null, null, 3.0, null, null, null));
    }

    @Test
    @Requires(NationPartitionedByBigintTable.class)
    public void testAnalyzeForTablePartitionedByBigint()
    {
        String tableNameInDatabase = mutableTablesState().get(NATION_PARTITIONED_BY_BIGINT_REGIONKEY.getName()).getNameInDatabase();

        String showStatsWholeTable = "SHOW STATS FOR " + tableNameInDatabase;
        String showStatsPartitionOne = "SHOW STATS FOR (SELECT * FROM " + tableNameInDatabase + " WHERE p_regionkey = 1)";
        String showStatsPartitionTwo = "SHOW STATS FOR (SELECT * FROM " + tableNameInDatabase + " WHERE p_regionkey = 2)";

        // table not analyzed

        assertThat(query(showStatsWholeTable)).containsOnly(
                row("p_nationkey", null, null, null, null, null, null, null),
                row("p_name", null, null, null, null, null, null, null),
                row("p_regionkey", null, null, null, null, null, null, null),
                row("p_comment", null, null, null, null, null, null, null),
                row(null, null, null, null, null, null, null, null));

        assertThat(query(showStatsPartitionOne)).containsOnly(
                row("p_nationkey", null, null, null, null, null, null, null),
                row("p_name", null, null, null, null, null, null, null),
                row("p_regionkey", null, null, null, null, null, null, null),
                row("p_comment", null, null, null, null, null, null, null),
                row(null, null, null, null, null, null, null, null));

        // analyze for single partition

        assertThat(query("ANALYZE " + tableNameInDatabase + " WITH (partitions = ARRAY[ARRAY['1']])")).containsExactly(row(5));

        assertThat(query(showStatsWholeTable)).containsOnly(
                row("p_nationkey", null, 5.0, 0.0, null, "1", "24", null),
                row("p_name", 114.0, 5.0, 0.0, null, null, null, null),
                row("p_regionkey", null, 3.0, 0.0, null, "1", "3", null),
                row("p_comment", 1497.0, 5.0, 0.0, null, null, null, null),
                row(null, null, null, null, 15.0, null, null, null));

        assertThat(query(showStatsPartitionOne)).containsOnly(
                row("p_nationkey", null, 5.0, 0.0, null, "1", "24", null),
                row("p_name", 38.0, 5.0, 0.0, null, null, null, null),
                row("p_regionkey", null, 1.0, 0.0, null, "1", "1", null),
                row("p_comment", 499.0, 5.0, 0.0, null, null, null, null),
                row(null, null, null, null, 5.0, null, null, null));

        assertThat(query(showStatsPartitionTwo)).containsOnly(
                row("p_nationkey", null, null, null, null, null, null, null),
                row("p_name", null, null, null, null, null, null, null),
                row("p_regionkey", null, null, null, null, null, null, null),
                row("p_comment", null, null, null, null, null, null, null),
                row(null, null, null, null, null, null, null, null));

        // analyze for all partitions

        assertThat(query("ANALYZE " + tableNameInDatabase)).containsExactly(row(15));

        assertThat(query(showStatsWholeTable)).containsOnly(
                row("p_nationkey", null, 5.0, 0.0, null, "1", "24", null),
                row("p_name", 109.0, 5.0, 0.0, null, null, null, null),
                row("p_regionkey", null, 3.0, 0.0, null, "1", "3", null),
                row("p_comment", 1197.0, 5.0, 0.0, null, null, null, null),
                row(null, null, null, null, 15.0, null, null, null));

        assertThat(query(showStatsPartitionOne)).containsOnly(
                row("p_nationkey", null, 5.0, 0.0, null, "1", "24", null),
                row("p_name", 38.0, 5.0, 0.0, null, null, null, null),
                row("p_regionkey", null, 1.0, 0.0, null, "1", "1", null),
                row("p_comment", 499.0, 5.0, 0.0, null, null, null, null),
                row(null, null, null, null, 5.0, null, null, null));

        assertThat(query(showStatsPartitionTwo)).containsOnly(
                row("p_nationkey", null, 5.0, 0.0, null, "8", "21", null),
                row("p_name", 31.0, 5.0, 0.0, null, null, null, null),
                row("p_regionkey", null, 1.0, 0.0, null, "2", "2", null),
                row("p_comment", 351.0, 5.0, 0.0, null, null, null, null),
                row(null, null, null, null, 5.0, null, null, null));
    }

    @Test
    @Requires(NationPartitionedByVarcharTable.class)
    public void testAnalyzeForTablePartitionedByVarchar()
    {
        String tableNameInDatabase = mutableTablesState().get(NATION_PARTITIONED_BY_VARCHAR_REGIONKEY.getName()).getNameInDatabase();

        String showStatsWholeTable = "SHOW STATS FOR " + tableNameInDatabase;
        String showStatsPartitionOne = "SHOW STATS FOR (SELECT * FROM " + tableNameInDatabase + " WHERE p_regionkey = 'AMERICA')";
        String showStatsPartitionTwo = "SHOW STATS FOR (SELECT * FROM " + tableNameInDatabase + " WHERE p_regionkey = 'ASIA')";

        // table not analyzed

        assertThat(query(showStatsWholeTable)).containsOnly(
                row("p_nationkey", null, null, null, null, null, null, null),
                row("p_name", null, null, null, null, null, null, null),
                row("p_regionkey", null, null, null, null, null, null, null),
                row("p_comment", null, null, null, null, null, null, null),
                row(null, null, null, null, null, null, null, null));

        assertThat(query(showStatsPartitionOne)).containsOnly(
                row("p_nationkey", null, null, null, null, null, null, null),
                row("p_name", null, null, null, null, null, null, null),
                row("p_regionkey", null, null, null, null, null, null, null),
                row("p_comment", null, null, null, null, null, null, null),
                row(null, null, null, null, null, null, null, null));

        // analyze for single partition

        assertThat(query("ANALYZE " + tableNameInDatabase + " WITH (partitions = ARRAY[ARRAY['AMERICA']])")).containsExactly(row(5));

        assertThat(query(showStatsWholeTable)).containsOnly(
                row("p_nationkey", null, 5.0, 0.0, null, "1", "24", null),
                row("p_name", 114.0, 5.0, 0.0, null, null, null, null),
                row("p_regionkey", 85.0, 3.0, 0.0, null, null, null, null),
                row("p_comment", 1497.0, 5.0, 0.0, null, null, null, null),
                row(null, null, null, null, 15.0, null, null, null));

        assertThat(query(showStatsPartitionOne)).containsOnly(
                row("p_nationkey", null, 5.0, 0.0, null, "1", "24", null),
                row("p_name", 38.0, 5.0, 0.0, null, null, null, null),
                row("p_regionkey", 35.0, 1.0, 0.0, null, null, null, null),
                row("p_comment", 499.0, 5.0, 0.0, null, null, null, null),
                row(null, null, null, null, 5.0, null, null, null));

        assertThat(query(showStatsPartitionTwo)).containsOnly(
                row("p_nationkey", null, null, null, null, null, null, null),
                row("p_name", null, null, null, null, null, null, null),
                row("p_regionkey", null, null, null, null, null, null, null),
                row("p_comment", null, null, null, null, null, null, null),
                row(null, null, null, null, null, null, null, null));

        // column analysis for all partitions

        assertThat(query("ANALYZE " + tableNameInDatabase)).containsExactly(row(15));

        assertThat(query(showStatsWholeTable)).containsOnly(
                row("p_nationkey", null, 5.0, 0.0, null, "1", "24", null),
                row("p_name", 109.0, 5.0, 0.0, null, null, null, null),
                row("p_regionkey", 85.0, 3.0, 0.0, null, null, null, null),
                row("p_comment", 1197.0, 5.0, 0.0, null, null, null, null),
                row(null, null, null, null, 15.0, null, null, null));

        assertThat(query(showStatsPartitionOne)).containsOnly(
                row("p_nationkey", null, 5.0, 0.0, null, "1", "24", null),
                row("p_name", 38.0, 5.0, 0.0, null, null, null, null),
                row("p_regionkey", 35.0, 1.0, 0.0, null, null, null, null),
                row("p_comment", 499.0, 5.0, 0.0, null, null, null, null),
                row(null, null, null, null, 5.0, null, null, null));

        assertThat(query(showStatsPartitionTwo)).containsOnly(
                row("p_nationkey", null, 5.0, 0.0, null, "8", "21", null),
                row("p_name", 31.0, 5.0, 0.0, null, null, null, null),
                row("p_regionkey", 20.0, 1.0, 0.0, null, null, null, null),
                row("p_comment", 351.0, 5.0, 0.0, null, null, null, null),
                row(null, null, null, null, 5.0, null, null, null));
    }

    // This covers also stats calculation for unpartitioned table
    @Test(groups = {SKIP_ON_CDH}) // skip on cdh due to no support for date column and stats
    @Requires(AllTypesTable.class)
    public void testAnalyzeForAllDataTypes()
    {
        String tableNameInDatabase = mutableTablesState().get(ALL_TYPES_TABLE_NAME).getNameInDatabase();

        assertThat(query("SHOW STATS FOR " + tableNameInDatabase)).containsOnly(
                row("c_tinyint", null, null, null, null, null, null, null),
                row("c_smallint", null, null, null, null, null, null, null),
                row("c_int", null, null, null, null, null, null, null),
                row("c_bigint", null, null, null, null, null, null, null),
                row("c_float", null, null, null, null, null, null, null),
                row("c_double", null, null, null, null, null, null, null),
                row("c_decimal", null, null, null, null, null, null, null),
                row("c_decimal_w_params", null, null, null, null, null, null, null),
                row("c_timestamp", null, null, null, null, null, null, null),
                row("c_date", null, null, null, null, null, null, null),
                row("c_string", null, null, null, null, null, null, null),
                row("c_varchar", null, null, null, null, null, null, null),
                row("c_char", null, null, null, null, null, null, null),
                row("c_boolean", null, null, null, null, null, null, null),
                row("c_binary", null, null, null, null, null, null, null),
                row(null, null, null, null, 0.0, null, null, null));

        assertThat(query("ANALYZE " + tableNameInDatabase)).containsExactly(row(2));

        // SHOW STATS FORMAT: column_name, data_size, distinct_values_count, nulls_fraction, row_count
        assertThat(query("SHOW STATS FOR " + tableNameInDatabase)).containsOnly(
                row("c_tinyint", null, 2.0, 0.0, null, "121", "127", null),
                row("c_smallint", null, 2.0, 0.0, null, "32761", "32767", null),
                row("c_int", null, 2.0, 0.0, null, "2147483641", "2147483647", null),
                row("c_bigint", null, 2.0, 0.0, null, "9223372036854775807", "9223372036854775807", null),
                row("c_float", null, 2.0, 0.0, null, "123.341", "123.345", null),
                row("c_double", null, 2.0, 0.0, null, "234.561", "235.567", null),
                row("c_decimal", null, 2.0, 0.0, null, "345.0", "346.0", null),
                row("c_decimal_w_params", null, 2.0, 0.0, null, "345.671", "345.678", null),
                row("c_timestamp", null, 2.0, 0.0, null, null, null, null),
                row("c_date", null, 2.0, 0.0, null, "2015-05-09", "2015-06-10", null),
                row("c_string", 22.0, 2.0, 0.0, null, null, null, null),
                row("c_varchar", 20.0, 2.0, 0.0, null, null, null, null),
                row("c_char", 12.0, 2.0, 0.0, null, null, null, null),
                row("c_boolean", null, 2.0, 0.0, null, null, null, null),
                row("c_binary", 23.0, null, 0.0, null, null, null, null),
                row(null, null, null, null, 2.0, null, null, null));
    }

    @Test(groups = {SKIP_ON_CDH}) // skip on cdh due to no support for date column and stats
    @Requires(AllTypesTable.class)
    public void testAnalyzeForAllDataTypesNoData()
    {
        String tableNameInDatabase = mutableTablesState().get(EMPTY_ALL_TYPES_TABLE_NAME).getNameInDatabase();

        assertThat(query("SHOW STATS FOR " + tableNameInDatabase)).containsOnly(
                row("c_tinyint", null, null, null, null, null, null, null),
                row("c_smallint", null, null, null, null, null, null, null),
                row("c_int", null, null, null, null, null, null, null),
                row("c_bigint", null, null, null, null, null, null, null),
                row("c_float", null, null, null, null, null, null, null),
                row("c_double", null, null, null, null, null, null, null),
                row("c_decimal", null, null, null, null, null, null, null),
                row("c_decimal_w_params", null, null, null, null, null, null, null),
                row("c_timestamp", null, null, null, null, null, null, null),
                row("c_date", null, null, null, null, null, null, null),
                row("c_string", null, null, null, null, null, null, null),
                row("c_varchar", null, null, null, null, null, null, null),
                row("c_char", null, null, null, null, null, null, null),
                row("c_boolean", null, null, null, null, null, null, null),
                row("c_binary", null, null, null, null, null, null, null),
                row(null, null, null, null, 0.0, null, null, null));

        assertThat(query("ANALYZE " + tableNameInDatabase)).containsExactly(row(0));

        assertThat(query("SHOW STATS FOR " + tableNameInDatabase)).containsOnly(
                row("c_tinyint", null, 0.0, 0.0, null, null, null, null),
                row("c_smallint", null, 0.0, 0.0, null, null, null, null),
                row("c_int", null, 0.0, 0.0, null, null, null, null),
                row("c_bigint", null, 0.0, 0.0, null, null, null, null),
                row("c_float", null, 0.0, 0.0, null, null, null, null),
                row("c_double", null, 0.0, 0.0, null, null, null, null),
                row("c_decimal", null, 0.0, 0.0, null, null, null, null),
                row("c_decimal_w_params", null, 0.0, 0.0, null, null, null, null),
                row("c_timestamp", null, 0.0, 0.0, null, null, null, null),
                row("c_date", null, 0.0, 0.0, null, null, null, null),
                row("c_string", 0.0, 0.0, 0.0, null, null, null, null),
                row("c_varchar", 0.0, 0.0, 0.0, null, null, null, null),
                row("c_char", 0.0, 0.0, 0.0, null, null, null, null),
                row("c_boolean", null, 0.0, 0.0, null, null, null, null),
                row("c_binary", 0.0, null, 0.0, null, null, null, null),
                row(null, null, null, null, 0.0, null, null, null));
    }

    @Test(groups = {SKIP_ON_CDH}) // skip on cdh due to no support for date column and stats
    @Requires(AllTypesTable.class)
    public void testAnalyzeForAllDataTypesOnlyNulls()
    {
        String tableNameInDatabase = mutableTablesState().get(EMPTY_ALL_TYPES_TABLE_NAME).getNameInDatabase();

        // insert from hive to prevent Presto collecting statistics on insert
        onHive().executeQuery("INSERT INTO TABLE " + tableNameInDatabase + " VALUES(null, null, null, null, null, null, null, null, null, null, null, null, null, null, null)");

        assertThat(query("SHOW STATS FOR " + tableNameInDatabase)).containsOnly(
                row("c_tinyint", null, 0.0, 1.0, null, null, null, null),
                row("c_smallint", null, 0.0, 1.0, null, null, null, null),
                row("c_int", null, 0.0, 1.0, null, null, null, null),
                row("c_bigint", null, 0.0, 1.0, null, null, null, null),
                row("c_float", null, 0.0, 1.0, null, null, null, null),
                row("c_double", null, 0.0, 1.0, null, null, null, null),
                row("c_decimal", null, 0.0, 1.0, null, null, null, null),
                row("c_decimal_w_params", null, 0.0, 1.0, null, null, null, null),
                row("c_timestamp", null, 0.0, 1.0, null, null, null, null),
                row("c_date", null, 0.0, 1.0, null, null, null, null),
                row("c_string", 0.0, 0.0, 1.0, null, null, null, null),
                row("c_varchar", 0.0, 0.0, 1.0, null, null, null, null),
                row("c_char", 0.0, 0.0, 1.0, null, null, null, null),
                row("c_boolean", null, 0.0, 1.0, null, null, null, null),
                row("c_binary", 0.0, null, 1.0, null, null, null, null),
                row(null, null, null, null, 1.0, null, null, null));
    }

    @Test
    @Requires(AllTypesTable.class)
    public void testComputeTableStatisticsOnCreateTable()
    {
        String allTypesTable = mutableTablesState().get(ALL_TYPES_TABLE_NAME).getNameInDatabase();
        String emptyAllTypesTable = mutableTablesState().get(EMPTY_ALL_TYPES_TABLE_NAME).getNameInDatabase();
        String allTypesAllNullTable = mutableTablesState().get(ALL_TYPES_ALL_NULL_TABLE_NAME).getNameInDatabase();

        assertComputeTableStatisticsOnCreateTable(allTypesTable, ALL_TYPES_TABLE_STATISTICS);
        assertComputeTableStatisticsOnCreateTable(emptyAllTypesTable, ALL_TYPES_EMPTY_TABLE_STATISTICS);
        assertComputeTableStatisticsOnCreateTable(allTypesAllNullTable, ALL_TYPES_ALL_NULL_TABLE_STATISTICS);
    }

    @Test
    @Requires(AllTypesTable.class)
    public void testComputeTableStatisticsOnInsert()
    {
        String allTypesTable = mutableTablesState().get(ALL_TYPES_TABLE_NAME).getNameInDatabase();
        String emptyAllTypesTable = mutableTablesState().get(EMPTY_ALL_TYPES_TABLE_NAME).getNameInDatabase();
        String allTypesAllNullTable = mutableTablesState().get(ALL_TYPES_ALL_NULL_TABLE_NAME).getNameInDatabase();

        assertComputeTableStatisticsOnInsert(allTypesTable, ALL_TYPES_TABLE_STATISTICS);
        assertComputeTableStatisticsOnInsert(emptyAllTypesTable, ALL_TYPES_EMPTY_TABLE_STATISTICS);
        assertComputeTableStatisticsOnInsert(allTypesAllNullTable, ALL_TYPES_ALL_NULL_TABLE_STATISTICS);

        String tableName = "test_update_table_statistics";
        query(format("DROP TABLE IF EXISTS %s", tableName));
        try {
            query(format("CREATE TABLE %s AS SELECT * FROM %s WITH NO DATA", tableName, allTypesTable));
            query(format("INSERT INTO %s SELECT * FROM %s", tableName, allTypesTable));
            query(format("INSERT INTO %s SELECT * FROM %s", tableName, allTypesAllNullTable));
            query(format("INSERT INTO %s SELECT * FROM %s", tableName, allTypesAllNullTable));
            assertThat(query("SHOW STATS FOR " + tableName)).containsOnly(
                    row("c_tinyint", null, 2.0, 0.5, null, "121", "127", null),
                    row("c_smallint", null, 2.0, 0.5, null, "32761", "32767", null),
                    row("c_int", null, 2.0, 0.5, null, "2147483641", "2147483647", null),
                    row("c_bigint", null, 2.0, 0.5, null, "9223372036854775807", "9223372036854775807", null),
                    row("c_float", null, 2.0, 0.5, null, "123.341", "123.345", null),
                    row("c_double", null, 2.0, 0.5, null, "234.561", "235.567", null),
                    row("c_decimal", null, 2.0, 0.5, null, "345.0", "346.0", null),
                    row("c_decimal_w_params", null, 2.0, 0.5, null, "345.671", "345.678", null),
                    row("c_timestamp", null, 2.0, 0.5, null, null, null, null),
                    row("c_date", null, 2.0, 0.5, null, "2015-05-09", "2015-06-10", null),
                    row("c_string", 22.0, 2.0, 0.5, null, null, null, null),
                    row("c_varchar", 20.0, 2.0, 0.5, null, null, null, null),
                    row("c_char", 12.0, 2.0, 0.5, null, null, null, null),
                    row("c_boolean", null, 2.0, 0.5, null, null, null, null),
                    row("c_binary", 23.0, null, 0.5, null, null, null, null),
                    row(null, null, null, null, 4.0, null, null, null));

            query(format("INSERT INTO %s VALUES( " +
                    "TINYINT '120', " +
                    "SMALLINT '32760', " +
                    "INTEGER '2147483640', " +
                    "BIGINT '9223372036854775800', " +
                    "REAL '123.340', " +
                    "DOUBLE '234.560', " +
                    "CAST(343.0 AS DECIMAL(10, 0)), " +
                    "CAST(345.670 AS DECIMAL(10, 5)), " +
                    "TIMESTAMP '2015-05-10 12:15:30', " +
                    "DATE '2015-05-08', " +
                    "CAST('ela ma kot' AS VARCHAR), " +
                    "CAST('ela ma ko' AS VARCHAR(10)), " +
                    "CAST('ela m     ' AS CHAR(10)), " +
                    "false, " +
                    "CAST('cGllcyBiaW5hcm54' as VARBINARY))", tableName));

            assertThat(query("SHOW STATS FOR " + tableName)).containsOnly(ImmutableList.of(
                    row("c_tinyint", null, 2.0, 0.4, null, "120", "127", null),
                    row("c_smallint", null, 2.0, 0.4, null, "32760", "32767", null),
                    row("c_int", null, 2.0, 0.4, null, "2147483640", "2147483647", null),
                    row("c_bigint", null, 2.0, 0.4, null, "9223372036854775807", "9223372036854775807", null),
                    row("c_float", null, 2.0, 0.4, null, "123.34", "123.345", null),
                    row("c_double", null, 2.0, 0.4, null, "234.56", "235.567", null),
                    row("c_decimal", null, 2.0, 0.4, null, "343.0", "346.0", null),
                    row("c_decimal_w_params", null, 2.0, 0.4, null, "345.67", "345.678", null),
                    row("c_timestamp", null, 2.0, 0.4, null, null, null, null),
                    row("c_date", null, 2.0, 0.4, null, "2015-05-08", "2015-06-10", null),
                    row("c_string", 32.0, 2.0, 0.4, null, null, null, null),
                    row("c_varchar", 29.0, 2.0, 0.4, null, null, null, null),
                    row("c_char", 17.0, 2.0, 0.4, null, null, null, null),
                    row("c_boolean", null, 2.0, 0.4, null, null, null, null),
                    row("c_binary", 39.0, null, 0.4, null, null, null, null),
                    row(null, null, null, null, 5.0, null, null, null)));
        }
        finally {
            query(format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    @Test
    @Requires(AllTypesTable.class)
    public void testComputePartitionStatisticsOnCreateTable()
    {
        String tableName = "test_compute_partition_statistics_on_create_table";
        query(format("DROP TABLE IF EXISTS %s", tableName));
        try {
            query(format("CREATE TABLE %s WITH ( " +
                    " partitioned_by = ARRAY['p_bigint', 'p_varchar']" +
                    ") AS " +
                    "SELECT * FROM ( " +
                    "    VALUES " +
                    "        (" +
                    "           TINYINT '120', " +
                    "           SMALLINT '32760', " +
                    "           INTEGER '2147483640', " +
                    "           BIGINT '9223372036854775800', " +
                    "           REAL '123.340', DOUBLE '234.560', " +
                    "           CAST(343.0 AS DECIMAL(10, 0)), " +
                    "           CAST(345.670 AS DECIMAL(10, 5)), " +
                    "           TIMESTAMP '2015-05-10 12:15:30', " +
                    "           DATE '2015-05-08', " +
                    "           CAST('p1 varchar' AS VARCHAR), " +
                    "           CAST('p1 varchar10' AS VARCHAR(10)), " +
                    "           CAST('p1 char10' AS CHAR(10)), false, " +
                    "           CAST('p1 binary' as VARBINARY), " +
                    "           BIGINT '1', " +
                    "           CAST('partition1' AS VARCHAR)" +
                    "        ), " +
                    "        (null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, BIGINT '1', 'partition1'), " +
                    "        (" +
                    "           TINYINT '99', " +
                    "           SMALLINT '333'," +
                    "           INTEGER '444', " +
                    "           BIGINT '555', " +
                    "           REAL '666.340', " +
                    "           DOUBLE '777.560', " +
                    "           CAST(888.0 AS DECIMAL(10, 0)), " +
                    "           CAST(999.670 AS DECIMAL(10, 5)), " +
                    "           TIMESTAMP '2015-05-10 12:45:30', " +
                    "           DATE '2015-05-09', " +
                    "           CAST('p2 varchar' AS VARCHAR), " +
                    "           CAST('p2 varchar10' AS VARCHAR(10)), " +
                    "           CAST('p2 char10' AS CHAR(10)), " +
                    "           true, " +
                    "           CAST('p2 binary' as VARBINARY), " +
                    "           BIGINT '2', " +
                    "           CAST('partition2' AS VARCHAR)" +
                    "        ), " +
                    "        (null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, BIGINT '2', 'partition2') " +
                    "" +
                    ") AS t (c_tinyint, c_smallint, c_int, c_bigint, c_float, c_double, c_decimal, c_decimal_w_params, c_timestamp, c_date, c_string, c_varchar, c_char, c_boolean, c_binary, p_bigint, p_varchar)", tableName));

            assertThat(query(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_bigint = 1 AND p_varchar = 'partition1')", tableName))).containsOnly(ImmutableList.of(
                    row("c_tinyint", null, 1.0, 0.5, null, "120", "120", null),
                    row("c_smallint", null, 1.0, 0.5, null, "32760", "32760", null),
                    row("c_int", null, 1.0, 0.5, null, "2147483640", "2147483640", null),
                    row("c_bigint", null, 1.0, 0.5, null, "9223372036854775807", "9223372036854775807", null),
                    row("c_float", null, 1.0, 0.5, null, "123.34", "123.34", null),
                    row("c_double", null, 1.0, 0.5, null, "234.56", "234.56", null),
                    row("c_decimal", null, 1.0, 0.5, null, "343.0", "343.0", null),
                    row("c_decimal_w_params", null, 1.0, 0.5, null, "345.67", "345.67", null),
                    row("c_timestamp", null, 1.0, 0.5, null, null, null, null),
                    row("c_date", null, 1.0, 0.5, null, "2015-05-08", "2015-05-08", null),
                    row("c_string", 10.0, 1.0, 0.5, null, null, null, null),
                    row("c_varchar", 10.0, 1.0, 0.5, null, null, null, null),
                    row("c_char", 9.0, 1.0, 0.5, null, null, null, null),
                    row("c_boolean", null, 1.0, 0.5, null, null, null, null),
                    row("c_binary", 9.0, null, 0.5, null, null, null, null),
                    row("p_bigint", null, 1.0, 0.0, null, "1", "1", null),
                    row("p_varchar", 20.0, 1.0, 0.0, null, null, null, null),
                    row(null, null, null, null, 2.0, null, null, null)));

            assertThat(query(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_bigint = 2 AND p_varchar = 'partition2')", tableName))).containsOnly(ImmutableList.of(
                    row("c_tinyint", null, 1.0, 0.5, null, "99", "99", null),
                    row("c_smallint", null, 1.0, 0.5, null, "333", "333", null),
                    row("c_int", null, 1.0, 0.5, null, "444", "444", null),
                    row("c_bigint", null, 1.0, 0.5, null, "555", "555", null),
                    row("c_float", null, 1.0, 0.5, null, "666.34", "666.34", null),
                    row("c_double", null, 1.0, 0.5, null, "777.56", "777.56", null),
                    row("c_decimal", null, 1.0, 0.5, null, "888.0", "888.0", null),
                    row("c_decimal_w_params", null, 1.0, 0.5, null, "999.67", "999.67", null),
                    row("c_timestamp", null, 1.0, 0.5, null, null, null, null),
                    row("c_date", null, 1.0, 0.5, null, "2015-05-09", "2015-05-09", null),
                    row("c_string", 10.0, 1.0, 0.5, null, null, null, null),
                    row("c_varchar", 10.0, 1.0, 0.5, null, null, null, null),
                    row("c_char", 9.0, 1.0, 0.5, null, null, null, null),
                    row("c_boolean", null, 1.0, 0.5, null, null, null, null),
                    row("c_binary", 9.0, null, 0.5, null, null, null, null),
                    row("p_bigint", null, 1.0, 0.0, null, "2", "2", null),
                    row("p_varchar", 20.0, 1.0, 0.0, null, null, null, null),
                    row(null, null, null, null, 2.0, null, null, null)));
        }
        finally {
            query(format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    @Test
    @Requires(AllTypesTable.class)
    public void testComputePartitionStatisticsOnInsert()
    {
        String tableName = "test_compute_partition_statistics_on_insert";

        query(format("DROP TABLE IF EXISTS %s", tableName));
        try {
            query(format("CREATE TABLE %s(" +
                    "c_tinyint            TINYINT, " +
                    "c_smallint           SMALLINT, " +
                    "c_int                INT, " +
                    "c_bigint             BIGINT, " +
                    "c_float              REAL, " +
                    "c_double             DOUBLE, " +
                    "c_decimal            DECIMAL(10,0), " +
                    "c_decimal_w_params   DECIMAL(10,5), " +
                    "c_timestamp          TIMESTAMP, " +
                    "c_date               DATE, " +
                    "c_string             VARCHAR, " +
                    "c_varchar            VARCHAR(10), " +
                    "c_char               CHAR(10), " +
                    "c_boolean            BOOLEAN, " +
                    "c_binary             VARBINARY, " +
                    "p_bigint             BIGINT, " +
                    "p_varchar            VARCHAR " +
                    ") WITH ( " +
                    " partitioned_by = ARRAY['p_bigint', 'p_varchar']" +
                    ")", tableName));

            query(format("INSERT INTO %s VALUES " +
                    "(TINYINT '120', SMALLINT '32760', INTEGER '2147483640', BIGINT '9223372036854775800', REAL '123.340', DOUBLE '234.560', CAST(343.0 AS DECIMAL(10, 0)), CAST(345.670 AS DECIMAL(10, 5)), TIMESTAMP '2015-05-10 12:15:30', DATE '2015-05-08', 'p1 varchar', CAST('p1 varchar10' AS VARCHAR(10)), CAST('p1 char10' AS CHAR(10)), false, CAST('p1 binary' as VARBINARY), BIGINT '1', 'partition1')," +
                    "(null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, BIGINT '1', 'partition1')", tableName));

            query(format("INSERT INTO %s VALUES " +
                    "(TINYINT '99', SMALLINT '333', INTEGER '444', BIGINT '555', REAL '666.340', DOUBLE '777.560', CAST(888.0 AS DECIMAL(10, 0)), CAST(999.670 AS DECIMAL(10, 5)), TIMESTAMP '2015-05-10 12:45:30', DATE '2015-05-09', 'p2 varchar', CAST('p2 varchar10' AS VARCHAR(10)), CAST('p2 char10' AS CHAR(10)), true, CAST('p2 binary' as VARBINARY), BIGINT '2', 'partition2')," +
                    "(null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, BIGINT '2', 'partition2')", tableName));

            String showStatsPartitionOne = format("SHOW STATS FOR (SELECT * FROM %s WHERE p_bigint = 1 AND p_varchar = 'partition1')", tableName);
            String showStatsPartitionTwo = format("SHOW STATS FOR (SELECT * FROM %s WHERE p_bigint = 2 AND p_varchar = 'partition2')", tableName);

            assertThat(query(showStatsPartitionOne)).containsOnly(ImmutableList.of(
                    row("c_tinyint", null, 1.0, 0.5, null, "120", "120", null),
                    row("c_smallint", null, 1.0, 0.5, null, "32760", "32760", null),
                    row("c_int", null, 1.0, 0.5, null, "2147483640", "2147483640", null),
                    row("c_bigint", null, 1.0, 0.5, null, "9223372036854775807", "9223372036854775807", null),
                    row("c_float", null, 1.0, 0.5, null, "123.34", "123.34", null),
                    row("c_double", null, 1.0, 0.5, null, "234.56", "234.56", null),
                    row("c_decimal", null, 1.0, 0.5, null, "343.0", "343.0", null),
                    row("c_decimal_w_params", null, 1.0, 0.5, null, "345.67", "345.67", null),
                    row("c_timestamp", null, 1.0, 0.5, null, null, null, null),
                    row("c_date", null, 1.0, 0.5, null, "2015-05-08", "2015-05-08", null),
                    row("c_string", 10.0, 1.0, 0.5, null, null, null, null),
                    row("c_varchar", 10.0, 1.0, 0.5, null, null, null, null),
                    row("c_char", 9.0, 1.0, 0.5, null, null, null, null),
                    row("c_boolean", null, 1.0, 0.5, null, null, null, null),
                    row("c_binary", 9.0, null, 0.5, null, null, null, null),
                    row("p_bigint", null, 1.0, 0.0, null, "1", "1", null),
                    row("p_varchar", 20.0, 1.0, 0.0, null, null, null, null),
                    row(null, null, null, null, 2.0, null, null, null)));

            assertThat(query(showStatsPartitionTwo)).containsOnly(ImmutableList.of(
                    row("c_tinyint", null, 1.0, 0.5, null, "99", "99", null),
                    row("c_smallint", null, 1.0, 0.5, null, "333", "333", null),
                    row("c_int", null, 1.0, 0.5, null, "444", "444", null),
                    row("c_bigint", null, 1.0, 0.5, null, "555", "555", null),
                    row("c_float", null, 1.0, 0.5, null, "666.34", "666.34", null),
                    row("c_double", null, 1.0, 0.5, null, "777.56", "777.56", null),
                    row("c_decimal", null, 1.0, 0.5, null, "888.0", "888.0", null),
                    row("c_decimal_w_params", null, 1.0, 0.5, null, "999.67", "999.67", null),
                    row("c_timestamp", null, 1.0, 0.5, null, null, null, null),
                    row("c_date", null, 1.0, 0.5, null, "2015-05-09", "2015-05-09", null),
                    row("c_string", 10.0, 1.0, 0.5, null, null, null, null),
                    row("c_varchar", 10.0, 1.0, 0.5, null, null, null, null),
                    row("c_char", 9.0, 1.0, 0.5, null, null, null, null),
                    row("c_boolean", null, 1.0, 0.5, null, null, null, null),
                    row("c_binary", 9.0, null, 0.5, null, null, null, null),
                    row("p_bigint", null, 1.0, 0.0, null, "2", "2", null),
                    row("p_varchar", 20.0, 1.0, 0.0, null, null, null, null),
                    row(null, null, null, null, 2.0, null, null, null)));

            query(format("INSERT INTO %s VALUES( TINYINT '119', SMALLINT '32759', INTEGER '2147483639', BIGINT '9223372036854775799', REAL '122.340', DOUBLE '233.560', CAST(342.0 AS DECIMAL(10, 0)), CAST(344.670 AS DECIMAL(10, 5)), TIMESTAMP '2015-05-10 12:15:29', DATE '2015-05-07', 'p1 varchar', CAST('p1 varchar10' AS VARCHAR(10)), CAST('p1 char10' AS CHAR(10)), true, CAST('p1 binary' as VARBINARY), BIGINT '1', 'partition1')", tableName));
            query(format("INSERT INTO %s VALUES( null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, BIGINT '1', 'partition1')", tableName));

            assertThat(query(showStatsPartitionOne)).containsOnly(ImmutableList.of(
                    row("c_tinyint", null, 1.0, 0.5, null, "119", "120", null),
                    row("c_smallint", null, 1.0, 0.5, null, "32759", "32760", null),
                    row("c_int", null, 1.0, 0.5, null, "2147483639", "2147483640", null),
                    row("c_bigint", null, 1.0, 0.5, null, "9223372036854775807", "9223372036854775807", null),
                    row("c_float", null, 1.0, 0.5, null, "122.34", "123.34", null),
                    row("c_double", null, 1.0, 0.5, null, "233.56", "234.56", null),
                    row("c_decimal", null, 1.0, 0.5, null, "342.0", "343.0", null),
                    row("c_decimal_w_params", null, 1.0, 0.5, null, "344.67", "345.67", null),
                    row("c_timestamp", null, 1.0, 0.5, null, null, null, null),
                    row("c_date", null, 1.0, 0.5, null, "2015-05-07", "2015-05-08", null),
                    row("c_string", 20.0, 1.0, 0.5, null, null, null, null),
                    row("c_varchar", 20.0, 1.0, 0.5, null, null, null, null),
                    row("c_char", 18.0, 1.0, 0.5, null, null, null, null),
                    row("c_boolean", null, 2.0, 0.5, null, null, null, null),
                    row("c_binary", 18.0, null, 0.5, null, null, null, null),
                    row("p_bigint", null, 1.0, 0.0, null, "1", "1", null),
                    row("p_varchar", 40.0, 1.0, 0.0, null, null, null, null),
                    row(null, null, null, null, 4.0, null, null, null)));

            query(format("INSERT INTO %s VALUES( TINYINT '100', SMALLINT '334', INTEGER '445', BIGINT '556', REAL '667.340', DOUBLE '778.560', CAST(889.0 AS DECIMAL(10, 0)), CAST(1000.670 AS DECIMAL(10, 5)), TIMESTAMP '2015-05-10 12:45:31', DATE '2015-05-10', CAST('p2 varchar' AS VARCHAR), CAST('p2 varchar10' AS VARCHAR(10)), CAST('p2 char10' AS CHAR(10)), true, CAST('p2 binary' as VARBINARY), BIGINT '2', 'partition2')", tableName));
            query(format("INSERT INTO %s VALUES( null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, BIGINT '2', 'partition2')", tableName));

            assertThat(query(showStatsPartitionTwo)).containsOnly(ImmutableList.of(
                    row("c_tinyint", null, 1.0, 0.5, null, "99", "100", null),
                    row("c_smallint", null, 1.0, 0.5, null, "333", "334", null),
                    row("c_int", null, 1.0, 0.5, null, "444", "445", null),
                    row("c_bigint", null, 1.0, 0.5, null, "555", "556", null),
                    row("c_float", null, 1.0, 0.5, null, "666.34", "667.34", null),
                    row("c_double", null, 1.0, 0.5, null, "777.56", "778.56", null),
                    row("c_decimal", null, 1.0, 0.5, null, "888.0", "889.0", null),
                    row("c_decimal_w_params", null, 1.0, 0.5, null, "999.67", "1000.67", null),
                    row("c_timestamp", null, 1.0, 0.5, null, null, null, null),
                    row("c_date", null, 1.0, 0.5, null, "2015-05-09", "2015-05-10", null),
                    row("c_string", 20.0, 1.0, 0.5, null, null, null, null),
                    row("c_varchar", 20.0, 1.0, 0.5, null, null, null, null),
                    row("c_char", 18.0, 1.0, 0.5, null, null, null, null),
                    row("c_boolean", null, 1.0, 0.5, null, null, null, null),
                    row("c_binary", 18.0, null, 0.5, null, null, null, null),
                    row("p_bigint", null, 1.0, 0.0, null, "2", "2", null),
                    row("p_varchar", 40.0, 1.0, 0.0, null, null, null, null),
                    row(null, null, null, null, 4.0, null, null, null)));
        }
        finally {
            query(format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    private static void assertComputeTableStatisticsOnCreateTable(String sourceTableName, List<Row> expectedStatistics)
    {
        String copiedTableName = "assert_compute_table_statistics_on_create_table_" + sourceTableName;
        query(format("DROP TABLE IF EXISTS %s", copiedTableName));
        try {
            query(format("CREATE TABLE %s AS SELECT * FROM %s", copiedTableName, sourceTableName));
            assertThat(query("SHOW STATS FOR " + copiedTableName)).containsOnly(expectedStatistics);
        }
        finally {
            query(format("DROP TABLE IF EXISTS %s", copiedTableName));
        }
    }

    private static void assertComputeTableStatisticsOnInsert(String sourceTableName, List<Row> expectedStatistics)
    {
        String copiedTableName = "assert_compute_table_statistics_on_insert_" + sourceTableName;
        query(format("DROP TABLE IF EXISTS %s", copiedTableName));
        try {
            query(format("CREATE TABLE %s AS SELECT * FROM %s WITH NO DATA", copiedTableName, sourceTableName));
            assertThat(query("SHOW STATS FOR " + copiedTableName)).containsOnly(ALL_TYPES_EMPTY_TABLE_STATISTICS);
            query(format("INSERT INTO %s SELECT * FROM %s", copiedTableName, sourceTableName));
            assertThat(query("SHOW STATS FOR " + copiedTableName)).containsOnly(expectedStatistics);
        }
        finally {
            query(format("DROP TABLE IF EXISTS %s", copiedTableName));
        }
    }
}
