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

import io.prestodb.tempto.ProductTest;
import io.prestodb.tempto.Requirement;
import io.prestodb.tempto.Requirements;
import io.prestodb.tempto.RequirementsProvider;
import io.prestodb.tempto.Requires;
import io.prestodb.tempto.configuration.Configuration;
import io.prestodb.tempto.fulfillment.table.MutableTableRequirement;
import io.prestodb.tempto.fulfillment.table.hive.HiveTableDefinition;
import io.prestodb.tempto.fulfillment.table.hive.InlineDataSource;
import io.prestodb.tempto.query.QueryExecutor;
import org.testng.annotations.Test;

import static com.facebook.presto.tests.TestGroups.HIVE_CONNECTOR;
import static com.facebook.presto.tests.TestGroups.SKIP_ON_CDH;
import static com.facebook.presto.tests.hive.AllSimpleTypesTableDefinitions.ALL_HIVE_SIMPLE_TYPES_TEXTFILE;
import static com.facebook.presto.tests.hive.HiveTableDefinitions.NATION_PARTITIONED_BY_REGIONKEY;
import static io.prestodb.tempto.assertions.QueryAssert.Row.row;
import static io.prestodb.tempto.assertions.QueryAssert.anyOf;
import static io.prestodb.tempto.assertions.QueryAssert.assertThat;
import static io.prestodb.tempto.context.ThreadLocalTestContextHolder.testContext;
import static io.prestodb.tempto.fulfillment.table.MutableTablesState.mutableTablesState;
import static io.prestodb.tempto.fulfillment.table.TableRequirements.mutableTable;
import static io.prestodb.tempto.fulfillment.table.hive.tpch.TpchTableDefinitions.NATION;
import static io.prestodb.tempto.query.QueryExecutor.query;

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

    private static class PartitionedNationTable
            implements RequirementsProvider
    {
        @Override
        public Requirement getRequirements(Configuration configuration)
        {
            return mutableTable(
                    HiveTableDefinition.from(NATION_PARTITIONED_BY_REGIONKEY)
                            .injectStats(false)
                            .build());
        }
    }

    private static final String ALL_TYPES_TABLE_NAME = "all_types";
    private static final String EMPTY_ALL_TYPES_TABLE_NAME = "empty_all_types";

    private static final HiveTableDefinition ALL_TYPES_TABLE = HiveTableDefinition.like(ALL_HIVE_SIMPLE_TYPES_TEXTFILE)
            .setDataSource(InlineDataSource.createStringDataSource(
                    "all_analyzable_types",
                    "",
                    "121|32761|2147483641|9223372036854775801|123.341|234.561|344.671|345.671|2015-05-10 12:15:31.123456|2015-05-09|ela ma kota|ela ma kot|ela ma    |false|cGllcyBiaW5hcm55|\n" +
                            "127|32767|2147483647|9223372036854775807|123.345|235.567|345.678|345.678|2015-05-10 12:15:35.123456|2015-06-10|ala ma kota|ala ma kot|ala ma    |true|a290IGJpbmFybnk=|\n"))
            .build();

    private static final class AllTypesTable
            implements RequirementsProvider
    {
        @Override
        public Requirement getRequirements(Configuration configuration)
        {
            return Requirements.compose(
                    mutableTable(ALL_TYPES_TABLE, ALL_TYPES_TABLE_NAME, MutableTableRequirement.State.LOADED),
                    mutableTable(ALL_TYPES_TABLE, EMPTY_ALL_TYPES_TABLE_NAME, MutableTableRequirement.State.CREATED));
        }
    }

    @Test(groups = {HIVE_CONNECTOR})
    @Requires(UnpartitionedNationTable.class)
    public void testStatisticsForUnpartitionedTable()
    {
        String tableNameInDatabase = mutableTablesState().get(NATION.getName()).getNameInDatabase();

        String showStatsWholeTable = "SHOW STATS FOR " + tableNameInDatabase;

        // table not analyzed

        assertThat(query(showStatsWholeTable)).containsOnly(
                row("n_nationkey", null, null, null, null, null, null),
                row("n_name", null, null, null, null, null, null),
                row("n_regionkey", null, null, null, null, null, null),
                row("n_comment", null, null, null, null, null, null),
                row(null, null, null, null, anyOf(null, 0.0), null, null)); // anyOf because of different behaviour on HDP (hive 1.2) and CDH (hive 1.1)

        // basic analysis

        onHive().executeQuery("ANALYZE TABLE " + tableNameInDatabase + " COMPUTE STATISTICS");

        assertThat(query(showStatsWholeTable)).containsOnly(
                row("n_nationkey", null, null, null, null, null, null),
                row("n_name", null, null, null, null, null, null),
                row("n_regionkey", null, null, null, null, null, null),
                row("n_comment", null, null, null, null, null, null),
                row(null, null, null, null, 25.0, null, null));

        // column analysis

        onHive().executeQuery("ANALYZE TABLE " + tableNameInDatabase + " COMPUTE STATISTICS FOR COLUMNS");

        assertThat(query(showStatsWholeTable)).containsOnly(
                row("n_nationkey", null, 19.0, 0.0, null, "0", "24"),
                row("n_name", null, 24.0, 0.0, null, null, null),
                row("n_regionkey", null, 5.0, 0.0, null, "0", "4"),
                row("n_comment", null, 31.0, 0.0, null, null, null),
                row(null, null, null, null, 25.0, null, null));
    }

    @Test(groups = {HIVE_CONNECTOR})
    @Requires(PartitionedNationTable.class)
    public void testStatisticsForPartitionedTable()
    {
        String tableNameInDatabase = mutableTablesState().get(NATION_PARTITIONED_BY_REGIONKEY.getName()).getNameInDatabase();

        String showStatsWholeTable = "SHOW STATS FOR " + tableNameInDatabase;
        String showStatsPartitionOne = "SHOW STATS FOR (SELECT * FROM " + tableNameInDatabase + " WHERE p_regionkey = 1)";
        String showStatsPartitionTwo = "SHOW STATS FOR (SELECT * FROM " + tableNameInDatabase + " WHERE p_regionkey = 2)";

        // table not analyzed

        assertThat(query(showStatsWholeTable)).containsOnly(
                row("p_nationkey", null, null, null, null, null, null),
                row("p_name", null, null, null, null, null, null),
                row("p_regionkey", null, 3.0, null, null, "1", "3"),
                row("p_comment", null, null, null, null, null, null),
                row(null, null, null, null, null, null, null));

        assertThat(query(showStatsPartitionOne)).containsOnly(
                row("p_nationkey", null, null, null, null, null, null),
                row("p_name", null, null, null, null, null, null),
                row("p_regionkey", null, 1.0, null, null, "1", "1"),
                row("p_comment", null, null, null, null, null, null),
                row(null, null, null, null, null, null, null));

        // basic analysis for single partition

        onHive().executeQuery("ANALYZE TABLE " + tableNameInDatabase + " PARTITION (p_regionkey = \"1\") COMPUTE STATISTICS");

        assertThat(query(showStatsWholeTable)).containsOnly(
                row("p_nationkey", null, null, null, null, null, null),
                row("p_name", null, null, null, null, null, null),
                row("p_regionkey", null, 3.0, 0.0, null, "1", "3"),
                row("p_comment", null, null, null, null, null, null),
                row(null, null, null, null, 15.0, null, null));

        assertThat(query(showStatsPartitionOne)).containsOnly(
                row("p_nationkey", null, null, null, null, null, null),
                row("p_name", null, null, null, null, null, null),
                row("p_regionkey", null, 1.0, 0.0, null, "1", "1"),
                row("p_comment", null, null, null, null, null, null),
                row(null, null, null, null, 5.0, null, null));

        assertThat(query(showStatsPartitionTwo)).containsOnly(
                row("p_nationkey", null, null, null, null, null, null),
                row("p_name", null, null, null, null, null, null),
                row("p_regionkey", null, 1.0, null, null, "2", "2"),
                row("p_comment", null, null, null, null, null, null),
                row(null, null, null, null, null, null, null));

        // basic analysis for all partitions

        onHive().executeQuery("ANALYZE TABLE " + tableNameInDatabase + " PARTITION (p_regionkey) COMPUTE STATISTICS");

        assertThat(query(showStatsWholeTable)).containsOnly(
                row("p_nationkey", null, null, null, null, null, null),
                row("p_name", null, null, null, null, null, null),
                row("p_regionkey", null, 3.0, 0.0, null, "1", "3"),
                row("p_comment", null, null, null, null, null, null),
                row(null, null, null, null, 15.0, null, null));

        assertThat(query(showStatsPartitionOne)).containsOnly(
                row("p_nationkey", null, null, null, null, null, null),
                row("p_name", null, null, null, null, null, null),
                row("p_regionkey", null, 1.0, 0.0, null, "1", "1"),
                row("p_comment", null, null, null, null, null, null),
                row(null, null, null, null, 5.0, null, null));

        assertThat(query(showStatsPartitionTwo)).containsOnly(
                row("p_nationkey", null, null, null, null, null, null),
                row("p_name", null, null, null, null, null, null),
                row("p_regionkey", null, 1.0, 0.0, null, "2", "2"),
                row("p_comment", null, null, null, null, null, null),
                row(null, null, null, null, 5.0, null, null));

        // column analysis for single partition

        onHive().executeQuery("ANALYZE TABLE " + tableNameInDatabase + " PARTITION (p_regionkey = \"1\") COMPUTE STATISTICS FOR COLUMNS");

        assertThat(query(showStatsWholeTable)).containsOnly(
                row("p_nationkey", null, 5.0, 0.0, null, "1", "24"),
                row("p_name", null, 6.0, 0.0, null, null, null),
                row("p_regionkey", null, 3.0, 0.0, null, "1", "3"),
                row("p_comment", null, 1.0, 0.0, null, null, null),
                row(null, null, null, null, 15.0, null, null));

        assertThat(query(showStatsPartitionOne)).containsOnly(
                row("p_nationkey", null, 5.0, 0.0, null, "1", "24"),
                row("p_name", null, 6.0, 0.0, null, null, null),
                row("p_regionkey", null, 1.0, 0.0, null, "1", "1"),
                row("p_comment", null, 1.0, 0.0, null, null, null),
                row(null, null, null, null, 5.0, null, null));

        assertThat(query(showStatsPartitionTwo)).containsOnly(
                row("p_nationkey", null, null, null, null, null, null),
                row("p_name", null, null, null, null, null, null),
                row("p_regionkey", null, 1.0, 0.0, null, "2", "2"),
                row("p_comment", null, null, null, null, null, null),
                row(null, null, null, null, 5.0, null, null));

        // column analysis for all partitions

        onHive().executeQuery("ANALYZE TABLE " + tableNameInDatabase + " PARTITION (p_regionkey) COMPUTE STATISTICS FOR COLUMNS");

        assertThat(query(showStatsWholeTable)).containsOnly(
                row("p_nationkey", null, 5.0, 0.0, null, "1", "24"),
                row("p_name", null, 6.0, 0.0, null, null, null),
                row("p_regionkey", null, 3.0, 0.0, null, "1", "3"),
                row("p_comment", null, 1.0, 0.0, null, null, null),
                row(null, null, null, null, 15.0, null, null));

        assertThat(query(showStatsPartitionOne)).containsOnly(
                row("p_nationkey", null, 5.0, 0.0, null, "1", "24"),
                row("p_name", null, 6.0, 0.0, null, null, null),
                row("p_regionkey", null, 1.0, 0.0, null, "1", "1"),
                row("p_comment", null, 1.0, 0.0, null, null, null),
                row(null, null, null, null, 5.0, null, null));

        assertThat(query(showStatsPartitionTwo)).containsOnly(
                row("p_nationkey", null, 4.0, 0.0, null, "8", "21"),
                row("p_name", null, 6.0, 0.0, null, null, null),
                row("p_regionkey", null, 1.0, 0.0, null, "2", "2"),
                row("p_comment", null, 1.0, 0.0, null, null, null),
                row(null, null, null, null, 5.0, null, null));
    }

    // This covers also stats calculation for unpartitioned table
    @Test(groups = {HIVE_CONNECTOR, SKIP_ON_CDH}) // skip on cdh due to no support for date column and stats
    @Requires(AllTypesTable.class)
    public void testStatisticsForAllDataTypes()
    {
        String tableNameInDatabase = mutableTablesState().get(ALL_TYPES_TABLE_NAME).getNameInDatabase();

        onHive().executeQuery("ANALYZE TABLE " + tableNameInDatabase + " COMPUTE STATISTICS");

        assertThat(query("SHOW STATS FOR " + tableNameInDatabase)).containsOnly(
                row("c_tinyint", null, null, null, null, null, null),
                row("c_smallint", null, null, null, null, null, null),
                row("c_int", null, null, null, null, null, null),
                row("c_bigint", null, null, null, null, null, null),
                row("c_float", null, null, null, null, null, null),
                row("c_double", null, null, null, null, null, null),
                row("c_decimal", null, null, null, null, null, null),
                row("c_decimal_w_params", null, null, null, null, null, null),
                row("c_timestamp", null, null, null, null, null, null),
                row("c_date", null, null, null, null, null, null),
                row("c_string", null, null, null, null, null, null),
                row("c_varchar", null, null, null, null, null, null),
                row("c_char", null, null, null, null, null, null),
                row("c_boolean", null, null, null, null, null, null),
                row("c_binary", null, null, null, null, null, null),
                row(null, null, null, null, 2.0, null, null));

        onHive().executeQuery("ANALYZE TABLE " + tableNameInDatabase + " COMPUTE STATISTICS FOR COLUMNS");

        // SHOW STATS FORMAT: column_name, data_size, distinct_values_count, nulls_fraction, row_count
        assertThat(query("SHOW STATS FOR " + tableNameInDatabase)).containsOnly(
                row("c_tinyint", null, 2.0, 0.0, null, "121", "127"),
                row("c_smallint", null, 2.0, 0.0, null, "32761", "32767"),
                row("c_int", null, 2.0, 0.0, null, "2147483641", "2147483647"),
                row("c_bigint", null, 2.0, 0.0, null, "9223372036854775801", "9223372036854775807"),
                row("c_float", null, 2.0, 0.0, null, "123.341", "123.345"),
                row("c_double", null, 2.0, 0.0, null, "234.561", "235.567"),
                row("c_decimal", null, 2.0, 0.0, null, "345", "346"),
                row("c_decimal_w_params", null, 2.0, 0.0, null, "345.67100", "345.67800"),
                row("c_timestamp", null, 2.0, 0.0, null, "2015-05-10 06:30:31.000", "2015-05-10 06:30:35.000"), // timestamp is shifted by hive.time-zone on read
                row("c_date", null, 2.0, 0.0, null, "2015-05-09", "2015-06-10"),
                row("c_string", null, 2.0, 0.0, null, null, null),
                row("c_varchar", null, 2.0, 0.0, null, null, null),
                row("c_char", null, 2.0, 0.0, null, null, null),
                row("c_boolean", null, 2.0, 0.0, null, null, null),
                row("c_binary", null, null, 0.0, null, null, null),
                row(null, null, null, null, 2.0, null, null));
    }

    @Test(groups = {HIVE_CONNECTOR, SKIP_ON_CDH}) // skip on cdh due to no support for date column and stats
    @Requires(AllTypesTable.class)
    public void testStatisticsForAllDataTypesNoData()
    {
        String tableNameInDatabase = mutableTablesState().get(EMPTY_ALL_TYPES_TABLE_NAME).getNameInDatabase();

        onHive().executeQuery("ANALYZE TABLE " + tableNameInDatabase + " COMPUTE STATISTICS");

        assertThat(query("SHOW STATS FOR " + tableNameInDatabase)).containsOnly(
                row("c_tinyint", null, null, null, null, null, null),
                row("c_smallint", null, null, null, null, null, null),
                row("c_int", null, null, null, null, null, null),
                row("c_bigint", null, null, null, null, null, null),
                row("c_float", null, null, null, null, null, null),
                row("c_double", null, null, null, null, null, null),
                row("c_decimal", null, null, null, null, null, null),
                row("c_decimal_w_params", null, null, null, null, null, null),
                row("c_timestamp", null, null, null, null, null, null),
                row("c_date", null, null, null, null, null, null),
                row("c_string", null, null, null, null, null, null),
                row("c_varchar", null, null, null, null, null, null),
                row("c_char", null, null, null, null, null, null),
                row("c_boolean", null, null, null, null, null, null),
                row("c_binary", null, null, null, null, null, null),
                row(null, null, null, null, 0.0, null, null));

        onHive().executeQuery("ANALYZE TABLE " + tableNameInDatabase + " COMPUTE STATISTICS FOR COLUMNS");

        assertThat(query("SHOW STATS FOR " + tableNameInDatabase)).containsOnly(
                row("c_tinyint", null, 0.0, 0.0, null, null, null),
                row("c_smallint", null, 0.0, 0.0, null, null, null),
                row("c_int", null, 0.0, 0.0, null, null, null),
                row("c_bigint", null, 0.0, 0.0, null, null, null),
                row("c_float", null, 0.0, 0.0, null, null, null),
                row("c_double", null, 0.0, 0.0, null, null, null),
                row("c_decimal", null, 0.0, 0.0, null, null, null),
                row("c_decimal_w_params", null, 0.0, 0.0, null, null, null),
                row("c_timestamp", null, 0.0, 0.0, null, null, null),
                row("c_date", null, 0.0, 0.0, null, null, null),
                row("c_string", null, 0.0, 0.0, null, null, null),
                row("c_varchar", null, 0.0, 0.0, null, null, null),
                row("c_char", null, 0.0, 0.0, null, null, null),
                row("c_boolean", null, 0.0, 0.0, null, null, null),
                row("c_binary", null, null, 0.0, null, null, null),
                row(null, null, null, null, 0.0, null, null));
    }

    @Test(groups = {HIVE_CONNECTOR, SKIP_ON_CDH}) // skip on cdh due to no support for date column and stats
    @Requires(AllTypesTable.class)
    public void testStatisticsForAllDataTypesOnlyNulls()
    {
        String tableNameInDatabase = mutableTablesState().get(EMPTY_ALL_TYPES_TABLE_NAME).getNameInDatabase();
        onHive().executeQuery("INSERT INTO TABLE " + tableNameInDatabase + " VALUES(null, null, null, null, null, null, null, null, null, null, null, null, null, null, null)");

        onHive().executeQuery("ANALYZE TABLE " + tableNameInDatabase + " COMPUTE STATISTICS");

        assertThat(query("SHOW STATS FOR " + tableNameInDatabase)).containsOnly(
                row("c_tinyint", null, null, null, null, null, null),
                row("c_smallint", null, null, null, null, null, null),
                row("c_int", null, null, null, null, null, null),
                row("c_bigint", null, null, null, null, null, null),
                row("c_float", null, null, null, null, null, null),
                row("c_double", null, null, null, null, null, null),
                row("c_decimal", null, null, null, null, null, null),
                row("c_decimal_w_params", null, null, null, null, null, null),
                row("c_timestamp", null, null, null, null, null, null),
                row("c_date", null, null, null, null, null, null),
                row("c_string", null, null, null, null, null, null),
                row("c_varchar", null, null, null, null, null, null),
                row("c_char", null, null, null, null, null, null),
                row("c_boolean", null, null, null, null, null, null),
                row("c_binary", null, null, null, null, null, null),
                row(null, null, null, null, 1.0, null, null));

        onHive().executeQuery("ANALYZE TABLE " + tableNameInDatabase + " COMPUTE STATISTICS FOR COLUMNS");

        assertThat(query("SHOW STATS FOR " + tableNameInDatabase)).containsOnly(
                row("c_tinyint", null, 1.0, 1.0, null, null, null),
                row("c_smallint", null, 1.0, 1.0, null, null, null),
                row("c_int", null, 1.0, 1.0, null, null, null),
                row("c_bigint", null, 1.0, 1.0, null, null, null),
                row("c_float", null, 1.0, 1.0, null, null, null),
                row("c_double", null, 1.0, 1.0, null, null, null),
                row("c_decimal", null, 1.0, 1.0, null, null, null),
                row("c_decimal_w_params", null, 1.0, 1.0, null, null, null),
                row("c_timestamp", null, 1.0, 1.0, null, null, null),
                row("c_date", null, 1.0, 1.0, null, null, null),
                row("c_string", null, 1.0, 1.0, null, null, null),
                row("c_varchar", null, 1.0, 1.0, null, null, null),
                row("c_char", null, 1.0, 1.0, null, null, null),
                row("c_boolean", null, 1.0, 1.0, null, null, null),
                row("c_binary", null, null, 1.0, null, null, null),
                row(null, null, null, null, 1.0, null, null));
    }

    private static QueryExecutor onHive()
    {
        return testContext().getDependency(QueryExecutor.class, "hive");
    }
}
