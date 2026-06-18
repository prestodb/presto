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

package com.facebook.presto.hive.hudi;

import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.hive.HivePlugin;
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Streams;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.facebook.presto.hive.hudi.HudiTestingDataGenerator.DATA_COLUMNS;
import static com.facebook.presto.hive.hudi.HudiTestingDataGenerator.HUDI_META_COLUMNS;
import static com.facebook.presto.hive.hudi.HudiTestingDataGenerator.PARTITION_COLUMNS;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;

public class TestHudiIntegration
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HudiTestUtils.createQueryRunner(
                ImmutableMap.of(),
                ImmutableMap.of(),
                metastore -> new HivePlugin("hive", metastore),
                "hive",
                "hive",
                "testing");
    }

    @Test
    public void testMetadata()
    {
        assertQuery("show tables",
                "SELECT * FROM VALUES " +
                        "('stock_ticks_cow'), " +
                        "('stock_ticks_cown'), " +
                        "('stock_ticks_mor_ro'), " +
                        "('stock_ticks_mor_rt')," +
                        "('stock_ticks_morn_ro')," +
                        "('stock_ticks_morn_rt')," +
                        "('stock_ticks_morn_only_log_ro')," +
                        "('stock_ticks_morn_only_log_rt')");

        FunctionAndTypeManager typeManager = getQueryRunner().getMetadata().getFunctionAndTypeManager();

        // partition tables
        @Language("SQL") String sql = generateDescribeIdenticalQuery(typeManager, HUDI_META_COLUMNS, DATA_COLUMNS, PARTITION_COLUMNS);
        assertQuery("DESCRIBE stock_ticks_cow", sql);
        assertQuery("DESCRIBE stock_ticks_mor_ro", sql);
        assertQuery("DESCRIBE stock_ticks_mor_rt", sql);

        // non-partition tables
        @Language("SQL") String sql2 = generateDescribeIdenticalQuery(typeManager, HUDI_META_COLUMNS, DATA_COLUMNS, ImmutableList.of());
        assertQuery("DESCRIBE stock_ticks_cown", sql2);
        assertQuery("DESCRIBE stock_ticks_morn_ro", sql2);
        assertQuery("DESCRIBE stock_ticks_morn_rt", sql2);
    }

    @Test
    public void testDemoQuery1()
    {
        @Language("SQL") String sqlTemplate = "SELECT symbol, max(ts) FROM %s GROUP BY symbol HAVING symbol = 'GOOG'";
        @Language("SQL") String sqlResult = "SELECT 'GOOG', '2018-08-31 10:59:00'";
        @Language("SQL") String sqlResultReadOptimized = "SELECT 'GOOG', '2018-08-31 10:29:00'";
        @Language("SQL") String sqlResultEmpty = "SELECT * FROM VALUES ('', '') LIMIT 0";

        assertQuery(format(sqlTemplate, "stock_ticks_cow"), sqlResult);
        assertQuery(format(sqlTemplate, "stock_ticks_cown"), sqlResult);
        assertQuery(format(sqlTemplate, "stock_ticks_mor_ro"), sqlResultReadOptimized);
        assertQuery(format(sqlTemplate, "stock_ticks_mor_rt"), sqlResult);
        assertQuery(format(sqlTemplate, "stock_ticks_morn_ro"), sqlResultReadOptimized);
        assertQuery(format(sqlTemplate, "stock_ticks_morn_rt"), sqlResult);
        assertQuery(format(sqlTemplate, "stock_ticks_morn_only_log_ro"), sqlResultEmpty);
        assertQuery(format(sqlTemplate, "stock_ticks_morn_only_log_rt"), sqlResult);
    }

    @Test
    public void testDemoQuery2()
    {
        // Column _hoodie_commit_time changed to _hoodie_record_key
        @Language("SQL") String sqlTemplate = "SELECT \"_hoodie_record_key\", symbol, ts, volume, open, close  FROM %s WHERE symbol = 'GOOG'";
        @Language("SQL") String sqlResult = "SELECT * FROM VALUES " +
                "('GOOG_2018-08-31 09', 'GOOG', '2018-08-31 09:59:00', 6330, 1230.5, 1230.02), " +
                "('GOOG_2018-08-31 10', 'GOOG', '2018-08-31 10:59:00', 9021, 1227.1993, 1227.215)";
        @Language("SQL") String sqlResultReadOptimized = "SELECT * FROM VALUES " +
                "('GOOG_2018-08-31 09', 'GOOG', '2018-08-31 09:59:00', 6330, 1230.5, 1230.02), " +
                "('GOOG_2018-08-31 10', 'GOOG', '2018-08-31 10:29:00', 3391, 1230.1899, 1230.085)";
        @Language("SQL") String sqlResultEmpty = "SELECT * FROM VALUES ('', '') LIMIT 0";

        assertQuery(format(sqlTemplate, "stock_ticks_cow"), sqlResult);
        assertQuery(format(sqlTemplate, "stock_ticks_cown"), sqlResult);
        assertQuery(format(sqlTemplate, "stock_ticks_mor_ro"), sqlResultReadOptimized);
        assertQuery(format(sqlTemplate, "stock_ticks_mor_rt"), sqlResult);
        assertQuery(format(sqlTemplate, "stock_ticks_morn_ro"), sqlResultReadOptimized);
        assertQuery(format(sqlTemplate, "stock_ticks_morn_rt"), sqlResult);
        assertQuery(format(sqlTemplate, "stock_ticks_morn_only_log_ro"), sqlResultEmpty);
        assertQuery(format(sqlTemplate, "stock_ticks_morn_only_log_rt"), sqlResult);
    }

    @Test
    public void testQueryWithPartitionColumn()
    {
        @Language("SQL") String sqlTemplate = "SELECT symbol, ts, dt FROM %s WHERE symbol = 'GOOG' AND dt = '2018-08-31'";
        @Language("SQL") String sqlResult = "SELECT * FROM VALUES " +
                "('GOOG', '2018-08-31 09:59:00', '2018-08-31')," +
                "('GOOG', '2018-08-31 10:59:00', '2018-08-31')";
        @Language("SQL") String sqlResultReadOptimized = "SELECT * FROM VALUES " +
                "('GOOG', '2018-08-31 09:59:00', '2018-08-31')," +
                "('GOOG', '2018-08-31 10:29:00', '2018-08-31')";

        assertQuery(format(sqlTemplate, "stock_ticks_cow"), sqlResult);
        assertQuery(format(sqlTemplate, "stock_ticks_mor_ro"), sqlResultReadOptimized);
        assertQuery(format(sqlTemplate, "stock_ticks_mor_rt"), sqlResult);
    }

    @Test
    public void testQueryOnUnavailablePartition()
    {
        @Language("SQL") String sqlTemplate = "SELECT symbol, ts, dt FROM %s WHERE symbol = 'GOOG' AND dt = '2018-08-30'";
        @Language("SQL") String sqlResult = "SELECT * FROM VALUES ('', '', '') LIMIT 0";

        assertQuery(format(sqlTemplate, "stock_ticks_cow"), sqlResult);
        assertQuery(format(sqlTemplate, "stock_ticks_mor_ro"), sqlResult);
        assertQuery(format(sqlTemplate, "stock_ticks_mor_rt"), sqlResult);
    }

    private static String generateDescribeIdenticalQuery(TypeManager typeManager, List<Column> metaColumns, List<Column> dataColumns, List<Column> partitionColumns)
    {
        Stream<String> regularRows = Streams.concat(metaColumns.stream(), dataColumns.stream())
                .map(column -> {
                    return format("('%s', '%s', '', '', %s, NULL, %s)", column.getName(), column.getType().getType(typeManager).getDisplayName(),
                            getColumnSize(column.getType().getType(typeManager).getDisplayName()),
                            (column.getType().getType(typeManager).getDisplayName()).toLowerCase(ENGLISH).equals("varchar") ? "2147483647" : "NULL");
                });

        Stream<String> partitionRows = partitionColumns.stream()
                .map(column -> {
                    return format("('%s', '%s', 'partition key', '', %s, NULL, %s)", column.getName(),
                            column.getType().getType(typeManager).getDisplayName(),
                            getColumnSize(column.getType().getType(typeManager).getDisplayName()),
                            (column.getType().getType(typeManager).getDisplayName()).toLowerCase(ENGLISH).equals("varchar") ? "2147483647" : "NULL");
                });

        String rows = Streams.concat(regularRows, partitionRows)
                .collect(Collectors.joining(", "));

        return "SELECT * FROM VALUES " + rows;
    }

    private static String getColumnSize(String type)
    {
        switch (type.toLowerCase(ENGLISH)) {
            case "bigint":
                return "19";
            case "integer":
                return "10";
            case "smallint":
                return "5";
            case "tinyint":
                return "3";
            case "double":
                return "53";
            case "real":
                return "24";
            default:
                return "NULL";
        }
    }
}
