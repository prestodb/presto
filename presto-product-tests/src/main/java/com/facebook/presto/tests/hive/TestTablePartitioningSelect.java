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

import com.google.inject.Inject;
import io.prestodb.tempto.ProductTest;
import io.prestodb.tempto.Requirement;
import io.prestodb.tempto.RequirementsProvider;
import io.prestodb.tempto.configuration.Configuration;
import io.prestodb.tempto.fulfillment.table.MutableTablesState;
import io.prestodb.tempto.fulfillment.table.hive.HiveDataSource;
import io.prestodb.tempto.fulfillment.table.hive.HiveTableDefinition;
import io.prestodb.tempto.query.QueryExecutionException;
import io.prestodb.tempto.query.QueryResult;
import org.testng.annotations.Test;

import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

import static com.facebook.presto.tests.TestGroups.HIVE_CONNECTOR;
import static io.prestodb.tempto.Requirements.allOf;
import static io.prestodb.tempto.assertions.QueryAssert.Row.row;
import static io.prestodb.tempto.assertions.QueryAssert.assertThat;
import static io.prestodb.tempto.fulfillment.table.MutableTableRequirement.State.LOADED;
import static io.prestodb.tempto.fulfillment.table.TableRequirements.mutableTable;
import static io.prestodb.tempto.fulfillment.table.hive.InlineDataSource.createResourceDataSource;
import static io.prestodb.tempto.fulfillment.table.hive.InlineDataSource.createStringDataSource;
import static io.prestodb.tempto.query.QueryExecutor.query;

public class TestTablePartitioningSelect
        extends ProductTest
        implements RequirementsProvider
{
    private static final HiveTableDefinition SINGLE_INT_COLUMN_PARTITIONEND_TEXTFILE = singleIntColumnPartitionedTableDefinition("TEXTFILE", Optional.of("DELIMITED FIELDS TERMINATED BY '|'"));
    private static final HiveTableDefinition SINGLE_INT_COLUMN_PARTITIONED_ORC = singleIntColumnPartitionedTableDefinition("ORC", Optional.empty());
    private static final HiveTableDefinition SINGLE_INT_COLUMN_PARTITIONED_RCFILE = singleIntColumnPartitionedTableDefinition("RCFILE", Optional.of("SERDE 'org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe'"));
    private static final HiveTableDefinition SINGLE_INT_COLUMN_PARTITIONED_PARQUET = singleIntColumnPartitionedTableDefinition("PARQUET", Optional.empty());
    private static final HiveTableDefinition SINGLE_INT_COLUMN_PARTITIONED_AVRO = singleIntColumnPartitionedTableDefinition("AVRO", Optional.of("SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'"));
    private static final String TABLE_NAME = "test_table";

    @Inject
    private MutableTablesState tablesState;

    private static HiveTableDefinition singleIntColumnPartitionedTableDefinition(String fileFormat, Optional<String> serde)
    {
        String tableName = fileFormat.toLowerCase(Locale.ENGLISH) + "_single_int_column_partitioned";
        HiveDataSource dataSource = createResourceDataSource(tableName, String.valueOf(ThreadLocalRandom.current().nextLong(Long.MAX_VALUE)), "com/facebook/presto/tests/hive/data/single_int_column/data." + fileFormat.toLowerCase(Locale.ENGLISH));
        HiveDataSource invalidData = createStringDataSource(tableName, String.valueOf(ThreadLocalRandom.current().nextLong(Long.MAX_VALUE)), "INVALID DATA");
        return HiveTableDefinition.builder(tableName)
                .setCreateTableDDLTemplate(buildSingleIntColumnPartitionedTableDDL(fileFormat, serde))
                .addPartition("part_col = 1", invalidData)
                .addPartition("part_col = 2", dataSource)
                .build();
    }

    private static String buildSingleIntColumnPartitionedTableDDL(String fileFormat, Optional<String> rowFormat)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE %EXTERNAL% TABLE %NAME%(");
        sb.append("   col INT");
        sb.append(") ");
        sb.append("PARTITIONED BY (part_col INT) ");
        if (rowFormat.isPresent()) {
            sb.append("ROW FORMAT ").append(rowFormat.get());
        }
        sb.append(" STORED AS " + fileFormat);
        return sb.toString();
    }

    @Override
    public Requirement getRequirements(Configuration configuration)
    {
        return allOf(
                mutableTable(SINGLE_INT_COLUMN_PARTITIONEND_TEXTFILE, TABLE_NAME, LOADED),
                mutableTable(SINGLE_INT_COLUMN_PARTITIONED_ORC, TABLE_NAME, LOADED),
                mutableTable(SINGLE_INT_COLUMN_PARTITIONED_RCFILE, TABLE_NAME, LOADED),
                mutableTable(SINGLE_INT_COLUMN_PARTITIONED_PARQUET, TABLE_NAME, LOADED),
                mutableTable(SINGLE_INT_COLUMN_PARTITIONED_AVRO, TABLE_NAME, LOADED));
    }

    @Test(groups = {HIVE_CONNECTOR})
    public void testSelectPartitionedHiveTableDifferentFormats()
    {
        String tableNameInDatabase = tablesState.get(TABLE_NAME).getNameInDatabase();

        String selectFromOnePartitionsSql = "SELECT * FROM " + tableNameInDatabase + " WHERE part_col = 2";
        QueryResult onePartitionQueryResult = query(selectFromOnePartitionsSql);
        assertThat(onePartitionQueryResult).containsOnly(row(42, 2));

        try {
            // This query should fail or return null values for invalid partition data
            assertThat(query("SELECT * FROM " + tableNameInDatabase)).containsOnly(row(42, 2), row(null, 1));
        }
        catch (QueryExecutionException expectedDueToInvalidPartitionData) {
        }
    }
}
