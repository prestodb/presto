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
package com.facebook.presto.tests.convention;

import com.teradata.tempto.ProductTest;
import com.teradata.tempto.Requirement;
import com.teradata.tempto.RequirementsProvider;
import com.teradata.tempto.configuration.Configuration;
import com.teradata.tempto.fulfillment.table.MutableTableRequirement;
import com.teradata.tempto.fulfillment.table.MutableTablesState;
import com.teradata.tempto.fulfillment.table.TableDefinition;
import com.teradata.tempto.fulfillment.table.hive.HiveDataSource;
import com.teradata.tempto.fulfillment.table.hive.HiveTableDefinition;
import com.teradata.tempto.query.QueryResult;
import org.testng.annotations.Test;

import javax.inject.Inject;

import java.sql.SQLException;
import java.util.concurrent.ThreadLocalRandom;

import static com.facebook.presto.tests.TestGroups.BASIC_SQL;
import static com.teradata.tempto.Requirements.compose;
import static com.teradata.tempto.assertions.QueryAssert.Row.row;
import static com.teradata.tempto.assertions.QueryAssert.assertThat;
import static com.teradata.tempto.fulfillment.table.TableRequirements.mutableTable;
import static com.teradata.tempto.fulfillment.table.hive.InlineDataSource.createResourceDataSource;
import static com.teradata.tempto.fulfillment.table.hive.InlineDataSource.createStringDataSource;
import static com.teradata.tempto.query.QueryExecutor.query;

public class TestShowPartitions
        extends ProductTest
        implements RequirementsProvider
{
    private static final String TABLE_NAME = "partitioned_table";

    @Inject
    private MutableTablesState tablesState;

    @Override
    public Requirement getRequirements(Configuration configuration)
    {
        return compose(mutableTable(generateTableDefinition(), TABLE_NAME, MutableTableRequirement.State.CREATED));
    }

    private static TableDefinition generateTableDefinition()
    {
        StringBuilder createTableDdl = new StringBuilder();
        createTableDdl.append("CREATE EXTERNAL TABLE %NAME%(");
        createTableDdl.append("   col INT");
        createTableDdl.append(") ");
        createTableDdl.append("PARTITIONED BY (part_col INT) ");
        createTableDdl.append(" STORED AS ORC");

        HiveDataSource dataSource = createResourceDataSource(TABLE_NAME, String.valueOf(ThreadLocalRandom.current().nextLong(Long.MAX_VALUE)), "com/facebook/presto/tests/hive/data/single_int_column/data.orc");
        HiveDataSource invalidData = createStringDataSource(TABLE_NAME, String.valueOf(ThreadLocalRandom.current().nextLong(Long.MAX_VALUE)), "INVALID DATA");
        return HiveTableDefinition.builder(TABLE_NAME)
                .setCreateTableDDLTemplate(createTableDdl.toString())
                .addPartition("part_col = 1", invalidData)
                .addPartition("part_col = 2", dataSource)
                .build();
    }

    @Test(groups = {BASIC_SQL})
    public void testSelectPartitionedHiveTableDifferentFormats()
            throws SQLException
    {
        String tableNameInDatabase = tablesState.get(TABLE_NAME).getNameInDatabase();

        String selectFromOnePartitionsSql = "show partitions from " + tableNameInDatabase;
        QueryResult partitionListResult = query(selectFromOnePartitionsSql);
        assertThat(partitionListResult).containsExactly(row(1), row(2));
    }
}
