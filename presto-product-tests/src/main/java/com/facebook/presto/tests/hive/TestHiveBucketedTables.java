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
import io.prestodb.tempto.configuration.Configuration;
import io.prestodb.tempto.fulfillment.table.MutableTableRequirement;
import io.prestodb.tempto.fulfillment.table.TableDefinitionsRepository;
import io.prestodb.tempto.fulfillment.table.hive.HiveTableDefinition;
import org.testng.annotations.Test;

import static com.facebook.presto.tests.TestGroups.HIVE_CONNECTOR;
import static com.facebook.presto.tests.utils.QueryExecutors.onHive;
import static io.prestodb.tempto.assertions.QueryAssert.Row.row;
import static io.prestodb.tempto.assertions.QueryAssert.assertThat;
import static io.prestodb.tempto.fulfillment.table.MutableTableRequirement.State.CREATED;
import static io.prestodb.tempto.fulfillment.table.MutableTablesState.mutableTablesState;
import static io.prestodb.tempto.fulfillment.table.TableRequirements.immutableTable;
import static io.prestodb.tempto.fulfillment.table.hive.tpch.TpchTableDefinitions.NATION;
import static io.prestodb.tempto.query.QueryExecutor.query;
import static java.lang.String.format;

public class TestHiveBucketedTables
        extends ProductTest
        implements RequirementsProvider
{
    @TableDefinitionsRepository.RepositoryTableDefinition
    public static final HiveTableDefinition BUCKETED_PARTITIONED_NATION = HiveTableDefinition.builder("bucket_partition_nation")
            .setCreateTableDDLTemplate("CREATE TABLE %NAME%(" +
                    "n_nationkey     BIGINT," +
                    "n_name          STRING," +
                    "n_regionkey     BIGINT," +
                    "n_comment       STRING) " +
                    "PARTITIONED BY (part_key STRING) " +
                    "CLUSTERED BY (n_regionkey) " +
                    "INTO 4 BUCKETS " +
                    "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'")
            .setNoData()
            .build();

    @Override
    public Requirement getRequirements(Configuration configuration)
    {
        return Requirements.compose(
                MutableTableRequirement.builder(BUCKETED_PARTITIONED_NATION).withState(CREATED).build(),
                immutableTable(NATION));
    }

    @Test(groups = {HIVE_CONNECTOR})
    public void testIgnorePartitionBucketingIfNotBucketed()
    {
        String tableName = mutableTablesState().get(BUCKETED_PARTITIONED_NATION).getNameInDatabase();
        populateHivePartitionedTable(tableName, NATION.getName(), "part_key = 'insert_1'");
        populateHivePartitionedTable(tableName, NATION.getName(), "part_key = 'insert_2'");

        onHive().executeQuery(format("ALTER TABLE %s NOT CLUSTERED", tableName));

        assertThat(query(format("SELECT count(*) FROM %s WHERE n_nationkey = 1", tableName)))
                .containsExactly(row(2));
    }

    private static void populateHivePartitionedTable(String destination, String source, String partition)
    {
        String queryStatement = format("INSERT INTO TABLE %s PARTITION (%s) SELECT * FROM %s", destination, partition, source);

        onHive().executeQuery("set hive.enforce.bucketing = true");
        onHive().executeQuery("set hive.enforce.sorting = true");
        onHive().executeQuery(queryStatement);
    }
}
