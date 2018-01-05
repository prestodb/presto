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

import com.teradata.tempto.ProductTest;
import com.teradata.tempto.Requirement;
import com.teradata.tempto.Requirements;
import com.teradata.tempto.RequirementsProvider;
import com.teradata.tempto.configuration.Configuration;
import com.teradata.tempto.fulfillment.table.MutableTableRequirement;
import com.teradata.tempto.fulfillment.table.TableDefinitionsRepository;
import com.teradata.tempto.fulfillment.table.hive.HiveTableDefinition;
import com.teradata.tempto.query.QueryExecutor;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.tests.TestGroups.HIVE_CONNECTOR;
import static com.facebook.presto.tests.utils.QueryExecutors.onHive;
import static com.teradata.tempto.assertions.QueryAssert.Row.row;
import static com.teradata.tempto.assertions.QueryAssert.assertThat;
import static com.teradata.tempto.context.ThreadLocalTestContextHolder.testContext;
import static com.teradata.tempto.fulfillment.table.MutableTableRequirement.State.CREATED;
import static com.teradata.tempto.fulfillment.table.MutableTablesState.mutableTablesState;
import static com.teradata.tempto.fulfillment.table.TableRequirements.immutableTable;
import static com.teradata.tempto.fulfillment.table.hive.tpch.TpchTableDefinitions.NATION;
import static com.teradata.tempto.query.QueryExecutor.query;
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
        populateDataToHiveTable(tableName, NATION.getName(), Optional.of("part_key = 'insert_1'"));
        populateDataToHiveTable(tableName, NATION.getName(), Optional.of("part_key = 'insert_2'"));

        testContext().getDependency(QueryExecutor.class, "hive").executeQuery(format("ALTER TABLE %s NOT CLUSTERED", tableName));

        assertThat(query(format("SELECT count(*) FROM %s WHERE n_nationkey = 1", tableName)))
                .containsExactly(row(2));
    }

    private static void populateDataToHiveTable(String destination, String source, Optional<String> partition)
    {
        String queryStatement = format("INSERT INTO TABLE %s" +
                        (partition.isPresent() ? format(" PARTITION (%s) ", partition.get()) : " ") +
                        "SELECT * FROM %s",
                destination, source);

        onHive().executeQuery("set hive.enforce.bucketing = true");
        onHive().executeQuery("set hive.enforce.sorting = true");
        onHive().executeQuery(queryStatement);
    }
}
