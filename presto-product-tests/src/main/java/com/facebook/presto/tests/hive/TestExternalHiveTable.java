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
import com.teradata.tempto.RequirementsProvider;
import com.teradata.tempto.configuration.Configuration;
import com.teradata.tempto.fulfillment.table.TableInstance;
import org.testng.annotations.Test;

import static com.facebook.presto.tests.TestGroups.HIVE_CONNECTOR;
import static com.facebook.presto.tests.hive.HiveTableDefinitions.NATION_PARTITIONED_BY_REGIONKEY;
import static com.facebook.presto.tests.hive.HiveTableDefinitions.NATION_PARTITIONED_BY_REGIONKEY_NUMBER_OF_LINES_PER_SPLIT;
import static com.facebook.presto.tests.utils.QueryExecutors.onHive;
import static com.facebook.presto.tests.utils.QueryExecutors.onPresto;
import static com.teradata.tempto.Requirements.compose;
import static com.teradata.tempto.assertions.QueryAssert.assertThat;
import static com.teradata.tempto.fulfillment.table.MutableTablesState.mutableTablesState;
import static com.teradata.tempto.fulfillment.table.TableRequirements.mutableTable;
import static com.teradata.tempto.fulfillment.table.hive.tpch.TpchTableDefinitions.NATION;

public class TestExternalHiveTable
        extends ProductTest
        implements RequirementsProvider
{
    private static final String EXTERNAL_TABLE_NAME = "target_table";

    public Requirement getRequirements(Configuration configuration)
    {
        return compose(
                mutableTable(NATION),
                mutableTable(NATION_PARTITIONED_BY_REGIONKEY));
    }

    @Test(groups = {HIVE_CONNECTOR})
    public void testInsertIntoExternalTable()
    {
        TableInstance nation = mutableTablesState().get(NATION.getName());
        onHive().executeQuery("DROP TABLE IF EXISTS " + EXTERNAL_TABLE_NAME);
        onHive().executeQuery("CREATE EXTERNAL TABLE " + EXTERNAL_TABLE_NAME + " LIKE " + nation.getNameInDatabase());
        assertThat(() -> onPresto().executeQuery(
                "INSERT INTO hive.default." + EXTERNAL_TABLE_NAME + " SELECT * FROM hive.default." + nation.getNameInDatabase()))
                .failsWithMessage("Cannot write to non-managed Hive table");
    }

    @Test(groups = {HIVE_CONNECTOR})
    public void testDeleteFromExternalTable()
    {
        TableInstance nation = mutableTablesState().get(NATION.getName());
        onHive().executeQuery("DROP TABLE IF EXISTS " + EXTERNAL_TABLE_NAME);
        onHive().executeQuery("CREATE EXTERNAL TABLE " + EXTERNAL_TABLE_NAME + " LIKE " + nation.getNameInDatabase());
        assertThat(() -> onPresto().executeQuery("DELETE FROM hive.default." + EXTERNAL_TABLE_NAME))
                .failsWithMessage("Cannot delete from non-managed Hive table");
    }

    @Test(groups = {HIVE_CONNECTOR})
    public void testDeleteFromExternalPartitionedTableTable()
    {
        TableInstance nation = mutableTablesState().get(NATION_PARTITIONED_BY_REGIONKEY.getName());
        onHive().executeQuery("DROP TABLE IF EXISTS " + EXTERNAL_TABLE_NAME);
        onHive().executeQuery("CREATE EXTERNAL TABLE " + EXTERNAL_TABLE_NAME + " LIKE " + nation.getNameInDatabase() + " LOCATION '/tmp/" + EXTERNAL_TABLE_NAME + "_" + nation.getNameInDatabase() + "'");
        insertNationPartition(nation, 1);
        insertNationPartition(nation, 2);
        insertNationPartition(nation, 3);
        assertThat(onPresto().executeQuery("SELECT * FROM " + EXTERNAL_TABLE_NAME))
                .hasRowsCount(3 * NATION_PARTITIONED_BY_REGIONKEY_NUMBER_OF_LINES_PER_SPLIT);

        assertThat(() -> onPresto().executeQuery("DELETE FROM hive.default." + EXTERNAL_TABLE_NAME + " WHERE p_name IS NOT NULL"))
                .failsWithMessage("This connector only supports delete where one or more partitions are deleted entirely");

        onPresto().executeQuery("DELETE FROM hive.default." + EXTERNAL_TABLE_NAME + " WHERE p_regionkey = 1");
        assertThat(onPresto().executeQuery("SELECT * FROM " + EXTERNAL_TABLE_NAME))
                .hasRowsCount(2 * NATION_PARTITIONED_BY_REGIONKEY_NUMBER_OF_LINES_PER_SPLIT);

        onPresto().executeQuery("DELETE FROM hive.default." + EXTERNAL_TABLE_NAME);
        assertThat(onPresto().executeQuery("SELECT * FROM " + EXTERNAL_TABLE_NAME)).hasRowsCount(0);
    }

    private void insertNationPartition(TableInstance nation, int partition)
    {
        onHive().executeQuery(
                "INSERT INTO TABLE " + EXTERNAL_TABLE_NAME + " PARTITION (p_regionkey=" + partition + ")"
                        + " SELECT p_nationkey, p_name, p_comment FROM " + nation.getNameInDatabase()
                        + " WHERE p_regionkey=" + partition);
    }
}
