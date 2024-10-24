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
package com.facebook.presto.nativeworker;

import com.facebook.presto.hive.HiveType;
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tpch.ColumnNaming;
import com.google.common.collect.ImmutableList;
import io.airlift.tpch.TpchColumn;
import io.airlift.tpch.TpchEntity;
import io.airlift.tpch.TpchTable;
import org.testng.annotations.AfterClass;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createCustomer;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createLineitem;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createNationWithFormat;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createOrders;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createPart;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createPartSupp;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createRegion;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createSupplier;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.createExternalTable;
import static com.facebook.presto.nativeworker.SymlinkManifestGeneratorUtils.cleanupSymlinkData;
import static com.facebook.presto.tpch.TpchMetadata.getPrestoType;
import static java.lang.String.format;

public abstract class AbstractTestNativeHiveExternalTableTpchQueries
        extends AbstractTestNativeTpchQueries
{
    private static final String HIVE = "hive";
    private static final String TPCH = "tpch";
    private static final String SYMLINK_FOLDER = "symlink_tables_manifests";
    private static final String HIVE_DATA = "hive_data";
    private static final ImmutableList<String> TPCH_TABLES = ImmutableList.of("orders", "lineitem", "nation",
            "customer", "part", "partsupp", "region", "supplier");

    /**
     * Returns the Hive type string corresponding to a given TPCH type.
     * Currently only supports conversion for "integer" TPCH type.
     *
     * @param tpchType the TPCH type as a string
     * @return the corresponding Hive type as a string
     */
    private static String getHiveTypeString(String tpchType)
    {
        switch (tpchType) {
            case "integer":
                return "int";
            default:
                return tpchType;
        }
    }

    /**
     * Retrieves a list of columns for a specified TPCH table.
     * The method looks up the TPCH table by name and converts its columns to Hive-compatible
     * column representations.
     *
     * @param tableName the name of the TPCH table
     * @return a list of Column objects representing the columns of the table
     */
    private static List<Column> getTpchTableColumns(String tableName)
    {
        TpchTable<?> table = TpchTable.getTable(tableName);
        ColumnNaming columnNaming = ColumnNaming.SIMPLIFIED;
        ImmutableList.Builder<Column> columns = ImmutableList.builder();
        for (TpchColumn<? extends TpchEntity> column : table.getColumns()) {
            columns.add(new Column(columnNaming.getName(column), HiveType.valueOf(getHiveTypeString(getPrestoType(column).getDisplayName())), Optional.empty(), Optional.empty()));
        }
        return columns.build();
    }

    @Override
    protected void createTables()
    {
        QueryRunner javaQueryRunner = (QueryRunner) getExpectedQueryRunner();
        createOrders(javaQueryRunner);
        createLineitem(javaQueryRunner);
        createNationWithFormat(javaQueryRunner, "PARQUET");
        createCustomer(javaQueryRunner);
        createPart(javaQueryRunner);
        createPartSupp(javaQueryRunner);
        createRegion(javaQueryRunner);
        createSupplier(javaQueryRunner);

        for (String tableName : TPCH_TABLES) {
            createExternalTable(javaQueryRunner, TPCH, tableName, getTpchTableColumns(tableName));
        }
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        QueryRunner javaQueryRunner = (QueryRunner) getExpectedQueryRunner();
        for (String tableName : TPCH_TABLES) {
            dropTableIfExists(javaQueryRunner, HIVE, TPCH, tableName);
        }
        assertUpdate(format("DROP SCHEMA IF EXISTS %s.%s", HIVE, TPCH));

        File dataDirectory = ((DistributedQueryRunner) javaQueryRunner).getCoordinator().getDataDirectory().resolve(HIVE_DATA).toFile();
        Path symlinkTableDataPath = dataDirectory.toPath().getParent().resolve(SYMLINK_FOLDER);
        try {
            cleanupSymlinkData(symlinkTableDataPath);
        }
        catch (IOException e) {
            // ignore
        }
    }
}
