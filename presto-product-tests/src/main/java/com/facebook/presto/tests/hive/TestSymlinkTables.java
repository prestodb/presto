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
import com.google.inject.name.Named;
import io.prestodb.tempto.ProductTest;
import io.prestodb.tempto.hadoop.hdfs.HdfsClient;
import jakarta.inject.Inject;
import org.testng.annotations.Test;

import java.io.IOException;

import static com.facebook.presto.tests.TestGroups.HIVE_SYMLINK_TABLE;
import static com.facebook.presto.tests.hive.SymlinkTestUtils.createParquetSymlinkTable;
import static com.facebook.presto.tests.hive.SymlinkTestUtils.deleteSymlinkTable;
import static com.facebook.presto.tests.hive.SymlinkTestUtils.saveResourceOnHdfs;
import static com.facebook.presto.tests.utils.QueryExecutors.onPresto;
import static io.prestodb.tempto.assertions.QueryAssert.Row.row;
import static io.prestodb.tempto.assertions.QueryAssert.assertThat;

public class TestSymlinkTables
        extends ProductTest
{
    @Inject
    @Named("databases.hive.warehouse_directory_path")
    private String warehouseDirectoryPath;

    @Inject
    private HdfsClient hdfsClient;

    @Test(groups = {HIVE_SYMLINK_TABLE})
    public void testHiveSymlinkTable()
            throws IOException
    {
        String table = "test_parquet_symlink";
        createParquetSymlinkTable(hdfsClient, table, warehouseDirectoryPath, false);

        String dataDir = warehouseDirectoryPath + "/data_" + table;

        // Save a non-parquet file in dataDir path which should throw an error if tried to access as parquet
        saveResourceOnHdfs(hdfsClient, "data.orc", dataDir + "/data.orc");

        assertThat(onPresto().executeQuery("SELECT * FROM hive.default." + table)).containsExactly(row(42));

        deleteSymlinkTable(hdfsClient, table, ImmutableList.of(dataDir));
    }

    @Test(groups = {HIVE_SYMLINK_TABLE})
    public void testHiveSymlinkTableWithTargetsInMultipleDirectories()
            throws IOException
    {
        String table = "test_parquet_symlink_with_multiple_directories";
        createParquetSymlinkTable(hdfsClient, table, warehouseDirectoryPath, true);

        String dataDir = warehouseDirectoryPath + "/data_" + table;
        String secondDataDir = warehouseDirectoryPath + "/secondData_" + table;

        // Save a non-parquet file in dataDir path which should throw an error if tried to access as parquet
        saveResourceOnHdfs(hdfsClient, "data.orc", dataDir + "/data.orc");

        assertThat(onPresto().executeQuery("SELECT count(*) as count FROM hive.default." + table)).containsExactly(row(2));

        deleteSymlinkTable(hdfsClient, table, ImmutableList.of(dataDir, secondDataDir));
    }
}
