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
package com.facebook.presto.hive;

import com.facebook.presto.hive.filesystem.ExtendedFileSystem;
import com.facebook.presto.hive.metastore.Storage;
import com.facebook.presto.hive.metastore.Table;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.exception.TableNotFoundException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Iterator;
import java.util.Optional;

import static com.facebook.presto.hive.BucketFunctionType.HIVE_COMPATIBLE;
import static com.facebook.presto.hive.HiveStorageFormat.PARQUET;
import static com.facebook.presto.hive.HiveTestUtils.SESSION;
import static com.facebook.presto.hive.NestedDirectoryPolicy.IGNORED;
import static com.facebook.presto.hive.metastore.PrestoTableType.EXTERNAL_TABLE;
import static com.facebook.presto.hive.metastore.StorageFormat.fromHiveStorageFormat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

public class TestHudiDirectoryLister
{
    private Configuration hadoopConf;

    @BeforeClass
    private void setup()
    {
        hadoopConf = new Configuration();
        hadoopConf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        hadoopConf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
    }

    @AfterClass(alwaysRun = true)
    private void tearDown()
    {
        hadoopConf = null;
    }

    @Test
    public void testDirectoryListerForHudiTable()
            throws IOException
    {
        Table mockTable = new Table(
                "schema",
                "hudi_non_part_cow",
                "user",
                EXTERNAL_TABLE,
                new Storage(fromHiveStorageFormat(PARQUET),
                        getTableBasePath("hudi_non_part_cow"),
                        Optional.of(new HiveBucketProperty(
                                ImmutableList.of(),
                                1,
                                ImmutableList.of(),
                                HIVE_COMPATIBLE,
                                Optional.empty())),
                        false,
                        ImmutableMap.of(),
                        ImmutableMap.of()),
                ImmutableList.of(),
                ImmutableList.of(),
                ImmutableMap.of(),
                Optional.empty(),
                Optional.empty());

        HudiDirectoryLister directoryLister = new HudiDirectoryLister(hadoopConf, SESSION, mockTable);
        HoodieTableMetaClient metaClient = directoryLister.getMetaClient();
        assertEquals(metaClient.getBasePath(), mockTable.getStorage().getLocation());
        Path path = new Path(mockTable.getStorage().getLocation());
        ExtendedFileSystem fs = (ExtendedFileSystem) path.getFileSystem(hadoopConf);
        Iterator<HiveFileInfo> fileInfoIterator = directoryLister.list(fs, mockTable, path, new NamenodeStats(), new HiveDirectoryContext(IGNORED, false));
        assertTrue(fileInfoIterator.hasNext());
        HiveFileInfo fileInfo = fileInfoIterator.next();
        assertEquals(fileInfo.getPath().getName(), "d0875d00-483d-4e8b-bbbe-c520366c47a0-0_0-6-11_20211217110514527.parquet");
    }

    @Test
    public void testDirectoryListerForNonHudiTable()
    {
        Table mockTable = new Table(
                "schema",
                "non_hudi_table",
                "user",
                EXTERNAL_TABLE,
                new Storage(fromHiveStorageFormat(PARQUET),
                        getTableBasePath("non_hudi_table"),
                        Optional.of(new HiveBucketProperty(
                                ImmutableList.of(),
                                1,
                                ImmutableList.of(),
                                HIVE_COMPATIBLE,
                                Optional.empty())),
                        false,
                        ImmutableMap.of(),
                        ImmutableMap.of()),
                ImmutableList.of(),
                ImmutableList.of(),
                ImmutableMap.of(),
                Optional.empty(),
                Optional.empty());

        assertThrows(TableNotFoundException.class, () -> new HudiDirectoryLister(hadoopConf, SESSION, mockTable));
    }

    private static String getTableBasePath(String tableName)
    {
        return TestHudiDirectoryLister.class.getClassLoader().getResource(tableName).toString();
    }
}
