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

import com.google.inject.name.Named;
import io.prestodb.tempto.AfterTestWithContext;
import io.prestodb.tempto.BeforeTestWithContext;
import io.prestodb.tempto.ProductTest;
import io.prestodb.tempto.hadoop.hdfs.HdfsClient;
import io.prestodb.tempto.query.QueryResult;
import org.testng.annotations.Test;

import javax.inject.Inject;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;

import static com.facebook.presto.tests.TestGroups.HIVE_LIST_CACHING;
import static com.facebook.presto.tests.utils.QueryExecutors.onHive;
import static com.google.common.io.Resources.getResource;
import static io.prestodb.tempto.query.QueryExecutor.query;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;

public class TestSymlinkTableListCaching
        extends ProductTest
{
    @Inject
    @Named("databases.hive.warehouse_directory_path")
    private String warehouseDirectoryPath;

    @Inject
    private HdfsClient hdfsClient;

    @BeforeTestWithContext
    public void setUp() throws IOException
    {
        String table = "hive_symlink_table";
        onHive().executeQuery("DROP TABLE IF EXISTS " + table);

        onHive().executeQuery("" +
                "CREATE TABLE default.hive_symlink_table " +
                "(col int) " +
                "ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' " +
                "STORED AS " +
                "INPUTFORMAT 'org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat' " +
                "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat' ");

        String tableRoot = warehouseDirectoryPath + "/hive_symlink_table";
        String dataDir = warehouseDirectoryPath + "/data_hive_symlink_table";

        saveResourceOnHdfs("data.parquet", dataDir + "/data.parquet");
        hdfsClient.saveFile(tableRoot + "/manifest", format("hdfs://hadoop-master:9000%s/data.parquet", dataDir));
    }

    @Test(groups = {HIVE_LIST_CACHING})
    public void testSymlinkTableListCaching()
    {
        String jmxMetricsQuery = "SELECT sum(hitcount), sum(misscount) from jmx.current.\"com.facebook.presto.hive:name=hivecached,type=cachingdirectorylister\"";
        String symlinkTableQuery = "SELECT * FROM hivecached.default.hive_symlink_table";

        // Initial cache entries, hitcount, misscount will all be zero
        QueryResult queryResult = query(jmxMetricsQuery);
        long hitCountBefore = (long) queryResult.row(0).get(0);
        long missCountBefore = (long) queryResult.row(0).get(1);

        for (int i = 0; i < 2; i++) {
            query(symlinkTableQuery);
        }

        QueryResult result = query(jmxMetricsQuery);

        long hitCountAfter = (long) result.row(0).get(0);
        long missCountAfter = (long) result.row(0).get(1);

        assertEquals(hitCountAfter, hitCountBefore + 1);
        assertEquals(missCountAfter, missCountBefore + 1);
    }

    @AfterTestWithContext
    public void tearDown()
    {
        String dataDir = warehouseDirectoryPath + "/data_hive_symlink_table";
        onHive().executeQuery("DROP TABLE IF EXISTS hive_symlink_table");
        hdfsClient.delete(dataDir);
    }

    private void saveResourceOnHdfs(String resource, String location)
            throws IOException
    {
        hdfsClient.delete(location);
        try (InputStream inputStream = getResource(Paths.get("com/facebook/presto/tests/hive/data/single_int_column/", resource).toString()).openStream()) {
            hdfsClient.saveFile(location, inputStream);
        }
    }
}
