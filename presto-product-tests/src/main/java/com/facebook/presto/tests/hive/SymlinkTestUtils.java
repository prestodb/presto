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

import io.prestodb.tempto.hadoop.hdfs.HdfsClient;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.List;

import static com.facebook.presto.tests.utils.QueryExecutors.onHive;
import static com.google.common.io.Resources.getResource;
import static java.lang.String.format;

public class SymlinkTestUtils
{
    private SymlinkTestUtils()
    {
    }

    public static void createParquetSymlinkTable(
            HdfsClient hdfsClient,
            String table,
            String warehouseDirectoryPath,
            boolean multipleParents)
            throws IOException
    {
        onHive().executeQuery("DROP TABLE IF EXISTS " + table);

        onHive().executeQuery("" +
                "CREATE TABLE " + table +
                "(col int) " +
                "ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' " +
                "STORED AS " +
                "INPUTFORMAT 'org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat' " +
                "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat' ");

        String tableRoot = warehouseDirectoryPath + "/" + table;
        String dataDir = warehouseDirectoryPath + "/data_" + table;

        // Save a parquet file
        saveResourceOnHdfs(hdfsClient, "data.parquet", dataDir + "/data.parquet");

        if (multipleParents) {
            String secondDataDir = warehouseDirectoryPath + "/secondData_" + table;
            saveResourceOnHdfs(hdfsClient, "data.parquet", secondDataDir + "/data.parquet");

            // Save Symlink Manifest
            hdfsClient.saveFile(
                    tableRoot + "/manifest",
                    format("hdfs://hadoop-master:9000%s/data.parquet\nhdfs://hadoop-master:9000%s/data.parquet", dataDir, secondDataDir));
        }
        else {
            // Save Symlink Manifest
            hdfsClient.saveFile(tableRoot + "/manifest", format("hdfs://hadoop-master:9000%s/data.parquet", dataDir));
        }
    }

    public static void deleteSymlinkTable(HdfsClient hdfsClient, String table, List<String> dataDirs)
    {
        onHive().executeQuery("DROP TABLE IF EXISTS " + table);

        dataDirs.forEach(hdfsClient::delete);
    }

    public static void saveResourceOnHdfs(HdfsClient hdfsClient, String resource, String location)
            throws IOException
    {
        hdfsClient.delete(location);
        try (InputStream inputStream = getResource(Paths.get("com/facebook/presto/tests/hive/data/single_int_column/", resource).toString()).openStream()) {
            hdfsClient.saveFile(location, inputStream);
        }
    }
}
