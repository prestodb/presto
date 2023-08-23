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
package com.facebook.presto.parquet.reader;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.parquet.ParquetDataSource;
import com.facebook.presto.parquet.ParquetDataSourceId;
import com.facebook.presto.parquet.cache.MetadataReader;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.schema.MessageType;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.parquet.ParquetTypeUtils.getColumnIO;
import static com.facebook.presto.parquet.ParquetTypeUtils.lookupColumnByName;
import static com.facebook.presto.parquet.reader.TestEncryption.constructField;
import static com.facebook.presto.parquet.reader.TestEncryption.createParquetReader;
import static org.testng.Assert.assertEquals;

public class TestLZ4Decompression
{
    @Test
    public void testHiveGeneratedLZ4()
            throws IOException
    {
        readFileAndValidate("src/test/resources/compressed/lz4/hive_generated.lz4.parquet");
    }

    @Test
    public void testSparkGeneratedLZ4()
            throws IOException
    {
        readFileAndValidate("src/test/resources/compressed/lz4/spark_generated.lz4.parquet");
    }

    private void readFileAndValidate(String parquetFilePath)
            throws IOException
    {
        ImmutableMap.Builder<String, List<Object>> builder = ImmutableMap.builder();
        builder.put("_c0", ImmutableList.of("10000", "10001", "10002"));
        builder.put("_c1", ImmutableList.of("name10000", "name10001", "name10002"));
        builder.put("_c2", ImmutableList.of("2021", "2021", "2021"));
        builder.put("_c3", ImmutableList.of("100", "101", "102"));
        builder.put("_c4", ImmutableList.of("abc", "abd", "abe"));
        Map<String, List<Object>> expected = builder.build();
        Path path = new Path(parquetFilePath);
        FileSystem fileSystem = path.getFileSystem(new Configuration(false));
        try (FSDataInputStream inputStream = fileSystem.open(path)) {
            ParquetDataSource dataSource = new MockParquetDataSource(new ParquetDataSourceId(path.toString()), inputStream);
            ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource, fileSystem.getFileStatus(path).getLen(), Optional.empty(), false).getParquetMetadata();
            FileMetaData fileMetaData = parquetMetadata.getFileMetaData();
            MessageType fileSchema = fileMetaData.getSchema();
            MessageColumnIO messageColumn = getColumnIO(fileSchema, fileSchema);
            ParquetReader parquetReader = createParquetReader(parquetMetadata, messageColumn, dataSource, Optional.empty());
            Map<String, List<Object>> columns = new HashMap<>();
            List<String> columnsToRead = Arrays.asList("_c0", "_c1", "_c2", "_c3", "_c4");
            int batchSize = parquetReader.nextBatch();
            while (batchSize > 0) {
                for (String colName : columnsToRead) {
                    columns.putIfAbsent(colName, new ArrayList<>());
                    Block block = parquetReader.readBlock(constructField(VARCHAR, lookupColumnByName(messageColumn, colName)).orElse(null));
                    for (int rowInBlock = 0; rowInBlock < block.getPositionCount(); rowInBlock++) {
                        String val = block.getSlice(rowInBlock, 0, block.getSliceLength(rowInBlock)).toStringUtf8();
                        columns.get(colName).add(val);
                    }
                }
                batchSize = parquetReader.nextBatch();
            }
            assertEquals(expected, columns);
        }
    }
}
