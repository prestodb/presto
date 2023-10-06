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
package com.facebook.presto.parquet.writer;

import com.facebook.presto.common.PageBuilder;
import com.facebook.presto.common.type.Type;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.io.Files.createTempDir;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static java.util.UUID.randomUUID;
import static org.testng.Assert.fail;

public class TestParquetWriter
{
    private File temporaryDirectory;
    private File parquetFile;

    @Test
    public void testRowGroupFlushInterleavedColumnWriterFallbacks()
    {
        temporaryDirectory = createTempDir();
        parquetFile = new File(temporaryDirectory, randomUUID().toString());
        List<Type> types = ImmutableList.of(BIGINT, INTEGER, VARCHAR, BOOLEAN);
        List<String> names = ImmutableList.of("col_1", "col_2", "col_3", "col_4");
        ParquetWriterOptions parquetWriterOptions = ParquetWriterOptions.builder()
                .setMaxPageSize(DataSize.succinctBytes(1000))
                .setMaxBlockSize(DataSize.succinctBytes(15000))
                .setMaxDictionaryPageSize(DataSize.succinctBytes(1000))
                .build();
        try (ParquetWriter parquetWriter = createParquetWriter(parquetFile, types, names, parquetWriterOptions, CompressionCodecName.UNCOMPRESSED)) {
            Random rand = new Random();
            for (int pageIdx = 0; pageIdx < 10; pageIdx++) {
                int pageRowCount = 100;
                PageBuilder pageBuilder = new PageBuilder(pageRowCount, types);
                for (int rowIdx = 0; rowIdx < pageRowCount; rowIdx++) {
                    // maintain col_1's dictionary size approximately half of raw data
                    BIGINT.writeLong(pageBuilder.getBlockBuilder(0), pageIdx * 100 + rand.nextInt(50));
                    INTEGER.writeLong(pageBuilder.getBlockBuilder(1), rand.nextInt(100000000));
                    VARCHAR.writeString(pageBuilder.getBlockBuilder(2), UUID.randomUUID().toString());
                    BOOLEAN.writeBoolean(pageBuilder.getBlockBuilder(3), rand.nextBoolean());
                    pageBuilder.declarePosition();
                }
                parquetWriter.write(pageBuilder.build());
            }
        }
        catch (Exception e) {
            fail("Should not fail, but throw an exception as follows:", e);
        }
    }

    public static ParquetWriter createParquetWriter(File outputFile, List<Type> types, List<String> columnNames,
                                                    ParquetWriterOptions parquetWriterOptions, CompressionCodecName compressionCodecName)
            throws Exception
    {
        checkArgument(types.size() == columnNames.size());
        ParquetSchemaConverter schemaConverter = new ParquetSchemaConverter(
                types,
                columnNames);
        return new ParquetWriter(
                new FileOutputStream(outputFile),
                schemaConverter.getMessageType(),
                schemaConverter.getPrimitiveTypes(),
                columnNames,
                types,
                parquetWriterOptions,
                compressionCodecName.getHadoopCompressionCodecClassName());
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws IOException
    {
        deleteRecursively(temporaryDirectory.toPath(), ALLOW_INSECURE);
    }
}
