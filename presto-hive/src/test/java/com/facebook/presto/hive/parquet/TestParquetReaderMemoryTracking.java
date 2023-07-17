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
package com.facebook.presto.hive.parquet;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.parquet.Field;
import com.facebook.presto.parquet.FileParquetDataSource;
import com.facebook.presto.parquet.cache.MetadataReader;
import com.facebook.presto.parquet.reader.ParquetReader;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.ColumnIOConverter;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.schema.MessageType;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

import static com.facebook.airlift.testing.Assertions.assertBetweenInclusive;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.hive.parquet.ParquetTester.writeParquetFileFromPresto;
import static com.facebook.presto.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static com.facebook.presto.parquet.ParquetTypeUtils.getColumnIO;
import static com.google.common.io.Files.createTempDir;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.UUID.randomUUID;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.UNCOMPRESSED;

public class TestParquetReaderMemoryTracking
{
    private static final int ROWS = 1020000;
    private static final boolean ENABLE_OPTIMIZED_READER = true;
    private static final boolean ENABLE_VERIFICATION = true;

    private File temporaryDirectory;
    private File parquetFile;
    private Field field;

    @Test
    public void testParquetReaderMemoryUsage()
            throws Exception
    {
        prepareDataFile(UNCOMPRESSED);

        try (ParquetReader parquetReader = createParquetReader()) {
            List<Block> blocks = new ArrayList<>();

            while (parquetReader.nextBatch() > 0) {
                Block block = parquetReader.readBlock(field);
                assertBetweenInclusive(parquetReader.getSystemMemoryUsage(), 320000L, 370000L);
                blocks.add(block);
            }
        }
    }

    private void prepareDataFile(CompressionCodecName compressionCodecName)
            throws Exception
    {
        temporaryDirectory = createTempDir();
        parquetFile = new File(temporaryDirectory, randomUUID().toString());
        Type type = INTEGER;

        writeParquetFileFromPresto(parquetFile,
                ImmutableList.of(type),
                ImmutableList.of("c1"),
                new Iterable<?>[] {generateValues()},
                ROWS,
                compressionCodecName);
    }

    private List<Integer> generateValues()
    {
        ImmutableList.Builder builder = ImmutableList.builder();
        for (int i = 0; i < ROWS; ++i) {
            builder.add(ThreadLocalRandom.current().nextInt());
        }
        return builder.build();
    }

    private ParquetReader createParquetReader()
            throws IOException
    {
        FileParquetDataSource dataSource = new FileParquetDataSource(parquetFile);
        ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource, parquetFile.length(), Optional.empty(), false).getParquetMetadata();
        MessageType schema = parquetMetadata.getFileMetaData().getSchema();
        MessageColumnIO messageColumnIO = getColumnIO(schema, schema);

        this.field = ColumnIOConverter.constructField(INTEGER, messageColumnIO.getChild(0)).get();

        return new ParquetReader(
                messageColumnIO,
                parquetMetadata.getBlocks(),
                Optional.empty(), dataSource,
                newSimpleAggregatedMemoryContext(),
                new DataSize(16, MEGABYTE),
                ENABLE_OPTIMIZED_READER,
                ENABLE_VERIFICATION,
                null,
                null,
                false,
                Optional.empty());
    }

    @AfterClass(alwaysRun = true)
    private void tearDown()
            throws IOException
    {
        deleteRecursively(temporaryDirectory.toPath(), ALLOW_INSECURE);
    }
}
