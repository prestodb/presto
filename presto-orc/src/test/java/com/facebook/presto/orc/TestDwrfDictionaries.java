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
package com.facebook.presto.orc;

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageSourceOptions;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;

import static com.facebook.presto.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static com.facebook.presto.orc.OrcReader.MAX_BATCH_SIZE;
import static com.facebook.presto.orc.OrcTester.HIVE_STORAGE_TIME_ZONE;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.io.Resources.getResource;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static org.testng.Assert.assertEquals;

public class TestDwrfDictionaries
{
    @Test
    public void testSlice()
            throws Exception
    {
        Block expected = readFile("test_dictionaries/string-dictionary.orc", VARCHAR);
        Block actual = readFile("test_dictionaries/string-row-group-dictionary.dwrf", VARCHAR);
        assertEqualBlocks(VARCHAR, expected, actual);
    }

    @Test
    public void testLong()
            throws Exception
    {
        Block expected = readFile("test_dictionaries/long-direct.orc", BIGINT);
        Block actual = readFile("test_dictionaries/long-dictionary.dwrf", BIGINT);
        assertEqualBlocks(BIGINT, expected, actual);
    }

    private static void assertEqualBlocks(Type type, Block expected, Block actual)
    {
        assertEquals(expected.getPositionCount(), actual.getPositionCount());
        for (int i = 0; i < expected.getPositionCount(); i++) {
            assertEquals(actual.isNull(i), expected.isNull(i));
            if (!expected.isNull(i)) {
                assertEquals(type.compareTo(expected, i, actual, i), 0);
            }
        }
    }

    private Block readFile(String testOrcFileName, Type type)
            throws IOException
    {
        OrcEncoding encoding = testOrcFileName.contains(".orc") ? OrcEncoding.ORC : OrcEncoding.DWRF;
        File file = new File(getResource(testOrcFileName).getFile());
        try (OrcRecordReader recordReader = createCustomOrcRecordReader(file, encoding, type, MAX_BATCH_SIZE)) {
            PageSourceOptions options = new PageSourceOptions(
                        new int[] {0},
                        new int[] {0},
                        true,
                        512 * 1024);
            recordReader.pushdownFilterAndProjection(options, new int[] {0}, ImmutableList.of(type), new Block[1]);

            assertEquals(recordReader.getReaderPosition(), 0);
            assertEquals(recordReader.getFilePosition(), 0);
            BlockBuilder builder = type.createBlockBuilder(null, 61000);
            while (true) {
                Page page = recordReader.getNextPage();
                if (page == null) {
                    break;
                }
                Block block = page.getBlock(0);
                int count = block.getPositionCount();
                for (int i = 0; i < count; i++) {
                    block.writePositionTo(i, builder);
                }
            }
            return builder.build();
        }
    }

    private static OrcRecordReader createCustomOrcRecordReader(File file, OrcEncoding orcEncoding, Type type, int initialBatchSize)
            throws IOException
    {
        OrcDataSource orcDataSource = new FileOrcDataSource(file, new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), true);
        OrcReader orcReader = new OrcReader(orcDataSource, orcEncoding, new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE));

        return orcReader.createRecordReader(ImmutableMap.of(0, type), OrcPredicate.TRUE, HIVE_STORAGE_TIME_ZONE, newSimpleAggregatedMemoryContext(), initialBatchSize);
    }
}
