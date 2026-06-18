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

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.IntStream;

import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.orc.NoOpOrcWriterStats.NOOP_WRITER_STATS;
import static com.facebook.presto.orc.OrcTester.Format.DWRF;
import static com.facebook.presto.orc.OrcTester.Format.ORC_12;
import static com.facebook.presto.orc.OrcTester.createCustomOrcSelectiveRecordReader;
import static com.facebook.presto.orc.OrcTester.writeOrcColumnsPresto;
import static com.facebook.presto.orc.TestingOrcPredicate.createOrcPredicate;
import static com.facebook.presto.orc.metadata.CompressionKind.NONE;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static org.testng.Assert.assertEquals;

public class TestOrcSelectiveRecordReader
{
    @Test
    public void testGetNextPage_withRowNumbers()
            throws Exception
    {
        List<Type> types = ImmutableList.of(VARCHAR);
        List<List<?>> values = ImmutableList.of(ImmutableList.of("a", ""));

        TempFile tempFile = new TempFile();
        writeOrcColumnsPresto(tempFile.getFile(), ORC_12, NONE, Optional.empty(), types, values, NOOP_WRITER_STATS);

        OrcPredicate orcPredicate = createOrcPredicate(types, values, DWRF, false);
        Map<Integer, Type> includedColumns = IntStream.range(0, types.size())
                .boxed()
                .collect(toImmutableMap(Function.identity(), types::get));
        List<Integer> outputColumns = IntStream.range(0, types.size())
                .boxed()
                .collect(toImmutableList());
        OrcAggregatedMemoryContext systemMemoryUsage = new TestingHiveOrcAggregatedMemoryContext();
        try (OrcSelectiveRecordReader recordReader = createCustomOrcSelectiveRecordReader(
                tempFile.getFile(),
                ORC_12.getOrcEncoding(),
                orcPredicate,
                types,
                1,
                ImmutableMap.of(),
                ImmutableList.of(),
                ImmutableMap.of(),
                OrcTester.OrcReaderSettings.builder().build().getRequiredSubfields(),
                ImmutableMap.of(),
                ImmutableMap.of(),
                includedColumns,
                outputColumns,
                false,
                systemMemoryUsage,
                false)) {
            assertEquals(recordReader.getReaderPosition(), 0);
            assertEquals(recordReader.getFilePosition(), 0);

            Page page = recordReader.getNextPage(true);
            // One VARCHAR column and one row number column
            assertEquals(2, page.getChannelCount());
            Block block = page.getBlock(1);
            assertEquals(block.getPositionCount(), 1);
            assertEquals(block.getLong(0), 0, "First row number is not zero");
        }
    }

    @Test
    public void testGetNextPage_withoutRowNumbers()
            throws Exception
    {
        List<Type> types = ImmutableList.of(VARCHAR);
        List<List<?>> values = ImmutableList.of(ImmutableList.of("a", ""));

        TempFile tempFile = new TempFile();
        writeOrcColumnsPresto(tempFile.getFile(), ORC_12, NONE, Optional.empty(), types, values, NOOP_WRITER_STATS);

        OrcPredicate orcPredicate = createOrcPredicate(types, values, DWRF, false);
        Map<Integer, Type> includedColumns = IntStream.range(0, types.size())
                .boxed()
                .collect(toImmutableMap(Function.identity(), types::get));
        List<Integer> outputColumns = IntStream.range(0, types.size())
                .boxed()
                .collect(toImmutableList());
        OrcAggregatedMemoryContext systemMemoryUsage = new TestingHiveOrcAggregatedMemoryContext();
        try (OrcSelectiveRecordReader recordReader = createCustomOrcSelectiveRecordReader(
                tempFile.getFile(),
                ORC_12.getOrcEncoding(),
                orcPredicate,
                types,
                1,
                ImmutableMap.of(),
                ImmutableList.of(),
                ImmutableMap.of(),
                OrcTester.OrcReaderSettings.builder().build().getRequiredSubfields(),
                ImmutableMap.of(),
                ImmutableMap.of(),
                includedColumns,
                outputColumns,
                false,
                systemMemoryUsage,
                false)) {
            assertEquals(recordReader.getReaderPosition(), 0);
            assertEquals(recordReader.getFilePosition(), 0);

            Page page = recordReader.getNextPage();
            // One VARCHAR column, no row number column
            assertEquals(1, page.getChannelCount());
            Block block = page.getBlock(0);
            assertEquals(block.getPositionCount(), 1);
        }
    }
}
