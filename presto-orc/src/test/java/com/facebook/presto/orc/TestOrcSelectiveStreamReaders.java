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

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.orc.metadata.CompressionKind;
import com.facebook.presto.orc.reader.SelectiveStreamReader;
import com.facebook.presto.orc.reader.SliceSelectiveStreamReader;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.IntStream;

import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.orc.OrcTester.Format.DWRF;
import static com.facebook.presto.orc.OrcTester.Format.ORC_11;
import static com.facebook.presto.orc.OrcTester.Format.ORC_12;
import static com.facebook.presto.orc.OrcTester.assertBlockEquals;
import static com.facebook.presto.orc.OrcTester.createCustomOrcSelectiveRecordReader;
import static com.facebook.presto.orc.OrcTester.writeOrcColumnsPresto;
import static com.facebook.presto.orc.TestingOrcPredicate.createOrcPredicate;
import static com.facebook.presto.orc.metadata.CompressionKind.LZ4;
import static com.facebook.presto.orc.metadata.CompressionKind.NONE;
import static com.facebook.presto.orc.metadata.CompressionKind.SNAPPY;
import static com.facebook.presto.orc.metadata.CompressionKind.ZLIB;
import static com.facebook.presto.orc.metadata.CompressionKind.ZSTD;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.lang.Math.min;
import static org.testng.Assert.assertEquals;

public class TestOrcSelectiveStreamReaders
{
    private Set<OrcTester.Format> formats = ImmutableSet.of(ORC_12, ORC_11, DWRF);
    private Set<CompressionKind> compressions = ImmutableSet.of(NONE, SNAPPY, ZLIB, LZ4, ZSTD);

    /**
     * This test tests SliceDirectSelectiveStreamReader for the case where all elements to read are empty strings. The output Block should be a valid VariableWidthBlock with an
     * empty Slice. It is to simulate a problem seen in production. The state of SliceDirectSelectiveStreamReader to reproduce the problem is:
     * - dataStream: null
     * - presentStream: null
     * - lengthStream: not null
     * - filter: null
     * - outputRequired: true
     * - offsets array: non zeros
     * The test issues two reads, the first one reads a non-empty string and populates non-zero offsets. The second one reads the empty string with the above conditions met.
     */
    @Test
    public void testEmptyStrings()
            throws Exception
    {
        Type type = VARCHAR;
        List<Type> types = ImmutableList.of(type);
        List<List<?>> values = ImmutableList.of(ImmutableList.of("a", ""));

        for (OrcTester.Format format : formats) {
            if (!types.stream().allMatch(readType -> format.supportsType(readType))) {
                return;
            }

            for (CompressionKind compression : compressions) {
                TempFile tempFile = new TempFile();
                writeOrcColumnsPresto(tempFile.getFile(), format, compression, Optional.empty(), types, values, new OrcWriterStats());

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
                        format.getOrcEncoding(),
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
                        systemMemoryUsage)) {
                    assertEquals(recordReader.getReaderPosition(), 0);
                    assertEquals(recordReader.getFilePosition(), 0);

                    SelectiveStreamReader streamReader = recordReader.getStreamReaders()[0];

                    // Read the first non-empty element. Do not call streamReader.getBlock() to preserve the offsets array in SliceDirectSelectiveStreamReader.
                    int batchSize = min(recordReader.prepareNextBatch(), 1);
                    int[] positions = IntStream.range(0, batchSize).toArray();
                    streamReader.read(0, positions, batchSize);
                    recordReader.batchRead(batchSize);

                    // Read the second element: an empty string. Set the dataStream in SliceDirectSelectiveStreamReader to null to simulate the conditions causing the problem.
                    ((SliceSelectiveStreamReader) streamReader).resetDataStream();
                    batchSize = min(recordReader.prepareNextBatch(), 1);
                    positions = IntStream.range(0, batchSize).toArray();
                    streamReader.read(0, positions, batchSize);
                    recordReader.batchRead(batchSize);

                    Block block = streamReader.getBlock(positions, batchSize);

                    List<?> expectedValues = ImmutableList.of("");
                    assertBlockEquals(type, block, expectedValues, 0);
                    assertEquals(recordReader.getReaderPosition(), 1);
                    assertEquals(recordReader.getFilePosition(), 1);
                }
            }
        }
    }
}
