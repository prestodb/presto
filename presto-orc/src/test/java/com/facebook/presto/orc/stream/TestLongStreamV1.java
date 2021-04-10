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
package com.facebook.presto.orc.stream;

import com.facebook.presto.orc.OrcCorruptionException;
import com.facebook.presto.orc.OrcDecompressor;
import com.facebook.presto.orc.TestingHiveOrcAggregatedMemoryContext;
import com.facebook.presto.orc.checkpoint.LongStreamCheckpoint;
import com.facebook.presto.orc.metadata.CompressionParameters;
import io.airlift.slice.Slice;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Random;

import static com.facebook.presto.orc.OrcDecompressor.createOrcDecompressor;
import static com.facebook.presto.orc.metadata.CompressionKind.SNAPPY;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.DATA;

public class TestLongStreamV1
        extends AbstractTestValueStream<Long, LongStreamCheckpoint, LongOutputStreamV1, LongInputStreamV1>
{
    @Test
    public void test()
            throws IOException
    {
        List<List<Long>> groups = new ArrayList<>();
        List<Long> group;

        group = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            group.add((long) (i));
        }
        groups.add(group);

        group = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            group.add((long) (10_000 + (i * 17)));
        }
        groups.add(group);

        group = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            group.add((long) (10_000 - (i * 17)));
        }
        groups.add(group);

        group = new ArrayList<>();
        Random random = new Random(22);
        for (int i = 0; i < 1000; i++) {
            group.add(-1000L + random.nextInt(17));
        }
        groups.add(group);

        testWriteValue(groups);
    }

    @Override
    protected LongOutputStreamV1 createValueOutputStream()
    {
        CompressionParameters compressionParameters = new CompressionParameters(
                SNAPPY,
                OptionalInt.empty(),
                COMPRESSION_BLOCK_SIZE);
        return new LongOutputStreamV1(compressionParameters, Optional.empty(), true, DATA);
    }

    @Override
    protected void writeValue(LongOutputStreamV1 outputStream, Long value)
    {
        outputStream.writeLong(value);
    }

    @Override
    protected LongInputStreamV1 createValueStream(Slice slice)
            throws OrcCorruptionException
    {
        Optional<OrcDecompressor> orcDecompressor = createOrcDecompressor(ORC_DATA_SOURCE_ID, SNAPPY, COMPRESSION_BLOCK_SIZE);
        TestingHiveOrcAggregatedMemoryContext aggregatedMemoryContext = new TestingHiveOrcAggregatedMemoryContext();
        OrcInputStream input = new OrcInputStream(
                ORC_DATA_SOURCE_ID,
                new SharedBuffer(aggregatedMemoryContext.newOrcLocalMemoryContext("sharedDecompressionBuffer")),
                slice.getInput(),
                orcDecompressor,
                Optional.empty(),
                aggregatedMemoryContext,
                slice.getRetainedSize());
        return new LongInputStreamV1(input, true);
    }

    @Override
    protected Long readValue(LongInputStreamV1 valueStream)
            throws IOException
    {
        return valueStream.next();
    }
}
