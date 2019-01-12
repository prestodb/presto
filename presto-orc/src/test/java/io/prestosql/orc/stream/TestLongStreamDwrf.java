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
package io.prestosql.orc.stream;

import io.airlift.slice.Slice;
import io.prestosql.orc.OrcCorruptionException;
import io.prestosql.orc.OrcDecompressor;
import io.prestosql.orc.checkpoint.LongStreamCheckpoint;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static io.prestosql.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.prestosql.orc.OrcDecompressor.createOrcDecompressor;
import static io.prestosql.orc.metadata.CompressionKind.SNAPPY;
import static io.prestosql.orc.metadata.OrcType.OrcTypeKind.LONG;
import static io.prestosql.orc.metadata.Stream.StreamKind.DATA;

public class TestLongStreamDwrf
        extends AbstractTestValueStream<Long, LongStreamCheckpoint, LongOutputStreamDwrf, LongInputStreamDwrf>
{
    @Test
    public void test()
            throws IOException
    {
        List<List<Long>> groups = new ArrayList<>();
        for (int groupIndex = 0; groupIndex < 3; groupIndex++) {
            List<Long> group = new ArrayList<>();
            for (int i = 0; i < 1000; i++) {
                group.add((long) (groupIndex * 10_000 + i));
            }
            groups.add(group);
        }
        testWriteValue(groups);
    }

    @Override
    protected LongOutputStreamDwrf createValueOutputStream()
    {
        return new LongOutputStreamDwrf(SNAPPY, COMPRESSION_BLOCK_SIZE, true, DATA);
    }

    @Override
    protected void writeValue(LongOutputStreamDwrf outputStream, Long value)
    {
        outputStream.writeLong(value);
    }

    @Override
    protected LongInputStreamDwrf createValueStream(Slice slice)
            throws OrcCorruptionException
    {
        Optional<OrcDecompressor> orcDecompressor = createOrcDecompressor(ORC_DATA_SOURCE_ID, SNAPPY, COMPRESSION_BLOCK_SIZE);
        OrcInputStream input = new OrcInputStream(ORC_DATA_SOURCE_ID, slice.getInput(), orcDecompressor, newSimpleAggregatedMemoryContext(), slice.getRetainedSize());
        return new LongInputStreamDwrf(input, LONG, true, true);
    }

    @Override
    protected Long readValue(LongInputStreamDwrf valueStream)
            throws IOException
    {
        return valueStream.next();
    }
}
