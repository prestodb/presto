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
import com.facebook.presto.orc.checkpoint.FloatStreamCheckpoint;
import com.facebook.presto.orc.metadata.CompressionParameters;
import io.airlift.slice.Slice;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static com.facebook.presto.orc.OrcDecompressor.createOrcDecompressor;
import static com.facebook.presto.orc.metadata.CompressionKind.SNAPPY;

public class TestFloatStream
        extends AbstractTestValueStream<Float, FloatStreamCheckpoint, FloatOutputStream, FloatInputStream>
{
    @Test
    public void test()
            throws IOException
    {
        List<List<Float>> groups = new ArrayList<>();
        for (int groupIndex = 0; groupIndex < 3; groupIndex++) {
            List<Float> group = new ArrayList<>();
            for (int i = 0; i < 1000; i++) {
                group.add((float) (groupIndex * 10_000 + i));
            }
            groups.add(group);
        }
        testWriteValue(groups);
    }

    @Override
    protected FloatOutputStream createValueOutputStream()
    {
        CompressionParameters compressionParameters = new CompressionParameters(SNAPPY, OptionalInt.empty(), COMPRESSION_BLOCK_SIZE);
        return new FloatOutputStream(compressionParameters, Optional.empty());
    }

    @Override
    protected void writeValue(FloatOutputStream outputStream, Float value)
    {
        outputStream.writeFloat(value);
    }

    @Override
    protected FloatInputStream createValueStream(Slice slice)
            throws OrcCorruptionException
    {
        Optional<OrcDecompressor> orcDecompressor = createOrcDecompressor(ORC_DATA_SOURCE_ID, SNAPPY, COMPRESSION_BLOCK_SIZE);
        TestingHiveOrcAggregatedMemoryContext aggregatedMemoryContext = new TestingHiveOrcAggregatedMemoryContext();
        return new FloatInputStream(new OrcInputStream(
                ORC_DATA_SOURCE_ID,
                new SharedBuffer(aggregatedMemoryContext.newOrcLocalMemoryContext("sharedDecompressionBuffer")),
                slice.getInput(),
                orcDecompressor,
                Optional.empty(),
                aggregatedMemoryContext,
                slice.getRetainedSize()));
    }

    @Override
    protected Float readValue(FloatInputStream valueStream)
            throws IOException
    {
        return valueStream.next();
    }
}
