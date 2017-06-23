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
import com.facebook.presto.orc.checkpoint.BooleanStreamCheckpoint;
import com.facebook.presto.orc.memory.AggregatedMemoryContext;
import io.airlift.slice.Slice;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.orc.OrcDecompressor.createOrcDecompressor;
import static com.facebook.presto.orc.OrcWriter.DEFAULT_BUFFER_SIZE;
import static com.facebook.presto.orc.metadata.CompressionKind.SNAPPY;

public class TestBooleanStream
        extends AbstractTestValueStream<Boolean, BooleanStreamCheckpoint, BooleanOutputStream, BooleanInputStream>
{
    @Test
    public void test()
            throws IOException
    {
        List<List<Boolean>> groups = new ArrayList<>();
        for (int groupIndex = 0; groupIndex < 3; groupIndex++) {
            List<Boolean> group = new ArrayList<>();
            for (int i = 0; i < 1000; i++) {
                group.add(i % 3 == 0);
            }
            groups.add(group);
        }
        List<Boolean> group = new ArrayList<>();
        for (int i = 0; i < 17; i++) {
            group.add(i % 3 == 0);
        }
        groups.add(group);
        testWriteValue(groups);
    }

    @Override
    protected BooleanOutputStream createValueOutputStream()
    {
        return new BooleanOutputStream(SNAPPY, DEFAULT_BUFFER_SIZE);
    }

    @Override
    protected void writeValue(BooleanOutputStream outputStream, Boolean value)
    {
        outputStream.writeBoolean(value);
    }

    @Override
    protected BooleanInputStream createValueStream(Slice slice)
            throws OrcCorruptionException
    {
        Optional<OrcDecompressor> orcDecompressor = createOrcDecompressor(ORC_DATA_SOURCE_ID, SNAPPY, DEFAULT_BUFFER_SIZE);
        return new BooleanInputStream(new OrcInputStream(ORC_DATA_SOURCE_ID, slice.getInput(), orcDecompressor, new AggregatedMemoryContext()));
    }

    @Override
    protected Boolean readValue(BooleanInputStream valueStream)
            throws IOException
    {
        return valueStream.nextBit();
    }
}
