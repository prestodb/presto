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
import io.prestosql.orc.checkpoint.DecimalStreamCheckpoint;
import org.testng.annotations.Test;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;

import static io.prestosql.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.prestosql.orc.OrcDecompressor.createOrcDecompressor;
import static io.prestosql.orc.metadata.CompressionKind.SNAPPY;
import static io.prestosql.spi.type.UnscaledDecimal128Arithmetic.unscaledDecimal;

public class TestLongDecimalStream
        extends AbstractTestValueStream<Slice, DecimalStreamCheckpoint, DecimalOutputStream, DecimalInputStream>
{
    @Test
    public void test()
            throws IOException
    {
        Random random = new Random(0);
        List<List<Slice>> groups = new ArrayList<>();
        for (int groupIndex = 0; groupIndex < 3; groupIndex++) {
            List<Slice> group = new ArrayList<>();
            for (int i = 0; i < 1000; i++) {
                BigInteger value = new BigInteger(120, random);
                group.add(unscaledDecimal(value));
            }
            groups.add(group);
        }
        testWriteValue(groups);
    }

    @Override
    protected DecimalOutputStream createValueOutputStream()
    {
        return new DecimalOutputStream(SNAPPY, COMPRESSION_BLOCK_SIZE);
    }

    @Override
    protected void writeValue(DecimalOutputStream outputStream, Slice value)
    {
        outputStream.writeUnscaledValue(value);
    }

    @Override
    protected DecimalInputStream createValueStream(Slice slice)
            throws OrcCorruptionException
    {
        Optional<OrcDecompressor> orcDecompressor = createOrcDecompressor(ORC_DATA_SOURCE_ID, SNAPPY, COMPRESSION_BLOCK_SIZE);
        return new DecimalInputStream(new OrcInputStream(ORC_DATA_SOURCE_ID, slice.getInput(), orcDecompressor, newSimpleAggregatedMemoryContext(), slice.getRetainedSize()));
    }

    @Override
    protected Slice readValue(DecimalInputStream valueStream)
            throws IOException
    {
        Slice decimal = unscaledDecimal();
        valueStream.nextLongDecimal(decimal);
        return decimal;
    }
}
