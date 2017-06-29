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
package com.facebook.presto.plugin.turbonium.storage;

import com.facebook.presto.plugin.turbonium.encodings.SliceEncoder;
import com.facebook.presto.plugin.turbonium.stats.SliceStatsBuilder;
import com.facebook.presto.plugin.turbonium.stats.Stats;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.ValueSet;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.Slice;
import org.openjdk.jol.info.ClassLayout;

import java.util.BitSet;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.plugin.turbonium.storage.Util.createDomain;
import static io.airlift.slice.SizeOf.sizeOf;

public class SliceSegments
{
    private SliceSegments() {}

    public static class Rle
            implements Segment
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(Rle.class).instanceSize();
        private final int size;
        private final Slice value;
        private final Domain domain;
        private final Type type;

        public Rle(Type type, Stats<Slice> stats)
        {
            this.size = stats.size();
            this.value = stats.getSingleValue().get();
            this.type = type;
            this.domain = Domain.singleValue(type, value);
        }

        @Override
        public int size()
        {
            return size;
        }

        @Override
        public void write(BlockBuilder blockBuilder, int position)
        {
            type.writeSlice(blockBuilder, value);
        }

        @Override
        public long getSizeBytes()
        {
            return INSTANCE_SIZE + value.getRetainedSize();
        }

        @Override
        public Domain getDomain()
        {
            return domain;
        }
    }

    public static class RleWithNulls
            extends AbstractSegment
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(RleWithNulls.class).instanceSize();
        private final Slice value;
        private final Domain domain;

        public RleWithNulls(Type type, BitSet isNull, Stats<Slice> stats)
        {
            super(type, isNull, stats.size());
            this.value = stats.getSingleValue().get();
            this.domain = Domain.create(ValueSet.of(type, value), true);
        }

        @Override
        protected void writeValue(BlockBuilder blockBuilder, int position)
        {
            getType().writeSlice(blockBuilder, value);
        }

        @Override
        public long getSizeBytes()
        {
            return INSTANCE_SIZE + isNullSizeBytes() + value.getRetainedSize();
        }

        @Override
        public Domain getDomain()
        {
            return domain;
        }
    }

    public static class Dictionary
            extends AbstractSegment
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(Dictionary.class).instanceSize();
        private final Slice[] dictionary;
        private final byte[] values;
        private final Domain domain;
        private final long sizeOfSlices;

        public Dictionary(Type type, BitSet isNull, Stats<Slice> stats)
        {
            super(type, isNull, stats.size());
            long sizeOf = 0L;
            Map<Slice, List<Integer>> distinctValues = stats.getDistinctValues().get();
            dictionary = new Slice[distinctValues.size()];
            values = new byte[stats.size()];
            int dictionaryId = 0;
            for (Map.Entry<Slice, List<Integer>> entry : distinctValues.entrySet()) {
                dictionary[dictionaryId] = entry.getKey();
                sizeOf += entry.getKey().length();
                for (int position : entry.getValue()) {
                    values[position] = (byte) dictionaryId;
                }
                dictionaryId++;
            }
            sizeOfSlices = sizeOf;
            this.domain = createDomain(type, stats);
        }

        @Override
        protected void writeValue(BlockBuilder blockBuilder, int position)
        {
            getType().writeSlice(blockBuilder, dictionary[values[position] & 0xff]);
        }

        @Override
        public long getSizeBytes()
        {
            return INSTANCE_SIZE + isNullSizeBytes() + sizeOf(values) + sizeOfSlices;
        }

        @Override
        public Domain getDomain()
        {
            return domain;
        }
    }

    public static class AllValues
        extends AbstractSegment
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(AllValues.class).instanceSize();
        private final Slice[] values;
        private final Domain domain;
        private final long sizeOfSlices;

        public AllValues(Type type, BitSet isNull, Stats<Slice> stats, Slice[] values)
        {
            super(type, isNull, stats.size());
            this.values = values;
            this.domain = createDomain(type, stats);
            long sizeOf = 0L;
            for (Slice value : values) {
                if (value != null) {
                    sizeOf += value.length();
                }
            }
            sizeOfSlices = sizeOf;
        }

        @Override
        protected void writeValue(BlockBuilder blockBuilder, int position)
        {
            getType().writeSlice(blockBuilder, values[position]);
        }

        @Override
        public long getSizeBytes()
        {
            return INSTANCE_SIZE + isNullSizeBytes() + sizeOf(values) + sizeOfSlices;
        }

        @Override
        public Domain getDomain()
        {
            return domain;
        }
    }

    public static Builder builder(int channel, Type type, boolean disableEncoding)
    {
        return new Builder(channel, type, disableEncoding);
    }

    public static class Builder
            extends AbstractSegmentBuilder
    {
        private final Slice[] values = new Slice[DEFAULT_SEGMENT_SIZE];
        private int internalPosition;
        private final BitSet isNull = new BitSet(DEFAULT_SEGMENT_SIZE);
        private final SliceStatsBuilder statsBuilder = new SliceStatsBuilder();
        Builder(int channel, Type type, boolean disableEncoding)
        {
            super(channel, type, disableEncoding);
        }

        private void appendValue(Block block, int position)
        {
            Slice extractedValue = getType().getSlice(block, position);
            values[internalPosition] = extractedValue;
            statsBuilder.add(extractedValue, internalPosition);
        }

        @Override
        public void append(Block block, int position)
        {
            if (block.isNull(position)) {
                isNull.set(internalPosition);
                statsBuilder.add(null, internalPosition);
            }
            else {
                appendValue(block, position);
            }
            internalPosition++;
        }

        @Override
        public int size()
        {
            return internalPosition;
        }

        @Override
        public Segment build()
        {
            return new SliceEncoder(getDisableEncoding(), statsBuilder.build(), getType(), isNull, values).encode();
        }
    }
}
