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

import com.facebook.presto.plugin.turbonium.encodings.LongEncoder;
import com.facebook.presto.plugin.turbonium.stats.LongStatsBuilder;
import com.facebook.presto.plugin.turbonium.stats.Stats;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.ValueSet;
import com.facebook.presto.spi.type.Type;
import org.openjdk.jol.info.ClassLayout;

import java.util.BitSet;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.plugin.turbonium.storage.Util.createDomain;
import static io.airlift.slice.SizeOf.sizeOf;

public class LongSegments
{
    private LongSegments() {}

    public static class Rle
        implements Segment
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(Rle.class).instanceSize();
        private final int size;
        private final long value;
        private final Domain domain;

        public Rle(Type type, Stats<Long> stats)
        {
            this.size = stats.size();
            this.value = stats.getSingleValue().get();
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
            blockBuilder.writeLong(value);
        }

        @Override
        public long getSizeBytes()
        {
            return INSTANCE_SIZE;
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
        private final long value;
        private final Domain domain;

        public RleWithNulls(Type type, BitSet isNull, Stats<Long> stats)
        {
            super(type, isNull, stats.size());
            this.value = stats.getSingleValue().get();
            this.domain = Domain.create(ValueSet.of(type, value), true);
        }

        @Override
        protected void writeValue(BlockBuilder blockBuilder, int position)
        {
            blockBuilder.writeLong(value);
        }

        @Override
        public long getSizeBytes()
        {
            return INSTANCE_SIZE + isNullSizeBytes();
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
        private final long[] dictionary;
        private final byte[] values;
        private final Domain domain;

        public Dictionary(Type type, BitSet isNull, Stats<Long> stats)
        {
            super(type, isNull, stats.size());
            Map<Long, List<Integer>> distinctValues = stats.getDistinctValues().get();
            dictionary = new long[distinctValues.size()];
            values = new byte[stats.size()];
            int dictionaryId = 0;
            for (Map.Entry<Long, List<Integer>> entry : distinctValues.entrySet()) {
                dictionary[dictionaryId] = entry.getKey();
                for (int position : entry.getValue()) {
                    values[position] = (byte) dictionaryId;
                }
                dictionaryId++;
            }
            this.domain = createDomain(type, stats);
        }

        @Override
        protected void writeValue(BlockBuilder blockBuilder, int position)
        {
            blockBuilder.writeLong(dictionary[values[position] & 0xff]);
        }

        @Override
        public long getSizeBytes()
        {
            return INSTANCE_SIZE + isNullSizeBytes() + sizeOf(dictionary) + sizeOf(values);
        }

        @Override
        public Domain getDomain()
        {
            return domain;
        }
    }

    public static class SortedDictionary
        extends AbstractSegment
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(SortedDictionary.class).instanceSize();
        private final long[] dictionary;
        private final byte[] values;
        private final Domain domain;

        public SortedDictionary(Type type, BitSet isNull, Stats<Long> stats)
        {
            super(type, isNull, stats.size());
            Map<Long, List<Integer>> distinctValues = stats.getDistinctValues().get();
            dictionary = new long[distinctValues.size()];
            values = new byte[stats.size()];
            int dictionaryId = 0;
            for (Iterator<Map.Entry<Long, List<Integer>>> iterator = distinctValues.entrySet().stream()
                    .sorted(Comparator.comparing(Map.Entry::getKey)).iterator(); iterator.hasNext(); ) {
                Map.Entry<Long, List<Integer>> entry = iterator.next();
                dictionary[dictionaryId] = entry.getKey();
                for (int position : entry.getValue()) {
                    values[position] = (byte) dictionaryId;
                }
                dictionaryId++;
            }
            this.domain = createDomain(type, stats);
        }

        @Override
        protected void writeValue(BlockBuilder blockBuilder, int position)
        {
            blockBuilder.writeLong(dictionary[values[position] & 0xff]);
        }

        @Override
        public long getSizeBytes()
        {
            return INSTANCE_SIZE + isNullSizeBytes() + sizeOf(dictionary) + sizeOf(values);
        }

        @Override
        public Domain getDomain()
        {
            return domain;
        }
    }

    public static class Delta
        extends AbstractSegment
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(Delta.class).instanceSize();
        private final long offset;
        private final Values values;
        private final Domain domain;

        public Delta(Type type, BitSet isNull, Stats<Long> stats, Values values)
        {
            super(type, isNull, stats.size());
            this.offset = stats.getMin().get();
            this.values = values;
            this.domain = createDomain(type, stats);
        }

        @Override
        protected void writeValue(BlockBuilder blockBuilder, int position)
        {
            blockBuilder.writeLong(offset + values.getLong(position));
        }

        @Override
        public long getSizeBytes()
        {
            return INSTANCE_SIZE + isNullSizeBytes() + values.getSizeBytes();
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
        private final long[] values;
        private final Domain domain;

        public AllValues(Type type, BitSet isNull, Stats<Long> stats, long[] values)
        {
            super(type, isNull, stats.size());
            this.values = values;
            this.domain = createDomain(type, stats);
        }

        @Override
        protected void writeValue(BlockBuilder blockBuilder, int position)
        {
            blockBuilder.writeLong(values[position]);
        }

        @Override
        public long getSizeBytes()
        {
            return INSTANCE_SIZE + isNullSizeBytes() + sizeOf(values);
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
        private final long[] values = new long[DEFAULT_SEGMENT_SIZE];
        private int internalPosition;
        private final BitSet isNull = new BitSet(DEFAULT_SEGMENT_SIZE);
        private final LongStatsBuilder statsBuilder = new LongStatsBuilder();

        private Builder(int channel, Type type, boolean disableEncoding)
        {
            super(channel, type, disableEncoding);
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

        private void appendValue(Block block, int position)
        {
            long extractedValue = getType().getLong(block, position);
            values[internalPosition] = extractedValue;
            statsBuilder.add(extractedValue, internalPosition);
        }

        @Override
        public int size()
        {
            return internalPosition;
        }

        @Override
        public Segment build()
        {
            return new LongEncoder(getDisableEncoding(), statsBuilder.build(), getType(), isNull, values).encode();
        }
    }
}
