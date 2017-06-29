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

import com.facebook.presto.plugin.turbonium.encodings.ShortEncoder;
import com.facebook.presto.plugin.turbonium.stats.ShortStatsBuilder;
import com.facebook.presto.plugin.turbonium.stats.Stats;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.ValueSet;
import com.facebook.presto.spi.type.Type;
import com.google.common.primitives.Primitives;
import org.openjdk.jol.info.ClassLayout;

import java.util.BitSet;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.plugin.turbonium.storage.Util.createDomain;
import static io.airlift.slice.SizeOf.sizeOf;

public class ShortSegments
{
    private ShortSegments() {}

    public static class Rle
        implements Segment
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(Rle.class).instanceSize();
        private final int size;
        private final short value;
        private final Domain domain;

        public Rle(Type type, Stats<Short> stats)
        {
            this.size = stats.size();
            this.value = stats.getSingleValue().get();
            this.domain = Domain.singleValue(type, Primitives.wrap(type.getJavaType()).cast(((Number) value).longValue()));
        }

        @Override
        public int size()
        {
            return size;
        }

        @Override
        public void write(BlockBuilder blockBuilder, int position)
        {
            blockBuilder.writeShort(value);
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
        private final short value;
        private final Domain domain;

        public RleWithNulls(Type type, BitSet isNull, Stats<Short> stats)
        {
            super(type, isNull, stats.size());
            this.value = stats.getSingleValue().get();
            this.domain = Domain.create(ValueSet.of(type, Primitives.wrap(type.getJavaType()).cast(((Number) value).longValue())), true);
        }

        @Override
        protected void writeValue(BlockBuilder blockBuilder, int position)
        {
            blockBuilder.writeShort(value);
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
        private final short[] dictionary;
        private final byte[] values;
        private final Domain domain;

        public Dictionary(Type type, BitSet isNull, Stats<Short> stats)
        {
            super(type, isNull, stats.size());
            Map<Short, List<Integer>> distinctValues = stats.getDistinctValues().get();
            dictionary = new short[distinctValues.size()];
            values = new byte[stats.size()];
            int dictionaryId = 0;
            for (Map.Entry<Short, List<Integer>> entry : distinctValues.entrySet()) {
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
            blockBuilder.writeShort(dictionary[values[position] & 0xff]);
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
        private final short[] dictionary;
        private final byte[] values;
        private final Domain domain;

        public SortedDictionary(Type type, BitSet isNull, Stats<Short> stats)
        {
            super(type, isNull, stats.size());
            Map<Short, List<Integer>> distinctValues = stats.getDistinctValues().get();
            dictionary = new short[distinctValues.size()];
            values = new byte[stats.size()];
            int dictionaryId = 0;
            for (Iterator<Map.Entry<Short, List<Integer>>> iterator = distinctValues.entrySet().stream()
                    .sorted(Comparator.comparing(Map.Entry::getKey)).iterator(); iterator.hasNext(); ) {
                Map.Entry<Short, List<Integer>> entry = iterator.next();
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
            blockBuilder.writeShort(dictionary[values[position] & 0xff]);
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
        private final short offset;
        private final Values values;
        private final Domain domain;

        public Delta(Type type, BitSet isNull, Stats<Short> stats, Values values)
        {
            super(type, isNull, stats.size());
            this.offset = stats.getMin().get();
            this.values = values;
            this.domain = createDomain(type, stats);
        }

        @Override
        protected void writeValue(BlockBuilder blockBuilder, int position)
        {
            blockBuilder.writeShort(offset + values.getShort(position));
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
        private final short[] values;
        private final Domain domain;

        public AllValues(Type type, BitSet isNull, Stats<Short> stats, short[] values)
        {
            super(type, isNull, stats.size());
            this.values = values;
            this.domain = createDomain(type, stats);
        }

        @Override
        protected void writeValue(BlockBuilder blockBuilder, int position)
        {
            blockBuilder.writeShort(values[position]);
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
        private final short[] values = new short[DEFAULT_SEGMENT_SIZE];
        private int internalPosition;
        private final BitSet isNull = new BitSet(DEFAULT_SEGMENT_SIZE);
        private final ShortStatsBuilder statsBuilder = new ShortStatsBuilder();

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
            short extractedValue = block.getShort(position, 0);
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
            return new ShortEncoder(getDisableEncoding(), statsBuilder.build(), getType(), isNull, values).encode();
        }
    }
}
