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

import com.facebook.presto.plugin.turbonium.encodings.BooleanEncoder;
import com.facebook.presto.plugin.turbonium.stats.BooleanStatsBuilder;
import com.facebook.presto.plugin.turbonium.stats.Stats;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.ValueSet;
import com.facebook.presto.spi.type.Type;
import org.openjdk.jol.info.ClassLayout;

import java.util.BitSet;

import static com.facebook.presto.plugin.turbonium.storage.Util.createDomain;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static io.airlift.slice.SizeOf.sizeOf;

public class BooleanSegments
{
    private BooleanSegments() {}

    public static class Rle
        implements Segment
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(Rle.class).instanceSize();
        private final int size;
        private final boolean value;
        private final Domain domain;

        public Rle(Type type, Stats<Boolean> stats)
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
            BOOLEAN.writeBoolean(blockBuilder, value);
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
        private final boolean value;
        private final Domain domain;

        public RleWithNulls(Type type, BitSet isNull, Stats<Boolean> stats)
        {
            super(type, isNull, stats.size());
            this.value = stats.getSingleValue().get();
            this.domain = Domain.create(ValueSet.of(type, value), true);
        }

        @Override
        protected void writeValue(BlockBuilder blockBuilder, int position)
        {
            BOOLEAN.writeBoolean(blockBuilder, value);
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

    public static class AllValues
            extends AbstractSegment
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(AllValues.class).instanceSize();
        private final boolean[] values;
        private final Domain domain;

        public AllValues(Type type, BitSet isNull, Stats<Boolean> stats, boolean[] values)
        {
            super(type, isNull, stats.size());
            this.values = values;
            this.domain = createDomain(type, stats);
        }

        @Override
        protected void writeValue(BlockBuilder blockBuilder, int position)
        {
            BOOLEAN.writeBoolean(blockBuilder, values[position]);
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
        private final boolean[] values = new boolean[DEFAULT_SEGMENT_SIZE];
        private int internalPosition;
        private final BitSet isNull = new BitSet(DEFAULT_SEGMENT_SIZE);
        private final BooleanStatsBuilder statsBuilder = new BooleanStatsBuilder();

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
            boolean extractedValue = getType().getBoolean(block, position);
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
            return new BooleanEncoder(getDisableEncoding(), statsBuilder.build(), getType(), isNull, values).encode();
        }
    }
}
