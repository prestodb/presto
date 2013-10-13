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
package com.facebook.presto.operator;

import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.tuple.TupleInfo.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.Long2IntOpenCustomHashMap;
import it.unimi.dsi.fastutil.longs.LongHash;
import it.unimi.dsi.fastutil.longs.LongHash.Strategy;

import java.util.Arrays;
import java.util.List;

import static io.airlift.slice.SizeOf.sizeOf;

public class JoinHash
{
    private static final long CURRENT_ROW_ADDRESS = 0xFF_FF_FF_FF_FF_FF_FF_FFL;

    private final PagesIndex pagesIndex;
    private final PagesHashStrategy hashStrategy;
    private final AddressToPositionMap addressToPositionMap;
    private final IntArrayList positionLinks;

    public JoinHash(PagesIndex pagesIndex, List<Integer> hashChannels, OperatorContext operatorContext)
    {
        this.pagesIndex = pagesIndex;
        this.hashStrategy = new PagesHashStrategy(pagesIndex, hashChannels);
        this.addressToPositionMap = new AddressToPositionMap(pagesIndex.getPositionCount(), hashStrategy);
        this.positionLinks = new IntArrayList(new int[pagesIndex.getPositionCount()]);
        Arrays.fill(positionLinks.elements(), -1);

        // index pages
        for (int position = 0; position < pagesIndex.getPositionCount(); position++) {
            operatorContext.setMemoryReservation(getEstimatedSize());

            // address and position are the same thing on this side
            // todo we should be able to do this with a set but fast utils doesn't have anything like that
            int oldPosition = addressToPositionMap.put(position, position);
            if (oldPosition >= 0) {
                // link the new position to the old position
                positionLinks.set(position, oldPosition);
            }
        }
    }

    public JoinHash(JoinHash joinHash)
    {
        this.positionLinks = joinHash.positionLinks;
        this.pagesIndex = joinHash.pagesIndex;
        this.hashStrategy = new PagesHashStrategy(joinHash.hashStrategy);
        this.addressToPositionMap = new AddressToPositionMap(joinHash.addressToPositionMap, hashStrategy);
    }

    public long getEstimatedSize()
    {
        return pagesIndex.getEstimatedSize().toBytes() + addressToPositionMap.getEstimatedSize().toBytes() + sizeOf(positionLinks.elements());
    }

    public int getChannelCount()
    {
        return pagesIndex.getTupleInfos().size();
    }

    public void setProbeCursors(BlockCursor[] cursors, int[] probeJoinChannels)
    {
        hashStrategy.setProbeCursors(cursors, probeJoinChannels);
    }

    public int getJoinPosition()
    {
        int position = addressToPositionMap.get(CURRENT_ROW_ADDRESS);
        return position;
    }

    public int getNextJoinPosition(int currentPosition)
    {
        return positionLinks.getInt(currentPosition);
    }

    public void appendTupleTo(int position, PageBuilder pageBuilder, int outputChannelOffset)
    {
        for (int channel = 0; channel < getChannelCount(); channel++) {
            pagesIndex.appendTupleTo(channel, position, pageBuilder.getBlockBuilder(outputChannelOffset + channel));
        }
    }

    private static class PagesHashStrategy
            implements Strategy
    {
        private final PagesIndex pagesIndex;
        private final List<Type> types;
        private final int[] hashChannels;
        private final BlockCursor[] joinCursors;

        private PagesHashStrategy(PagesIndex pagesIndex, List<Integer> hashChannels)
        {
            this.pagesIndex = pagesIndex;
            this.hashChannels = Ints.toArray(hashChannels);

            ImmutableList.Builder<Type> types = ImmutableList.builder();
            for (int channel : hashChannels) {
                types.add(pagesIndex.getTupleInfo(channel).getType());
            }
            this.types = types.build();
            this.joinCursors = new BlockCursor[hashChannels.size()];
        }

        private PagesHashStrategy(PagesHashStrategy pagesHashStrategy)
        {
            this.pagesIndex = pagesHashStrategy.pagesIndex;
            this.types = pagesHashStrategy.types;
            this.hashChannels = pagesHashStrategy.hashChannels;
            this.joinCursors = new BlockCursor[types.size()];
        }

        public void setProbeCursors(BlockCursor[] cursors, int[] probeJoinChannels)
        {
            for (int i = 0; i < probeJoinChannels.length; i++) {
                int probeJoinChannel = probeJoinChannels[i];
                joinCursors[i] = cursors[probeJoinChannel];
            }
        }

        @Override
        public int hashCode(long address)
        {
            if (address == CURRENT_ROW_ADDRESS) {
                return hashCurrentRow();
            }
            else {
                return hashPosition(address);
            }
        }

        private int hashPosition(long address)
        {
            int position = Ints.checkedCast(address);
            return pagesIndex.hashCode(hashChannels, position);
        }

        private int hashCurrentRow()
        {
            int result = 0;
            for (int channel = 0; channel < types.size(); channel++) {
                BlockCursor cursor = joinCursors[channel];
                result = 31 * result + cursor.calculateHashCode();
            }
            return result;
        }

        @Override
        public boolean equals(long leftAddress, long rightAddress)
        {
            // current row always equals itself
            if (leftAddress == CURRENT_ROW_ADDRESS && rightAddress == CURRENT_ROW_ADDRESS) {
                return true;
            }

            // current row == position
            if (leftAddress == CURRENT_ROW_ADDRESS) {
                return positionEqualsCurrentRow(Ints.saturatedCast(rightAddress));
            }

            // position == current row
            if (rightAddress == CURRENT_ROW_ADDRESS) {
                return positionEqualsCurrentRow(Ints.saturatedCast(leftAddress));
            }

            // position == position
            return positionEqualsPosition(Ints.saturatedCast(leftAddress), Ints.saturatedCast(rightAddress));
        }

        public boolean positionEqualsPosition(int leftPosition, int rightPosition)
        {
            return pagesIndex.equals(hashChannels, leftPosition, rightPosition);
        }

        private boolean positionEqualsCurrentRow(int position)
        {
            return pagesIndex.equals(hashChannels, position, joinCursors);
        }
    }

    private static class AddressToPositionMap
            extends Long2IntOpenCustomHashMap
    {
        private AddressToPositionMap(int expected, LongHash.Strategy strategy)
        {
            super(expected, strategy);
            defaultReturnValue(-1);
        }

        private AddressToPositionMap(AddressToPositionMap map, LongHash.Strategy strategy)
        {
            // this is super expensive
            super(0, DEFAULT_LOAD_FACTOR, strategy);
            key = map.key;
            value = map.value;
            used = map.used;

            n = map.n;
            maxFill = map.maxFill;
            mask = map.mask;
            size = map.size;

            defaultReturnValue(-1);
        }

        public DataSize getEstimatedSize()
        {
            return new DataSize(sizeOf(this.key) + sizeOf(this.value) + sizeOf(this.used), Unit.BYTE);
        }
    }
}
