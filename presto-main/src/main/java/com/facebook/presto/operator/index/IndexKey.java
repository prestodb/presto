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
package com.facebook.presto.operator.index;

import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.operator.ChannelIndex;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.facebook.presto.operator.HashStrategyUtils.addToHashCode;
import static com.facebook.presto.operator.HashStrategyUtils.valueEquals;
import static com.facebook.presto.operator.HashStrategyUtils.valueHashCode;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkPositionIndex;
import static com.google.common.base.Preconditions.checkState;

public final class IndexKey
{
    private final List<ChannelIndex> channels;
    private final int position;
    private BlockCursor[] joinCursors;

    private IndexKey(List<ChannelIndex> channels, int position)
    {
        this.channels = channels;
        this.position = position;
    }

    public static IndexKey createStaticKey(List<ChannelIndex> channels, int position)
    {
        checkNotNull(channels, "channels is null");
        checkArgument(position >= 0, "position must be positive");
        checkPositionIndex(position, channels.get(0).getPositionCount());
        return new IndexKey(ImmutableList.copyOf(channels), position);
    }

    public static IndexKey createMutableJoinCursorKey()
    {
        return new IndexKey(null, -1);
    }

    public IndexKey setJoinCursors(BlockCursor[] joinCursors)
    {
        checkNotNull(joinCursors, "joinCursors is null");
        checkState(channels == null, "Key is not mutable");
        this.joinCursors = joinCursors;
        return this;
    }

    @Override
    public int hashCode()
    {
        int hashCode = 0;
        if (channels == null) {
            checkState(joinCursors != null, "joinCursors not set!");
            for (BlockCursor joinCursor : joinCursors) {
                TupleInfo.Type type = joinCursor.getTupleInfo().getType();
                hashCode = addToHashCode(hashCode, valueHashCode(type, joinCursor.getRawSlice(), joinCursor.getRawOffset()));
            }
        }
        else {
            for (ChannelIndex hashChannel : channels) {
                hashCode = addToHashCode(hashCode, hashChannel.hashCode(position));
            }
        }
        return hashCode;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        IndexKey indexKey = (IndexKey) o;

        if (channels == null && indexKey.channels == null) {
            checkState(joinCursors != null, "joinCursors not set!");
            checkState(indexKey.joinCursors != null, "indexKey.joinCursors not set!");
            return cursorsEqualsCursors(joinCursors, indexKey.joinCursors);
        }

        if (channels == null) {
            checkState(joinCursors != null, "joinCursors not set!");
            return cursorsEqualsChannelPosition(joinCursors, indexKey.channels, indexKey.position);
        }

        if (indexKey.channels == null) {
            checkState(indexKey.joinCursors != null, "indexKey.joinCursors not set!");
            return cursorsEqualsChannelPosition(indexKey.joinCursors, channels, position);
        }

        return channelPositionEqualsChannelPosition(channels, position, indexKey.channels, indexKey.position);
    }

    private static boolean cursorsEqualsCursors(BlockCursor[] cursors1, BlockCursor[] cursors2)
    {
        checkArgument(cursors1.length == cursors2.length, "Mismatched cursor field lengths");

        if (cursors1 == cursors2) {
            return true;
        }

        for (int i = 0; i < cursors1.length; i++) {
            BlockCursor cursor1 = cursors1[i];
            BlockCursor cursor2 = cursors2[i];

            TupleInfo.Type type = cursor1.getTupleInfo().getType();
            checkArgument(type == cursor2.getTupleInfo().getType(), "Mismatched cursor field types");

            if (!valueEquals(type, cursor1.getRawSlice(), cursor1.getRawOffset(), cursor2.getRawSlice(), cursor2.getRawOffset())) {
                return false;
            }
        }
        return true;
    }

    private static boolean channelPositionEqualsChannelPosition(List<ChannelIndex> channels1, int position1, List<ChannelIndex> channels2, int position2)
    {
        checkArgument(channels1.size() == channels2.size(), "Mismatched channel counts");

        if (channels1 == channels2 && position1 == position2) {
            return true;
        }

        for (int i = 0; i < channels1.size(); i++) {
            ChannelIndex channelIndex1 = channels1.get(i);
            ChannelIndex channelIndex2 = channels2.get(i);

            TupleInfo.Type type = channelIndex1.getTupleInfo().getType();
            checkArgument(type == channelIndex2.getTupleInfo().getType(), "Mismatched channel field types");

            if (!channelIndex1.equals(position1, channelIndex2, position2)) {
                return false;
            }
        }
        return true;
    }

    private static boolean cursorsEqualsChannelPosition(BlockCursor[] cursors, List<ChannelIndex> channels, int position)
    {
        checkArgument(cursors.length == channels.size(), "Mismatched field counts");

        for (int i = 0; i < cursors.length; i++) {
            BlockCursor cursor = cursors[i];
            ChannelIndex channelIndex = channels.get(i);

            TupleInfo.Type type = cursor.getTupleInfo().getType();
            checkArgument(type == channelIndex.getTupleInfo().getType(), "Mismatched field types");

            if (!channelIndex.equals(position, cursor)) {
                return false;
            }
        }
        return true;
    }
}
