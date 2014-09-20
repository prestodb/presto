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
package com.facebook.presto.hive.orc;

import com.facebook.presto.hive.orc.stream.StreamSource;
import com.facebook.presto.hive.orc.stream.StreamSources;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Ordering;
import io.airlift.slice.Slice;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public final class RowGroupLayout
{
    private final int groupId;
    private final long rowCount;
    private final List<StreamLayout> streamLayouts;

    public RowGroupLayout(int groupId, long rowCount, Iterable<StreamLayout> streamLayouts)
    {
        this.groupId = groupId;
        this.rowCount = rowCount;
        this.streamLayouts = ImmutableList.copyOf(checkNotNull(streamLayouts, "streamLayouts is null"));
    }

    public int getGroupId()
    {
        return groupId;
    }

    public long getRowCount()
    {
        return rowCount;
    }

    public List<StreamLayout> getStreamLayouts()
    {
        return streamLayouts;
    }

    public RowGroup createRowGroup(Map<StreamId, Slice> streamsData, int bufferSize)
    {
        ImmutableMap.Builder<StreamId, StreamSource<?>> builder = ImmutableMap.builder();
        for (StreamLayout streamLayout : streamLayouts) {
            builder.put(streamLayout.getStreamId(), streamLayout.createStreamSource(streamsData.get(streamLayout.getStreamId()), bufferSize));
        }
        StreamSources rowGroupStreams = new StreamSources(builder.build());
        return new RowGroup(groupId, rowCount, rowGroupStreams);
    }

    public RowGroupLayout mergeWith(RowGroupLayout otherGroup)
    {
        checkNotNull(otherGroup, "otherGroup is null");
        checkArgument(groupId != otherGroup.groupId, "can not merge with self");

        // if the new group is before this stream, merge in the opposite order
        if (otherGroup.getGroupId() < groupId) {
            return otherGroup.mergeWith(this);
        }

        List<StreamLayout> otherStreamLayouts = otherGroup.getStreamLayouts();
        checkArgument(otherStreamLayouts.size() == streamLayouts.size());

        ImmutableList.Builder<StreamLayout> builder = ImmutableList.builder();
        for (int i = 0; i < streamLayouts.size(); i++) {
            StreamLayout streamLayout = streamLayouts.get(i);
            StreamLayout otherStreamLayout = otherStreamLayouts.get(i);
            builder.add(streamLayout.mergeWith(otherStreamLayout));
        }
        return new RowGroupLayout(groupId, rowCount + otherGroup.getRowCount(), builder.build());
    }

    private static Function<RowGroupLayout, Integer> groupIdGetter()
    {
        return new Function<RowGroupLayout, Integer>()
        {
            @Override
            public Integer apply(RowGroupLayout input)
            {
                return input.getGroupId();
            }
        };
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("groupId", groupId)
                .add("rowCount", rowCount)
                .add("streams", streamLayouts)
                .toString();
    }

    public static List<RowGroupLayout> mergeAdjacentRowGroups(List<RowGroupLayout> rowGroups)
    {
        checkNotNull(rowGroups, "rowGroups is null");
        if (rowGroups.isEmpty()) {
            return rowGroups;
        }

        rowGroups = Ordering.natural().onResultOf(groupIdGetter()).sortedCopy(rowGroups);

        ImmutableList.Builder<RowGroupLayout> builder = ImmutableList.builder();
        RowGroupLayout previousGroup = rowGroups.get(0);
        int previousGroupId = previousGroup.getGroupId();
        for (int i = 1; i < rowGroups.size(); i++) {
            RowGroupLayout group = rowGroups.get(i);
            if (previousGroupId + 1 == group.getGroupId()) {
                previousGroup = previousGroup.mergeWith(group);
            }
            else {
                builder.add(previousGroup);
                previousGroup = group;
            }
            previousGroupId = group.getGroupId();
        }
        builder.add(previousGroup);
        return builder.build();
    }
}
