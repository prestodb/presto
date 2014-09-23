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

import com.facebook.presto.hive.orc.reader.StreamSources;
import com.facebook.presto.hive.orc.stream.StreamSource;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import io.airlift.slice.Slice;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.ColumnEncoding;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.CompressionKind;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.Type;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class StreamLayout
{
    private final StreamId streamId;
    private final int groupId;
    private final Type.Kind type;
    private final ColumnEncoding.Kind encoding;
    private final CompressionKind compressionKind;
    private final DiskRange diskRange;
    private final List<Integer> offsetPositions;

    public StreamLayout(StreamId streamId,
            int groupId,
            Type.Kind type,
            ColumnEncoding.Kind encoding,
            CompressionKind compressionKind,
            DiskRange diskRange,
            List<Integer> offsetPositions)
    {
        this.streamId = checkNotNull(streamId, "streamId is null");
        this.groupId = groupId;
        this.type = checkNotNull(type, "type is null");
        this.encoding = checkNotNull(encoding, "encoding is null");
        this.compressionKind = checkNotNull(compressionKind, "compressionKind is null");
        this.diskRange = checkNotNull(diskRange, "diskRange is null");
        this.offsetPositions = ImmutableList.copyOf(checkNotNull(offsetPositions, "offsetPositions is null"));
    }

    public StreamId getStreamId()
    {
        return streamId;
    }

    public int getGroupId()
    {
        return groupId;
    }

    public Type.Kind getType()
    {
        return type;
    }

    public ColumnEncoding.Kind getEncoding()
    {
        return encoding;
    }

    public CompressionKind getCompressionKind()
    {
        return compressionKind;
    }

    public DiskRange getDiskRange()
    {
        return diskRange;
    }

    public StreamSource<?> createStreamSource(List<StripeSlice> stripeSlices, int bufferSize)
    {
        long offset = diskRange.getOffset();
        int length = Ints.checkedCast(diskRange.getLength());

        StripeSlice stripeSlice = null;
        for (StripeSlice range : stripeSlices) {
            if (range.containsRange(offset, length)) {
                stripeSlice = range;
                break;
            }
        }
        if (stripeSlice == null) {
            throw new IllegalArgumentException("No buffer for stream " + this);
        }

        Slice slice = stripeSlice.slice(offset, length);
        return StreamSources.createStreamSource(streamId, slice, type, encoding, compressionKind, offsetPositions, bufferSize);
    }

    public StreamLayout mergeWith(StreamLayout otherStreamLayout)
    {
        checkNotNull(otherStreamLayout, "otherStreamLayout is null");

        // if the new stream is before this stream, merge in the opposite order
        if (otherStreamLayout.getGroupId() < groupId) {
            return otherStreamLayout.mergeWith(this);
        }

        checkArgument(streamId.equals(otherStreamLayout.getStreamId()), "Streams must have the same name");
        checkArgument(type == otherStreamLayout.getType(), "Streams must have the same type");
        checkArgument(encoding == otherStreamLayout.getEncoding(), "Streams must have the same encoding");
        checkArgument(compressionKind == otherStreamLayout.getCompressionKind(), "Streams must have the same compression kind");
        return new StreamLayout(streamId, groupId, type, encoding, compressionKind, diskRange.mergeWith(otherStreamLayout.getDiskRange()), offsetPositions);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("streamId", streamId)
                .add("groupId", groupId)
                .add("type", type)
                .add("encoding", encoding)
                .add("compressionKind", compressionKind)
                .add("diskRange", diskRange)
                .add("offsetPositions", offsetPositions)
                .toString();
    }

    public static Function<StreamLayout, DiskRange> diskRangeGetter()
    {
        return new Function<StreamLayout, DiskRange>()
        {
            @Override
            public DiskRange apply(StreamLayout streamLayout)
            {
                return streamLayout.getDiskRange();
            }
        };
    }
}
