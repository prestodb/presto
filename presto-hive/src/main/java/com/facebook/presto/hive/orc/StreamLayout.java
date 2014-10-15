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

import com.facebook.presto.hive.orc.metadata.ColumnEncoding.ColumnEncodingKind;
import com.facebook.presto.hive.orc.metadata.CompressionKind;
import com.facebook.presto.hive.orc.metadata.OrcType.OrcTypeKind;
import com.facebook.presto.hive.orc.reader.StreamSources;
import com.facebook.presto.hive.orc.stream.OrcInputStream;
import com.facebook.presto.hive.orc.stream.StreamSource;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class StreamLayout
{
    private final StreamId streamId;
    private final int groupId;
    private final OrcTypeKind type;
    private final ColumnEncodingKind encoding;
    private final boolean usesVInt;
    private final CompressionKind compressionKind;
    private final DiskRange diskRange;
    private final List<Integer> offsetPositions;

    public StreamLayout(StreamId streamId,
            int groupId,
            OrcTypeKind type,
            ColumnEncodingKind encoding,
            boolean usesVInt,
            CompressionKind compressionKind,
            DiskRange diskRange,
            List<Integer> offsetPositions)
    {
        this.streamId = checkNotNull(streamId, "streamId is null");
        this.groupId = groupId;
        this.type = checkNotNull(type, "type is null");
        this.encoding = checkNotNull(encoding, "encoding is null");
        this.usesVInt = usesVInt;
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

    public OrcTypeKind getType()
    {
        return type;
    }

    public ColumnEncodingKind getEncoding()
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

    public StreamSource<?> createStreamSource(OrcInputStream inputStream)
    {
        checkNotNull(inputStream, "inputStream is null");
        return StreamSources.createStreamSource(streamId, inputStream, type, encoding, usesVInt, compressionKind, offsetPositions);
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
        return new StreamLayout(streamId, groupId, type, encoding, usesVInt, compressionKind, diskRange.span(otherStreamLayout.getDiskRange()), offsetPositions);
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
}
