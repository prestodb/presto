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
package com.facebook.presto.orc;

import com.facebook.presto.orc.StreamDescriptorFactory.AllStreams;
import com.facebook.presto.orc.StreamDescriptorFactory.StreamProperty;
import com.facebook.presto.orc.metadata.OrcType;
import com.facebook.presto.orc.metadata.OrcType.OrcTypeKind;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.facebook.presto.orc.metadata.ColumnEncoding.DEFAULT_SEQUENCE_ID;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class StreamDescriptor
{
    private final AllStreams allStreams;
    private final int streamId;
    private final int sequence;

    public StreamDescriptor(int streamId, AllStreams allStreams)
    {
        this(streamId, DEFAULT_SEQUENCE_ID, allStreams);
    }

    public StreamDescriptor(int streamId, int sequence, AllStreams allStreams)
    {
        this.streamId = streamId;
        this.sequence = sequence;
        this.allStreams = requireNonNull(allStreams, "allStreams is null");
    }

    public StreamDescriptor duplicate(int sequence)
    {
        return new StreamDescriptor(streamId, sequence, allStreams);
    }

    public String getStreamName()
    {
        return getStreamProperty().getStreamName();
    }

    public int getStreamId()
    {
        return streamId;
    }

    public int getSequence()
    {
        return sequence;
    }

    public OrcTypeKind getOrcTypeKind()
    {
        return getOrcType().getOrcTypeKind();
    }

    public OrcType getOrcType()
    {
        return getStreamProperty().getOrcType();
    }

    public String getFieldName()
    {
        return getStreamProperty().getFieldName();
    }

    public OrcDataSourceId getOrcDataSourceId()
    {
        return getOrcDataSource().getId();
    }

    public OrcDataSource getOrcDataSource()
    {
        return allStreams.getOrcDataSource();
    }

    public List<StreamDescriptor> getNestedStreams()
    {
        List<Integer> nestedStreamIds = getStreamProperty().getNestedStreamIds();
        if (nestedStreamIds.isEmpty()) {
            return ImmutableList.of();
        }

        ImmutableList.Builder<StreamDescriptor> nestedStreamsBuilder = ImmutableList.builderWithExpectedSize(nestedStreamIds.size());
        for (int nestedStreamId : nestedStreamIds) {
            nestedStreamsBuilder.add(new StreamDescriptor(nestedStreamId, sequence, allStreams));
        }
        return nestedStreamsBuilder.build();
    }

    private StreamProperty getStreamProperty()
    {
        return allStreams.getStreamProperty(streamId);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("streamName", getStreamName())
                .add("streamId", streamId)
                .add("sequence", sequence)
                .add("orcType", getOrcType())
                .add("dataSource", getOrcDataSourceId())
                .toString();
    }
}
