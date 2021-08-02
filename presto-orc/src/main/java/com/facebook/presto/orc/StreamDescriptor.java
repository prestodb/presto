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

import com.facebook.presto.orc.metadata.OrcType;
import com.facebook.presto.orc.metadata.OrcType.OrcTypeKind;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.facebook.presto.orc.metadata.ColumnEncoding.DEFAULT_SEQUENCE_ID;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class StreamDescriptor
{
    private final String streamName;
    private final int streamId;
    private final int sequence;
    private final OrcType orcType;
    private final String fieldName;
    private final OrcDataSource orcDataSource;
    private final List<StreamDescriptor> nestedStreams;

    public StreamDescriptor(String streamName, int streamId, String fieldName, OrcType orcType, OrcDataSource orcDataSource, List<StreamDescriptor> nestedStreams)
    {
        this(streamName, streamId, fieldName, orcType, orcDataSource, nestedStreams, DEFAULT_SEQUENCE_ID);
    }

    public StreamDescriptor(String streamName, int streamId, String fieldName, OrcType orcType, OrcDataSource orcDataSource, List<StreamDescriptor> nestedStreams, int sequence)
    {
        this.streamName = requireNonNull(streamName, "streamName is null");
        this.streamId = streamId;
        this.sequence = sequence;
        this.fieldName = requireNonNull(fieldName, "fieldName is null");
        this.orcType = requireNonNull(orcType, "orcType is null");
        this.orcDataSource = requireNonNull(orcDataSource, "orcDataSource is null");
        this.nestedStreams = ImmutableList.copyOf(requireNonNull(nestedStreams, "nestedStreams is null"));
    }

    public StreamDescriptor duplicate(int sequence)
    {
        return new StreamDescriptor(streamName, streamId, fieldName, orcType, orcDataSource, nestedStreams, sequence);
    }

    public String getStreamName()
    {
        return streamName;
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
        return orcType.getOrcTypeKind();
    }

    public OrcType getOrcType()
    {
        return this.orcType;
    }

    public String getFieldName()
    {
        return fieldName;
    }

    public OrcDataSourceId getOrcDataSourceId()
    {
        return orcDataSource.getId();
    }

    public OrcDataSource getOrcDataSource()
    {
        return orcDataSource;
    }

    public List<StreamDescriptor> getNestedStreams()
    {
        return nestedStreams;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("streamName", streamName)
                .add("streamId", streamId)
                .add("sequence", sequence)
                .add("orcType", orcType)
                .add("dataSource", orcDataSource.getId())
                .toString();
    }
}
