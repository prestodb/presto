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
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.facebook.presto.orc.metadata.ColumnEncoding.DEFAULT_SEQUENCE_ID;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class StreamDescriptorWithoutSequence
        implements StreamDescriptor
{
    private final String streamName;
    private final int streamId;
    private final OrcType orcType;
    private final String fieldName;
    private final OrcDataSource orcDataSource;
    private final List<StreamDescriptor> nestedStreams;

    public StreamDescriptorWithoutSequence(String streamName, int streamId, String fieldName, OrcType orcType, OrcDataSource orcDataSource, List<StreamDescriptor> nestedStreams)
    {
        this.streamName = requireNonNull(streamName, "streamName is null");
        this.streamId = streamId;
        this.fieldName = requireNonNull(fieldName, "fieldName is null");
        this.orcType = requireNonNull(orcType, "orcType is null");
        this.orcDataSource = requireNonNull(orcDataSource, "orcDataSource is null");
        this.nestedStreams = ImmutableList.copyOf(requireNonNull(nestedStreams, "nestedStreams is null"));
    }

    @Override
    public StreamDescriptor duplicate(int sequence)
    {
        return new StreamDescriptorWithSequence(this, sequence);
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
        return DEFAULT_SEQUENCE_ID;
    }

    public OrcType getOrcType()
    {
        return this.orcType;
    }

    public String getFieldName()
    {
        return fieldName;
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
                .add("sequence", getSequence())
                .add("orcType", orcType)
                .add("dataSource", orcDataSource.getId())
                .toString();
    }
}
