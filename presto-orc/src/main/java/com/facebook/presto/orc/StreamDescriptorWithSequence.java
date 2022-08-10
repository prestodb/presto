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

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class StreamDescriptorWithSequence
        implements StreamDescriptor
{
    private final StreamDescriptor streamDescriptor;
    private final int sequence;

    public StreamDescriptorWithSequence(StreamDescriptor streamDescriptor, int sequence)
    {
        this.streamDescriptor = requireNonNull(streamDescriptor, "streamDescriptor is null");
        this.sequence = sequence;
    }

    public StreamDescriptorWithSequence duplicate(int sequence)
    {
        return new StreamDescriptorWithSequence(streamDescriptor, sequence);
    }

    public String getStreamName()
    {
        return streamDescriptor.getStreamName();
    }

    public int getStreamId()
    {
        return streamDescriptor.getStreamId();
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
        return streamDescriptor.getOrcType();
    }

    public String getFieldName()
    {
        return streamDescriptor.getFieldName();
    }

    public OrcDataSourceId getOrcDataSourceId()
    {
        return getOrcDataSource().getId();
    }

    public OrcDataSource getOrcDataSource()
    {
        return streamDescriptor.getOrcDataSource();
    }

    public List<StreamDescriptor> getNestedStreams()
    {
        return streamDescriptor.getNestedStreams().stream()
                .map(stream -> copyStreamDescriptorWithSequence(stream, sequence))
                .collect(toImmutableList());
    }

    private static StreamDescriptor copyStreamDescriptorWithSequence(StreamDescriptor streamDescriptor, int sequence)
    {
        if (!streamDescriptor.getNestedStreams().isEmpty()) {
            List<StreamDescriptor> nestedStreams = streamDescriptor.getNestedStreams().stream()
                    .map(stream -> copyStreamDescriptorWithSequence(stream, sequence))
                    .collect(toImmutableList());
            streamDescriptor = new StreamDescriptorWithoutSequence(
                    streamDescriptor.getStreamName(),
                    streamDescriptor.getStreamId(),
                    streamDescriptor.getFieldName(),
                    streamDescriptor.getOrcType(),
                    streamDescriptor.getOrcDataSource(),
                    nestedStreams);
        }

        return new StreamDescriptorWithSequence(streamDescriptor, sequence);
    }
}
