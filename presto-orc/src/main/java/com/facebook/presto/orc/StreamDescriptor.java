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

import com.facebook.presto.orc.metadata.OrcType.OrcTypeKind;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class StreamDescriptor
{
    private final int parent;
    private final int streamId;
    private final OrcTypeKind streamType;
    private final String streamName;
    private final OrcDataSource fileInput;
    private final List<StreamDescriptor> nestedStreams;

    public StreamDescriptor(int parent, int streamId, String streamName, OrcTypeKind streamType, OrcDataSource fileInput, List<StreamDescriptor> nestedStreams)
    {
        this.parent = parent;
        this.streamId = streamId;
        this.streamName = requireNonNull(streamName, "fieldName is null");
        this.streamType = requireNonNull(streamType, "type is null");
        this.fileInput = requireNonNull(fileInput, "fileInput is null");
        this.nestedStreams = ImmutableList.copyOf(requireNonNull(nestedStreams, "nestedStreams is null"));
    }

    public int getParent()
    {
        return parent;
    }

    public String getStreamName()
    {
        return streamName;
    }

    public int getStreamId()
    {
        return streamId;
    }

    public OrcTypeKind getStreamType()
    {
        return streamType;
    }

    public OrcDataSource getFileInput()
    {
        return fileInput;
    }

    public List<StreamDescriptor> getNestedStreams()
    {
        return nestedStreams;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("streamName", getStreamName())
                .add("streamId", streamId)
                .add("streamType", streamType)
                .add("path", fileInput)
                .toString();
    }
}
