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

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.Type;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public final class StreamDescriptor
{
    private final String streamName;
    private final int streamId;
    private final Type.Kind streamType;
    private final String fieldName;
    private final Path path;
    private final List<StreamDescriptor> nestedStreams;

    public StreamDescriptor(String streamName, int streamId, String fieldName, Type.Kind streamType, Path path, List<StreamDescriptor> nestedStreams)
    {
        this.fieldName = fieldName;
        this.streamName = checkNotNull(streamName, "streamName is null");
        this.streamId = streamId;
        this.streamType = checkNotNull(streamType, "type is null");
        this.path = path;
        this.nestedStreams = ImmutableList.copyOf(checkNotNull(nestedStreams, "nestedStreams is null"));
    }

    public String getStreamName()
    {
        return streamName;
    }

    public int getStreamId()
    {
        return streamId;
    }

    public Type.Kind getStreamType()
    {
        return streamType;
    }

    public String getFieldName()
    {
        return fieldName;
    }

    public Path getPath()
    {
        return path;
    }

    public List<StreamDescriptor> getNestedStreams()
    {
        return nestedStreams;
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

        StreamDescriptor that = (StreamDescriptor) o;

        if (streamId != that.streamId) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        return streamId;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("streamName", streamName)
                .add("streamId", streamId)
                .add("streamType", streamType)
                .add("path", path)
                .toString();
    }
}
