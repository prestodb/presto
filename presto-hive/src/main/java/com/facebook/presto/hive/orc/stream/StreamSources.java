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
package com.facebook.presto.hive.orc.stream;

import com.facebook.presto.hive.orc.StreamDescriptor;
import com.facebook.presto.hive.orc.StreamId;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.Stream;

import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class StreamSources
{
    private final Map<StreamId, StreamSource<?>> streamSources;

    public StreamSources(Map<StreamId, StreamSource<?>> streamSources)
    {
        this.streamSources = ImmutableMap.copyOf(checkNotNull(streamSources, "streamSources is null"));
    }

    public <T extends StreamSource<?>> T getStreamSource(StreamDescriptor streamDescriptor, Stream.Kind kind, Class<T> type)
    {
        checkNotNull(streamDescriptor, "streamDescriptor is null");
        checkNotNull(type, "type is null");

        T streamSource = getStreamSourceIfPresent(streamDescriptor, kind, type);

        checkNotNull(streamSource, "Required %s is not present", streamDescriptor);
        return streamSource;
    }

    public <T extends StreamSource<?>> T getStreamSourceIfPresent(StreamDescriptor streamDescriptor, Stream.Kind kind, Class<T> type)
    {
        checkNotNull(streamDescriptor, "streamDescriptor is null");
        checkNotNull(type, "type is null");

        StreamSource<?> streamSource = streamSources.get(new StreamId(streamDescriptor.getStreamId(), kind));

        if (streamSource != null) {
            checkArgument(type.isInstance(streamSource),
                    "%s must be of type %s, not %s",
                    streamDescriptor,
                    type.getName(),
                    streamSource.getClass().getName());
        }
        return type.cast(streamSource);
    }
}
