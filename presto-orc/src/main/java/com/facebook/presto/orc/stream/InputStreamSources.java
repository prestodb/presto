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
package com.facebook.presto.orc.stream;

import com.facebook.presto.orc.StreamDescriptor;
import com.facebook.presto.orc.StreamId;
import com.facebook.presto.orc.metadata.Stream.StreamKind;
import com.google.common.collect.ImmutableMap;

import javax.annotation.Nonnull;

import java.util.Map;

import static com.facebook.presto.orc.stream.MissingInputStreamSource.missingStreamSource;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class InputStreamSources
{
    private final Map<StreamId, InputStreamSource<?>> streamSources;

    public InputStreamSources(Map<StreamId, InputStreamSource<?>> streamSources)
    {
        this.streamSources = ImmutableMap.copyOf(requireNonNull(streamSources, "streamSources is null"));
    }

    @Nonnull
    public <S extends ValueInputStream<?>> InputStreamSource<S> getInputStreamSource(StreamDescriptor streamDescriptor, StreamKind streamKind, Class<S> streamType)
    {
        requireNonNull(streamDescriptor, "streamDescriptor is null");
        requireNonNull(streamType, "streamType is null");

        InputStreamSource<?> streamSource = streamSources.get(new StreamId(streamDescriptor.getStreamId(), streamKind));
        if (streamSource == null) {
            streamSource = missingStreamSource(streamType);
        }

        checkArgument(streamType.isAssignableFrom(streamSource.getStreamType()),
                "%s must be of type %s, not %s",
                streamDescriptor,
                streamType.getName(),
                streamSource.getStreamType().getName());

        return (InputStreamSource<S>) streamSource;
    }
}
