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

import java.util.Map;
import java.util.OptionalInt;

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

    public <S extends ValueInputStream<?>> InputStreamSource<S> getInputStreamSource(StreamDescriptor streamDescriptor, StreamKind streamKind, Class<S> streamType)
    {
        requireNonNull(streamDescriptor, "streamDescriptor is null");
        requireNonNull(streamType, "streamType is null");

        InputStreamSource<?> streamSource = streamSources.get(new StreamId(streamDescriptor.getStreamId(), streamDescriptor.getSequence(), streamKind));

        // Here is the problematic place #2. We use StreamDescriptor from the problematic place #1 and attempt to find a StreamId key
        // in the map. The StreamDescriptor created by the AbstractOrcRecordReader.createStreamDescriptor will have missing sequence.
        // It will work fine for files that have missing sequence, but won't for files that use 0 sequence instead of missing sequence.
        if (streamSource == null && !streamDescriptor.getSequence().isPresent()) {
            // this is a workaround for such file, we need to treat StreamId with missing sequence and 0 sequence as the same object.
            streamSource = streamSources.get(new StreamId(streamDescriptor.getStreamId(), OptionalInt.of(0), streamKind));
        }

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
