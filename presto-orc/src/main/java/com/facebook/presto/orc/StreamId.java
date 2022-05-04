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

import com.facebook.presto.orc.metadata.Stream;
import com.facebook.presto.orc.metadata.Stream.StreamKind;

import java.util.Objects;
import java.util.OptionalInt;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class StreamId
{
    private final int column;
    private final OptionalInt sequence;
    private final StreamKind streamKind;

    public StreamId(Stream stream)
    {
        this.column = stream.getColumn();
        this.sequence = stream.getSequence();
        this.streamKind = stream.getStreamKind();
    }

    public StreamId(int column, OptionalInt sequence, StreamKind streamKind)
    {
        this.column = column;
        this.sequence = requireNonNull(sequence, "sequence is null");
        this.streamKind = streamKind;
    }

    public int getColumn()
    {
        return column;
    }

    public OptionalInt getSequence()
    {
        return sequence;
    }

    public StreamKind getStreamKind()
    {
        return streamKind;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(column, sequence, streamKind);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        StreamId other = (StreamId) obj;
        return column == other.column && sequence.equals(other.sequence) && streamKind == other.streamKind;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("column", column)
                .add("sequence", sequence)
                .add("streamKind", streamKind)
                .toString();
    }
}
