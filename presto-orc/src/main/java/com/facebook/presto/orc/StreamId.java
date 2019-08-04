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

import static com.google.common.base.MoreObjects.toStringHelper;

public final class StreamId
{
    private final int column;
    private final int sequence;
    private final StreamKind streamKind;
    // Optional label used for recording access stats. Not considered in equality.
    private final String label;

    public StreamId(Stream stream)
    {
        this.column = stream.getColumn();
        this.sequence = stream.getSequence();
        this.streamKind = stream.getStreamKind();
        label = null;
    }

    public StreamId(Stream stream, String label)
    {
        this.column = stream.getColumn();
        this.sequence = stream.getSequence();
        this.streamKind = stream.getStreamKind();
        this.label = label;
    }

    public StreamId(int column, int sequence, StreamKind streamKind)
    {
        this.column = column;
        this.sequence = sequence;
        this.streamKind = streamKind;
        this.label = null;
    }

    public int getColumn()
    {
        return column;
    }

    public int getSequence()
    {
        return sequence;
    }

    public StreamKind getStreamKind()
    {
        return streamKind;
    }

    public String getLabel()
    {
        return label;
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
        return column == other.column && sequence == other.sequence && streamKind == other.streamKind;
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
