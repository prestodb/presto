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

import com.facebook.presto.hive.orc.metadata.Stream;
import com.facebook.presto.hive.orc.metadata.Stream.StreamKind;

import java.util.Objects;

public final class StreamId
{
    private final int column;
    private final StreamKind streamKind;

    public StreamId(Stream stream)
    {
        this.column = stream.getColumn();
        this.streamKind = stream.getStreamKind();
    }

    public StreamId(int column, StreamKind streamKind)
    {
        this.column = column;
        this.streamKind = streamKind;
    }

    public int getColumn()
    {
        return column;
    }

    public StreamKind getStreamKind()
    {
        return streamKind;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(column, streamKind);
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
        return Objects.equals(this.column, other.column) && Objects.equals(this.streamKind, other.streamKind);
    }

    @Override
    public String toString()
    {
        return com.google.common.base.Objects.toStringHelper(this)
                .add("column", column)
                .add("streamKind", streamKind)
                .toString();
    }
}
