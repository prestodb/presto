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

import java.util.Objects;

public final class StreamId
{
    private final int column;
    private final Stream.Kind kind;

    public StreamId(Stream stream)
    {
        this.column = stream.getColumn();
        this.kind = stream.getKind();
    }

    public StreamId(int column, Stream.Kind kind)
    {
        this.column = column;
        this.kind = kind;
    }

    public int getColumn()
    {
        return column;
    }

    public Stream.Kind getKind()
    {
        return kind;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(column, kind);
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
        return Objects.equals(this.column, other.column) && Objects.equals(this.kind, other.kind);
    }

    @Override
    public String toString()
    {
        return com.google.common.base.Objects.toStringHelper(this)
                .add("column", column)
                .add("kind", kind)
                .toString();
    }
}
