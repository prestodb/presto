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
package com.facebook.presto.orc.metadata;

import java.util.Optional;

import static com.facebook.presto.orc.metadata.ColumnEncoding.DEFAULT_SEQUENCE_ID;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class Stream
{
    public enum StreamKind
    {
        PRESENT,
        DATA,
        LENGTH,
        DICTIONARY_DATA,
        DICTIONARY_COUNT,
        SECONDARY,
        ROW_INDEX,
        BLOOM_FILTER,
        BLOOM_FILTER_UTF8,
        IN_DICTIONARY,
        ROW_GROUP_DICTIONARY,
        ROW_GROUP_DICTIONARY_LENGTH,
        IN_MAP,
    }

    private final int column;
    private final StreamKind streamKind;
    private final int length;
    private final boolean useVInts;
    private final int sequence;

    private final Optional<Long> offset;

    public Stream(int column, StreamKind streamKind, int length, boolean useVInts)
    {
        this(column, DEFAULT_SEQUENCE_ID, streamKind, length, useVInts, Optional.empty());
    }

    public Stream(int column, int sequence, StreamKind streamKind, int length, boolean useVInts)
    {
        this(column, sequence, streamKind, length, useVInts, Optional.empty());
    }

    public Stream(int column, int sequence, StreamKind streamKind, int length, boolean useVInts, Optional<Long> offset)
    {
        this.column = column;
        this.streamKind = requireNonNull(streamKind, "streamKind is null");
        this.length = length;
        this.useVInts = useVInts;
        this.sequence = sequence;
        this.offset = requireNonNull(offset, "offset is null");
    }

    public int getColumn()
    {
        return column;
    }

    public StreamKind getStreamKind()
    {
        return streamKind;
    }

    public int getLength()
    {
        return length;
    }

    public boolean isUseVInts()
    {
        return useVInts;
    }

    public int getSequence()
    {
        return sequence;
    }

    public Optional<Long> getOffset()
    {
        return offset;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("column", column)
                .add("streamKind", streamKind)
                .add("length", length)
                .add("useVInts", useVInts)
                .add("sequence", sequence)
                .add("offset", offset)
                .toString();
    }

    public Stream withOffset(long offset)
    {
        return new Stream(
                this.column,
                this.sequence,
                this.streamKind,
                this.length,
                this.useVInts,
                Optional.of(offset));
    }
}
