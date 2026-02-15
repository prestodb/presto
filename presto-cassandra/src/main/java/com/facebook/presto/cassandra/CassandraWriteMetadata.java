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
package com.facebook.presto.cassandra;

import com.facebook.airlift.json.JsonCodec;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.google.common.base.Preconditions.checkArgument;

/**
 * Metadata about a Cassandra write operation, including the number of rows written.
 * This is returned by CassandraPageSink.finish() to report write statistics.
 */
public class CassandraWriteMetadata
{
    private static final JsonCodec<CassandraWriteMetadata> CODEC = jsonCodec(CassandraWriteMetadata.class);

    private final long rowsWritten;

    @JsonCreator
    public CassandraWriteMetadata(@JsonProperty("rowsWritten") long rowsWritten)
    {
        checkArgument(rowsWritten >= 0, "rowsWritten cannot be negative");
        this.rowsWritten = rowsWritten;
    }

    @JsonProperty
    public long getRowsWritten()
    {
        return rowsWritten;
    }

    public Slice toSlice()
    {
        return Slices.wrappedBuffer(CODEC.toJsonBytes(this));
    }

    public static CassandraWriteMetadata fromSlice(Slice slice)
    {
        return CODEC.fromJson(slice.getBytes());
    }
}

