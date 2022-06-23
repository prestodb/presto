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
package com.facebook.presto.parquet;

import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;

import java.io.Closeable;
import java.io.IOException;
import java.util.Optional;

public interface ParquetDataSource
        extends Closeable
{
    ParquetDataSourceId getId();

    long getReadBytes();

    long getReadTimeNanos();

    void readFully(long position, byte[] buffer);

    void readFully(long position, byte[] buffer, int bufferOffset, int bufferLength);

    Optional<ColumnIndex> readColumnIndex(ColumnChunkMetaData column) throws IOException;

    Optional<OffsetIndex> readOffsetIndex(ColumnChunkMetaData column) throws IOException;

    @Override
    default void close()
            throws IOException
    {
    }
}
