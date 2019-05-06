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

import com.facebook.presto.orc.OrcDataSourceId;
import io.airlift.slice.SizeOf;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import javax.annotation.Nullable;

import java.io.IOException;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public abstract class AbstractDiskOrcDataReader
        implements OrcDataReader
{
    private final OrcDataSourceId orcDataSourceId;
    private final int dataSize;
    private final int maxBufferSize;

    @Nullable
    private byte[] buffer;
    private int bufferSize;
    private int bufferStartPosition;

    public AbstractDiskOrcDataReader(OrcDataSourceId orcDataSourceId, int dataSize, int bufferSize)
    {
        this.orcDataSourceId = requireNonNull(orcDataSourceId, "orcDataSourceId is null");
        this.dataSize = dataSize;
        this.maxBufferSize = min(bufferSize, dataSize);
    }

    @Override
    public final OrcDataSourceId getOrcDataSourceId()
    {
        return orcDataSourceId;
    }

    @Override
    public long getRetainedSize()
    {
        return buffer == null ? 0 : SizeOf.sizeOf(buffer);
    }

    @Override
    public final int getSize()
    {
        return dataSize;
    }

    @Override
    public final int getMaxBufferSize()
    {
        return maxBufferSize;
    }

    @Override
    public final Slice seekBuffer(int newPosition)
            throws IOException
    {
        int newBufferSize = toIntExact(min(dataSize - newPosition, maxBufferSize));
        if (buffer == null || buffer.length < newBufferSize) {
            buffer = new byte[newBufferSize];
        }

        // is the new position withing the current buffer
        if (newPosition > bufferStartPosition && newPosition < bufferStartPosition + bufferSize) {
            // move existing data to the start of the buffer
            int overlapSize = (bufferStartPosition + bufferSize) - newPosition;
            System.arraycopy(buffer, bufferSize - overlapSize, buffer, 0, overlapSize);

            // fill the remaining part of the buffer
            read(newPosition + overlapSize, buffer, overlapSize, newBufferSize - overlapSize);
        }
        else {
            read(newPosition, buffer, 0, newBufferSize);
        }

        bufferSize = newBufferSize;
        bufferStartPosition = newPosition;
        return Slices.wrappedBuffer(buffer, 0, bufferSize);
    }

    public abstract void read(long position, byte[] buffer, int bufferOffset, int length)
            throws IOException;

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("orcDataSourceId", orcDataSourceId)
                .add("dataSize", dataSize)
                .add("maxBufferSize", maxBufferSize)
                .toString();
    }
}
