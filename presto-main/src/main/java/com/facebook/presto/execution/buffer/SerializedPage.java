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
package com.facebook.presto.execution.buffer;

import com.facebook.presto.spi.block.ConcatenatedByteArrayInputStream;
import com.facebook.presto.spi.memory.ByteArrayPool;
import io.airlift.slice.Slice;
import org.openjdk.jol.info.ClassLayout;

import static com.facebook.presto.execution.buffer.PageCompression.COMPRESSED;
import static com.facebook.presto.execution.buffer.PageCompression.UNCOMPRESSED;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class SerializedPage
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(SerializedPage.class).instanceSize();
    private static final int PAGE_COMPRESSION_SIZE = ClassLayout.parseClass(PageCompression.class).instanceSize();

    private Slice slice;
    private final PageCompression compression;
    private final int positionCount;
    private final int uncompressedSizeInBytes;
    private ByteArrayPool pool;
    private final ConcatenatedByteArrayInputStream stream;
    private final long position;

    public SerializedPage(Slice slice, PageCompression compression, int positionCount, int uncompressedSizeInBytes)
    {
        this.slice = requireNonNull(slice, "slice is null");
        this.compression = requireNonNull(compression, "compression is null");
        this.positionCount = positionCount;
        checkArgument(uncompressedSizeInBytes >= 0, "uncompressedSizeInBytes is negative");
        checkArgument(compression == UNCOMPRESSED || uncompressedSizeInBytes > slice.length(), "compressed size must be smaller than uncompressed size when compressed");
        checkArgument(compression == COMPRESSED || uncompressedSizeInBytes == slice.length(), "uncompressed size must be equal to slice length when uncompressed");
        this.uncompressedSizeInBytes = uncompressedSizeInBytes;
        stream = null;
        position = 0;
    }

    public SerializedPage(ConcatenatedByteArrayInputStream stream, long position, PageCompression compression, int positionCount, int uncompressedSizeInBytes)
    {
        this.stream = stream;
        this.position = position;
        this.slice = null;
        this.compression = requireNonNull(compression, "compression is null");
        this.positionCount = positionCount;
        checkArgument(uncompressedSizeInBytes >= 0, "uncompressedSizeInBytes is negative");
        this.uncompressedSizeInBytes = uncompressedSizeInBytes;
    }

    public void setByteArrayPool(ByteArrayPool pool)
    {
        this.pool = pool;
    }

    public int getSizeInBytes()
    {
        return slice != null ? slice.length() : uncompressedSizeInBytes;
    }

    public int getUncompressedSizeInBytes()
    {
        return uncompressedSizeInBytes;
    }

    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + (slice != null ? slice.getRetainedSize() : uncompressedSizeInBytes) + PAGE_COMPRESSION_SIZE;
    }

    public int getPositionCount()
    {
        return positionCount;
    }

    public Slice getSlice()
    {
        return slice;
    }

    public PageCompression getCompression()
    {
        return compression;
    }

    void dereferenced()
    {
        if (pool != null) {
            pool.release((byte[]) slice.getBase());
            slice = null;
        }
    }

    ConcatenatedByteArrayInputStream getStream()
    {
        return stream;
    }

    long getPosition()
    {
        return position;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("positionCount", positionCount)
                .add("compression", compression)
                .add("sizeInBytes", slice.length())
                .add("uncompressedSizeInBytes", uncompressedSizeInBytes)
                .toString();
    }
}
