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

import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.openjdk.jol.info.ClassLayout;

import static com.facebook.presto.execution.buffer.PageCodecMarker.COMPRESSED;
import static com.facebook.presto.execution.buffer.PageCodecMarker.ENCRYPTED;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

@ThriftStruct
public class SerializedPage
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(SerializedPage.class).instanceSize();

    private final Slice slice;
    private final int positionCount;
    private final int uncompressedSizeInBytes;
    private final byte pageCodecMarkers;

    public SerializedPage(Slice slice, byte pageCodecMarkers, int positionCount, int uncompressedSizeInBytes)
    {
        this.slice = requireNonNull(slice, "slice is null");
        checkArgument(slice.getBase() == null || slice.getBase() instanceof byte[], "serialization type only supports byte[]");
        this.positionCount = positionCount;
        checkArgument(uncompressedSizeInBytes >= 0, "uncompressedSizeInBytes is negative");
        this.uncompressedSizeInBytes = uncompressedSizeInBytes;
        this.pageCodecMarkers = pageCodecMarkers;
        //  Encrypted pages may include arbitrary overhead from ciphers, sanity checks skipped
        if (!ENCRYPTED.isSet(pageCodecMarkers)) {
            if (COMPRESSED.isSet(pageCodecMarkers)) {
                checkArgument(uncompressedSizeInBytes > slice.length(), "compressed size must be smaller than uncompressed size when compressed");
            }
            else {
                checkArgument(uncompressedSizeInBytes == slice.length(), "uncompressed size must be equal to slice length when uncompressed");
            }
        }
    }

    @ThriftConstructor
    public SerializedPage(byte[] data, byte pageCodecMarkers, int positionCount, int uncompressedSizeInBytes)
    {
        this(Slices.wrappedBuffer(data), pageCodecMarkers, positionCount, uncompressedSizeInBytes);
    }

    @ThriftField(1)
    public byte[] getData()
    {
        if (slice.isCompact()) {
            Object base = slice.getBase();
            checkState(base instanceof byte[], "unexpected serialization type %s", base.getClass());
            return (byte[]) base;
        }

        // do a copy
        return slice.getBytes();
    }

    @ThriftField(2)
    public byte getPageCodecMarkers()
    {
        return pageCodecMarkers;
    }

    @ThriftField(3)
    public int getPositionCount()
    {
        return positionCount;
    }

    @ThriftField(4)
    public int getUncompressedSizeInBytes()
    {
        return uncompressedSizeInBytes;
    }

    public int getSizeInBytes()
    {
        return slice.length();
    }

    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + slice.getRetainedSize();
    }

    public Slice getSlice()
    {
        return slice;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("positionCount", positionCount)
                .add("pageCodecMarkers", PageCodecMarker.toSummaryString(pageCodecMarkers))
                .add("sizeInBytes", slice.length())
                .add("uncompressedSizeInBytes", uncompressedSizeInBytes)
                .toString();
    }
}
