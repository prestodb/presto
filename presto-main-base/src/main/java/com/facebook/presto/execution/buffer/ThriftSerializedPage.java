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
import com.facebook.presto.spi.page.SerializedPage;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import static com.facebook.presto.spi.page.PageCodecMarker.COMPRESSED;
import static com.facebook.presto.spi.page.PageCodecMarker.ENCRYPTED;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

@ThriftStruct
public class ThriftSerializedPage
{
    private final Slice slice;
    private final int positionCount;
    private final int uncompressedSizeInBytes;
    private final byte pageCodecMarkers;
    private final long checksum;

    public ThriftSerializedPage(SerializedPage serializedPage)
    {
        this(
                serializedPage.getSlice(),
                serializedPage.getPageCodecMarkers(),
                serializedPage.getPositionCount(),
                serializedPage.getUncompressedSizeInBytes(),
                serializedPage.getChecksum());
    }

    private ThriftSerializedPage(
            Slice slice,
            byte pageCodecMarkers,
            int positionCount,
            int uncompressedSizeInBytes,
            long checksum)
    {
        this.slice = requireNonNull(slice, "slice is null");
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
        this.checksum = checksum;
    }

    /**
     * Thrift deserialization only, there should be no explicit call on this method.
     */
    @ThriftConstructor
    public ThriftSerializedPage(
            byte[] data,
            byte pageCodecMarkers,
            int positionCount,
            int uncompressedSizeInBytes,
            long checksum)
    {
        this(Slices.wrappedBuffer(data), pageCodecMarkers, positionCount, uncompressedSizeInBytes, checksum);
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

    @ThriftField(5)
    public long getChecksum()
    {
        return checksum;
    }

    public SerializedPage toSerializedPage()
    {
        return new SerializedPage(slice, pageCodecMarkers, positionCount, uncompressedSizeInBytes, checksum);
    }
}
