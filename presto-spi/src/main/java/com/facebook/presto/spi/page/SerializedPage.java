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
package com.facebook.presto.spi.page;

import io.airlift.slice.Slice;
import org.openjdk.jol.info.ClassLayout;

import java.util.Objects;

import static com.facebook.presto.spi.page.PageCodecMarker.COMPRESSED;
import static com.facebook.presto.spi.page.PageCodecMarker.ENCRYPTED;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class SerializedPage
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(SerializedPage.class).instanceSize();

    private final Slice slice;
    private final int positionCount;
    private final int uncompressedSizeInBytes;
    private final byte pageCodecMarkers;
    private final long checksum;

    public SerializedPage(
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

    public byte getPageCodecMarkers()
    {
        return pageCodecMarkers;
    }

    public int getPositionCount()
    {
        return positionCount;
    }

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

    public long getChecksum()
    {
        return checksum;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SerializedPage that = (SerializedPage) o;
        return Objects.equals(slice, that.slice) &&
                Objects.equals(positionCount, that.positionCount) &&
                Objects.equals(uncompressedSizeInBytes, that.uncompressedSizeInBytes) &&
                Objects.equals(pageCodecMarkers, that.pageCodecMarkers) &&
                Objects.equals(checksum, that.checksum);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(slice, positionCount, uncompressedSizeInBytes, pageCodecMarkers, checksum);
    }

    private static void checkArgument(boolean condition, String message, Object... messageArgs)
    {
        if (!condition) {
            throw new IllegalArgumentException(format(message, messageArgs));
        }
    }
}
