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
package com.facebook.presto.spark.classloader_interface;

import java.io.Serializable;

import static java.util.Objects.requireNonNull;

public class PrestoSparkSerializedPage
        implements Serializable, PrestoSparkTaskOutput
{
    private final byte[] bytes;
    private final int positionCount;
    private final int uncompressedSizeInBytes;
    private final byte pageCodecMarkers;
    private final long checksum;

    public PrestoSparkSerializedPage(byte[] bytes, int positionCount, int uncompressedSizeInBytes, byte pageCodecMarkers, long checksum)
    {
        this.bytes = requireNonNull(bytes, "bytes is null");
        this.positionCount = positionCount;
        this.uncompressedSizeInBytes = uncompressedSizeInBytes;
        this.pageCodecMarkers = pageCodecMarkers;
        this.checksum = checksum;
    }

    public byte[] getBytes()
    {
        return bytes;
    }

    @Override
    public long getPositionCount()
    {
        return positionCount;
    }

    public int getUncompressedSizeInBytes()
    {
        return uncompressedSizeInBytes;
    }

    public byte getPageCodecMarkers()
    {
        return pageCodecMarkers;
    }

    public long getChecksum()
    {
        return checksum;
    }

    @Override
    public long getSize()
    {
        return bytes.length;
    }
}
