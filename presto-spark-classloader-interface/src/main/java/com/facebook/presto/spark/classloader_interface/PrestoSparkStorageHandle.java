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

public class PrestoSparkStorageHandle
        implements Serializable, PrestoSparkTaskOutput
{
    private final byte[] serializedStorageHandle;
    private final long uncompressedSizeInBytes;
    private final long compressedSizeInBytes;
    private final long checksum;
    private final int positionCount;

    public PrestoSparkStorageHandle(
            byte[] serializedStorageHandle,
            long uncompressedSizeInBytes,
            long compressedSizeInBytes,
            long checksum,
            int positionCount)
    {
        this.serializedStorageHandle = requireNonNull(serializedStorageHandle, "serializedStorageHandle is null");
        this.uncompressedSizeInBytes = uncompressedSizeInBytes;
        this.compressedSizeInBytes = compressedSizeInBytes;
        this.checksum = requireNonNull(checksum, "checksum is null");
        this.positionCount = positionCount;
    }

    public long getUncompressedSizeInBytes()
    {
        return uncompressedSizeInBytes;
    }

    public long getCompressedSizeInBytes()
    {
        return compressedSizeInBytes;
    }

    public byte[] getSerializedStorageHandle()
    {
        return serializedStorageHandle;
    }

    public long getChecksum()
    {
        return checksum;
    }

    @Override
    public long getPositionCount()
    {
        return positionCount;
    }

    @Override
    public long getSize()
    {
        return uncompressedSizeInBytes;
    }
}
