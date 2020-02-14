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
package com.facebook.presto.client;

import java.util.Base64;

import static com.google.common.base.Preconditions.checkArgument;

public class SerializedData
{
    private byte[] data;
    private byte pageCodecMarker;
    private int positionCount;
    private int uncompressedSizeBytes;

    private static final Base64.Encoder encoder = Base64.getEncoder();
    private static final Base64.Decoder decoder = Base64.getDecoder();

    public SerializedData(byte[] data, byte pageCodecMarker, int positionCount, int uncompressedSizeBytes)
    {
        this.data = data;
        this.pageCodecMarker = pageCodecMarker;
        this.positionCount = positionCount;
        this.uncompressedSizeBytes = uncompressedSizeBytes;
    }

    public static SerializedData getSerializedData(String input)
    {
        String[] items = input.split("\\|");
        checkArgument(items.length == 4, "Should have only 4 items in encoded string");
        return new SerializedData(
                decoder.decode(items[3]),
                Byte.valueOf(items[0]),
                Integer.valueOf(items[1]),
                Integer.valueOf(items[2]));
    }

    public byte[] getData()
    {
        return data;
    }

    public byte getPageCodecMarker()
    {
        return pageCodecMarker;
    }

    public int getPositionCount()
    {
        return positionCount;
    }

    public int getUncompressedSizeBytes()
    {
        return uncompressedSizeBytes;
    }

    @Override
    public String toString()
    {
        return pageCodecMarker +
                "|" +
                positionCount +
                "|" +
                uncompressedSizeBytes +
                "|" +
                encoder.encodeToString(data);
    }
}
