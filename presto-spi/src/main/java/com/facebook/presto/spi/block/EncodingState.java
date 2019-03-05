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
package com.facebook.presto.spi.block;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import static java.nio.charset.StandardCharsets.UTF_8;

public class EncodingState
{
    int numValues;
    int maxValues;
    boolean anyNulls;
    Slice topLevelBuffer;
    int startInBuffer;
    int newStartInBuffer;
    int bytesInBuffer;
    int valueOffset;
    String encodingName;

    public int getBytesInBuffer()
    {
        return bytesInBuffer;
    }

    public void setBuffer(Slice buffer)
    {
        numValues = 0;
        anyNulls = false;
        topLevelBuffer = buffer;
        byte[] nameBytes = encodingName.getBytes(UTF_8);
        buffer.setInt(startInBuffer, nameBytes.length);
        buffer.setBytes(startInBuffer + 4, Slices.wrappedBuffer(nameBytes));
        valueOffset = startInBuffer + 4 + nameBytes.length;
    }
}
