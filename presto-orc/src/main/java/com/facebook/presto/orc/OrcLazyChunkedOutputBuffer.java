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
package com.facebook.presto.orc;

import io.airlift.slice.SliceOutput;
import org.openjdk.jol.info.ClassLayout;

import java.util.ArrayList;
import java.util.List;

import static java.lang.Math.min;
import static java.lang.Math.toIntExact;

public class OrcLazyChunkedOutputBuffer
        implements OrcChunkedOutputBuffer
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(ChunkedSliceOutput.class).instanceSize();
    private byte[] buffer;
    private final List<byte[]> closedBuffers = new ArrayList<>();
    private final List<Integer> closedBufferLengths = new ArrayList<>();
    private long closedBuffersRetainedSize;

    /**
     * Offset of buffer within stream.
     */
    private long streamOffset;

    /**
     * Current position for writing in buffer.
     */
    private int bufferPosition;

    @Override
    public void writeTo(SliceOutput outputStream)
    {
        for (int i = 0; i < closedBuffers.size(); i++) {
            outputStream.writeBytes(closedBuffers.get(i), 0, closedBufferLengths.get(i));
        }
        if (bufferPosition > 0) {
            outputStream.writeBytes(buffer, 0, bufferPosition);
        }
    }

    @Override
    public void reset()
    {
        closedBuffers.clear();
        closedBufferLengths.clear();
        closedBuffersRetainedSize = 0;
        streamOffset = 0;
        bufferPosition = 0;
    }

    @Override
    public int size()
    {
        return toIntExact(streamOffset + bufferPosition);
    }

    @Override
    public long getRetainedSize()
    {
        return buffer.length + closedBuffersRetainedSize + INSTANCE_SIZE;
    }

    // need to be called before writing
    @Override
    public void ensureAvailable(int minLength, int length)
    {
        if (buffer == null) {
            buffer = new byte[length];
            bufferPosition = 0;
        }
        // no room for minLength
        if (bufferPosition + minLength > buffer.length) {
            closeChunk(length);
        }
    }

    @Override
    public void writeBytes(byte[] source, int sourceIndex, int length)
    {
        while (length > 0) {
            int batch = ensureBatchSize(length);
            System.arraycopy(source, sourceIndex, buffer, bufferPosition, batch);
            bufferPosition += batch;
            sourceIndex += batch;
            length -= batch;
        }
    }

    @Override
    public void writeHeader(int value)
    {
        buffer[bufferPosition] = (byte) (value & 0x00_00FF);
        bufferPosition += 1;
        buffer[bufferPosition] = (byte) ((value & 0x00_FF00) >> 8);
        bufferPosition += 1;
        buffer[bufferPosition] = (byte) ((value & 0xFF_0000) >> 16);
        bufferPosition += 1;
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder("OrcLazyChunkedOutputBuffer{");
        builder.append("position=").append(size());
        builder.append('}');
        return builder.toString();
    }

    private int ensureBatchSize(int length)
    {
        // no room
        if (bufferPosition >= buffer.length) {
            closeChunk(length);
        }
        return min(length, buffer.length - bufferPosition);
    }

    private void closeChunk(int length)
    {
        // add trimmed view of slice to closed slices
        closedBuffers.add(buffer);
        closedBufferLengths.add(bufferPosition);
        closedBuffersRetainedSize += buffer.length;

        // create a new buffer
        buffer = new byte[length];

        streamOffset += bufferPosition;
        bufferPosition = 0;
    }
}
