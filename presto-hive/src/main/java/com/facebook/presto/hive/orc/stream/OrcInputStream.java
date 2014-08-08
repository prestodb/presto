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
package com.facebook.presto.hive.orc.stream;

import com.facebook.presto.hive.shaded.org.iq80.snappy.Snappy;
import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.Slices;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.CompressionKind;

import java.io.IOException;
import java.io.InputStream;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static io.airlift.slice.Slices.EMPTY_SLICE;
import static org.apache.hadoop.hive.ql.io.orc.OrcProto.CompressionKind.NONE;
import static org.apache.hadoop.hive.ql.io.orc.OrcProto.CompressionKind.SNAPPY;
import static org.apache.hadoop.hive.ql.io.orc.OrcProto.CompressionKind.ZLIB;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;

public final class OrcInputStream
        extends InputStream
{
    public static final int BLOCK_HEADER_SIZE = 3;

    private final SliceInput compressedSliceInput;
    private final CompressionKind compressionKind;
    private final int bufferSize;
    private BasicSliceInput current;
    private Slice buffer;

    public OrcInputStream(BasicSliceInput sliceInput, CompressionKind compressionKind, int bufferSize)
            throws IOException
    {
        checkNotNull(sliceInput, "sliceInput is null");

        this.compressionKind = checkNotNull(compressionKind, "compressionKind is null");
        this.bufferSize = bufferSize;

        if (compressionKind == NONE) {
            this.current = sliceInput;
            this.compressedSliceInput = EMPTY_SLICE.getInput();
        }
        else {
            checkArgument(compressionKind == SNAPPY || compressionKind == ZLIB, "%s compression not supported", compressionKind);
            this.compressedSliceInput = checkNotNull(sliceInput, "compressedSliceInput is null");
            advance();
        }
    }

    @Override
    public void close()
            throws IOException
    {
        if (current == null) {
            return;
        }

        try {
            current.close();
        }
        finally {
            current = null;
        }
    }

    @Override
    public int available()
            throws IOException
    {
        if (current == null) {
            return 0;
        }
        return current.available();
    }

    @Override
    public boolean markSupported()
    {
        return false;
    }

    @Override
    public int read()
            throws IOException
    {
        if (current == null) {
            return -1;
        }

        int result = current.read();
        if (result != -1) {
            return result;
        }

        advance();
        return read();
    }

    @Override
    public int read(byte[] b, int off, int length)
            throws IOException
    {
        if (current == null) {
            return -1;
        }

        if (!current.isReadable()) {
            advance();
            if (current == null) {
                return -1;
            }
        }

        return current.read(b, off, length);
    }

    @Override
    public long skip(long n)
            throws IOException
    {
        if (current == null || n <= 0) {
            return 0;
        }

        long result = current.skip(n);
        if (result != 0) {
            return result;
        }
        if (read() == -1) {
            return 0;
        }
        return 1 + current.skip(n - 1);
    }

    private void advance()
            throws IOException
    {
        if (compressedSliceInput == null || compressedSliceInput.available() == 0) {
            current = null;
            return;
        }

        // 3 byte header
        // NOTE: this must match BLOCK_HEADER_SIZE
        int b0 = compressedSliceInput.readUnsignedByte();
        int b1 = compressedSliceInput.readUnsignedByte();
        int b2 = compressedSliceInput.readUnsignedByte();

        boolean isUncompressed = (b0 & 0x01) == 1;
        int chunkLength = (b2 << 15) | (b1 << 7) | (b0 >> 1);

        Slice chunk = compressedSliceInput.readSlice(chunkLength);

        if (isUncompressed) {
            current = chunk.getInput();
        }
        else {
            if (buffer == null) {
                buffer = Slices.allocate(bufferSize);
            }

            int uncompressedSize;
            if (compressionKind == ZLIB) {
                uncompressedSize = decompressZip(chunk, buffer);
            }
            else {
                uncompressedSize = decompressSnappy(chunk, buffer);
            }

            current = buffer.slice(0, uncompressedSize).getInput();
        }
    }

    public static int decompressZip(Slice in, Slice buffer)
            throws IOException
    {
        byte[] outArray = (byte[]) buffer.getBase();
        int outOffset = 0;

        byte[] inArray = (byte[]) in.getBase();
        int inOffset = (int) (in.getAddress() - ARRAY_BYTE_BASE_OFFSET);
        int inLength = in.length();

        Inflater inflater = new Inflater(true);
        inflater.setInput(inArray, inOffset, inLength);
        while (!(inflater.finished() || inflater.needsDictionary() || inflater.needsInput())) {
            try {
                int count = inflater.inflate(outArray, outOffset, outArray.length - outOffset);
                outOffset += count;
            }
            catch (DataFormatException e) {
                throw new IOException("Invalid compressed stream", e);
            }
        }
        inflater.end();
        return outOffset;
    }

    public static int decompressSnappy(Slice in, Slice buffer)
            throws IOException
    {
        byte[] outArray = (byte[]) buffer.getBase();

        byte[] inArray = (byte[]) in.getBase();
        int inOffset = (int) (in.getAddress() - ARRAY_BYTE_BASE_OFFSET);
        int inLength = in.length();

        return Snappy.uncompress(inArray, inOffset, inLength, outArray, 0);
    }
}
