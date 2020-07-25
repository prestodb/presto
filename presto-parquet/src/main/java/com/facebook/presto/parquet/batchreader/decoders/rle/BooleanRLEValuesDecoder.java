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
package com.facebook.presto.parquet.batchreader.decoders.rle;

import com.facebook.presto.parquet.batchreader.BytesUtils;
import com.facebook.presto.parquet.batchreader.decoders.ValuesDecoder.BooleanValuesDecoder;
import org.apache.parquet.Preconditions;
import org.apache.parquet.io.ParquetDecodingException;

import java.nio.ByteBuffer;

import static com.google.common.base.Preconditions.checkState;

public class BooleanRLEValuesDecoder
        implements BooleanValuesDecoder
{
    private final ByteBuffer inputBuffer;

    private MODE mode;
    private int currentCount;
    private byte currentValue;
    private int currentByteOffset;
    private byte currentByte;

    public BooleanRLEValuesDecoder(ByteBuffer inputBuffer)
    {
        this.inputBuffer = inputBuffer;
    }

    // Copied from BytesUtils.readUnsignedVarInt(InputStream in)
    public static int readUnsignedVarInt(ByteBuffer in)
    {
        int value = 0;

        int i;
        int b = in.get();
        for (i = 0; (b & 128) != 0; i += 7) {
            value |= (b & 127) << i;
            b = in.get();
        }

        return value | b << i;
    }

    @Override
    public void readNext(byte[] values, int offset, int length)
    {
        int destinationIndex = offset;
        int remainingToCopy = length;
        while (remainingToCopy > 0) {
            if (currentCount == 0) {
                readNext();
                if (currentCount == 0) {
                    break;
                }
            }

            int numEntriesToFill = Math.min(remainingToCopy, currentCount);
            int endIndex = destinationIndex + numEntriesToFill;
            switch (mode) {
                case RLE: {
                    byte rleValue = currentValue;
                    while (destinationIndex < endIndex) {
                        values[destinationIndex] = rleValue;
                        destinationIndex++;
                    }
                    break;
                }
                case PACKED: {
                    int remainingPackedBlock = numEntriesToFill;
                    if (currentByteOffset > 0) {
                        // read from the partial values remaining in current byte
                        int readChunk = Math.min(remainingPackedBlock, 8 - currentByteOffset);

                        final byte inValue = currentByte;
                        for (int i = 0; i < readChunk; i++) {
                            values[destinationIndex++] = (byte) (inValue >> currentByteOffset & 1);
                            currentByteOffset++;
                        }

                        remainingPackedBlock -= readChunk;
                        currentByteOffset = currentByteOffset % 8;
                    }

                    final ByteBuffer localInputBuffer = inputBuffer;
                    while (remainingPackedBlock >= 8) {
                        BytesUtils.unpack8Values(localInputBuffer.get(), values, destinationIndex);
                        remainingPackedBlock -= 8;
                        destinationIndex += 8;
                    }

                    if (remainingPackedBlock > 0) {
                        // read partial values from current byte until the requested length is satisfied
                        byte inValue = localInputBuffer.get();
                        for (int i = 0; i < remainingPackedBlock; i++) {
                            values[destinationIndex++] = (byte) (inValue >> i & 1);
                        }

                        currentByte = inValue;
                        currentByteOffset = remainingPackedBlock;
                    }

                    break;
                }
                default:
                    throw new ParquetDecodingException("not a valid mode " + mode);
            }
            currentCount -= numEntriesToFill;
            remainingToCopy -= numEntriesToFill;
        }
    }

    @Override
    public void skip(int length)
    {
        int remainingToSkip = length;
        while (remainingToSkip > 0) {
            if (currentCount == 0) {
                readNext();
                if (currentCount == 0) {
                    break;
                }
            }

            int numEntriesToSkip = Math.min(remainingToSkip, currentCount);
            switch (mode) {
                case RLE:
                    break;
                case PACKED: {
                    int remainingPackedBlock = numEntriesToSkip;
                    if (currentByteOffset > 0) {
                        // read from the partial values remaining in current byte
                        int skipChunk = Math.min(remainingPackedBlock, 8 - currentByteOffset);

                        currentByteOffset += skipChunk;

                        remainingPackedBlock -= skipChunk;
                        currentByteOffset = currentByteOffset % 8;
                    }

                    int fullBytes = remainingPackedBlock / 8;

                    if (fullBytes > 0) {
                        inputBuffer.position(inputBuffer.position() + fullBytes);
                    }

                    remainingPackedBlock = remainingPackedBlock % 8;

                    if (remainingPackedBlock > 0) {
                        // read partial values from current byte until the requested length is satisfied
                        currentByte = inputBuffer.get();
                        currentByteOffset = remainingPackedBlock;
                    }

                    break;
                }
                default:
                    throw new ParquetDecodingException("not a valid mode " + mode);
            }
            currentCount -= numEntriesToSkip;
            remainingToSkip -= numEntriesToSkip;
        }
        checkState(remainingToSkip == 0, "Invalid read size request");
    }

    private void readNext()
    {
        Preconditions.checkArgument(inputBuffer.hasRemaining(), "Reading past RLE/BitPacking stream.");
        int header = readUnsignedVarInt(inputBuffer);
        mode = (header & 1) == 0 ? MODE.RLE : MODE.PACKED;
        switch (mode) {
            case RLE:
                currentCount = header >>> 1;
                currentValue = inputBuffer.get();
                return;
            case PACKED:
                int numGroups = header >>> 1;
                currentCount = numGroups * 8;
                currentByteOffset = 0;
                return;
            default:
                throw new ParquetDecodingException("not a valid mode " + mode);
        }
    }

    private enum MODE
    {
        RLE,
        PACKED;
    }
}
