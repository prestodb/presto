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

import com.facebook.presto.parquet.batchreader.decoders.ValuesDecoder.BooleanValuesDecoder;
import org.apache.parquet.io.ParquetDecodingException;
import org.openjdk.jol.info.ClassLayout;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class BooleanRLEValuesDecoder
        extends GenericRLEDictionaryValuesDecoder
        implements BooleanValuesDecoder
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(BooleanRLEValuesDecoder.class).instanceSize();

    private final ByteBuffer inputBuffer;

    public BooleanRLEValuesDecoder(ByteBuffer inputBuffer)
    {
        super(Integer.MAX_VALUE, 1, new ByteArrayInputStream(inputBuffer.array(), inputBuffer.arrayOffset() + inputBuffer.position(), inputBuffer.remaining()));
        this.inputBuffer = requireNonNull(inputBuffer);
        currentBuffer = null;
        mode = Mode.RLE;
    }

    @Override
    public void readNext(byte[] values, int offset, int length)
    {
        int destinationIndex = offset;
        int remainingToCopy = length;
        while (remainingToCopy > 0) {
            if (getCurrentCount() == 0) {
                try {
                    if (!decode()) {
                        break;
                    }
                }
                catch (IOException e) {
                    throw new ParquetDecodingException("Error decoding boolean values", e);
                }
            }

            int numEntriesToFill = Math.min(remainingToCopy, getCurrentCount());
            int endIndex = destinationIndex + numEntriesToFill;
            byte rleValue = (byte) getDecodedInt();
            while (destinationIndex < endIndex) {
                values[destinationIndex++] = rleValue;
            }
            decrementCurrentCount(numEntriesToFill);
            remainingToCopy -= numEntriesToFill;
        }
    }

    @Override
    public void skip(int length)
    {
        int remainingToSkip = length;
        while (remainingToSkip > 0) {
            if (getCurrentCount() == 0) {
                try {
                    if (!decode()) {
                        break;
                    }
                }
                catch (IOException e) {
                    throw new ParquetDecodingException("Error skipping boolean values", e);
                }
            }

            int numEntriesToSkip = Math.min(remainingToSkip, getCurrentCount());
            decrementCurrentCount(numEntriesToSkip);
            remainingToSkip -= numEntriesToSkip;
        }
        checkState(remainingToSkip == 0, "Invalid skip size request");
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + inputBuffer.array().length;
    }
}
