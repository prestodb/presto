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
package com.facebook.presto.parquet.batchreader.decoders.plain;

import com.facebook.presto.parquet.batchreader.SimpleSliceInputStream;
import com.facebook.presto.parquet.batchreader.decoders.ValuesDecoder.ShortDecimalValuesDecoder;
import com.facebook.presto.spi.PrestoException;
import io.airlift.slice.Slices;
import org.apache.parquet.column.ColumnDescriptor;
import org.openjdk.jol.info.ClassLayout;

import static com.facebook.presto.parquet.ParquetTypeUtils.getShortDecimalValue;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class FixedLenByteArrayShortDecimalPlainValuesDecoder
        implements ShortDecimalValuesDecoder
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(FixedLenByteArrayShortDecimalPlainValuesDecoder.class).instanceSize();

    private final ColumnDescriptor columnDescriptor;
    private final int typeLength;
    private final ShortDecimalFixedWidthByteArrayBatchDecoder decimalValueDecoder;
    private final SimpleSliceInputStream input;

    public FixedLenByteArrayShortDecimalPlainValuesDecoder(ColumnDescriptor columnDescriptor, byte[] byteBuffer, int bufferOffset, int length)
    {
        this.columnDescriptor = requireNonNull(columnDescriptor, "columnDescriptor is null");
        this.typeLength = columnDescriptor.getPrimitiveType().getTypeLength();
        checkArgument(typeLength > 0 && typeLength <= 16, "Expected column %s to have type length in range (1-16)", columnDescriptor);
        this.decimalValueDecoder = new ShortDecimalFixedWidthByteArrayBatchDecoder(Math.min(typeLength, Long.BYTES));
        input = new SimpleSliceInputStream(Slices.wrappedBuffer(requireNonNull(byteBuffer, "byteBuffer is null"), bufferOffset, length));
    }

    @Override
    public void readNext(long[] values, int offset, int length)
    {
        input.ensureBytesAvailable(typeLength * length);
        if (typeLength <= Long.BYTES) {
            decimalValueDecoder.getShortDecimalValues(input, values, offset, length);
            return;
        }
        int extraBytesLength = typeLength - Long.BYTES;
        byte[] inputBytes = input.getByteArray();
        int inputBytesOffset = input.getByteArrayOffset();
        for (int i = offset; i < offset + length; i++) {
            checkBytesFitInShortDecimal(inputBytes, inputBytesOffset, extraBytesLength, columnDescriptor);
            values[i] = getShortDecimalValue(inputBytes, inputBytesOffset + extraBytesLength, Long.BYTES);
            inputBytesOffset += typeLength;
        }
        input.skip(length * typeLength);
    }

    public static void checkBytesFitInShortDecimal(byte[] bytes, int offset, int length, ColumnDescriptor descriptor)
    {
        int endOffset = offset + length;
        // Equivalent to expectedValue = bytes[endOffset] < 0 ? -1 : 0
        byte expectedValue = (byte) (bytes[endOffset] >> 7);
        for (int i = offset; i < endOffset; i++) {
            if (bytes[i] != expectedValue) {
                throw new PrestoException(NOT_SUPPORTED, "Could not read unscaled value into a short decimal from column " + descriptor);
            }
        }
    }

    @Override
    public void skip(int length)
    {
        checkArgument(length >= 0, "invalid length %s", length);
        input.skip(length * typeLength);
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE;
    }
}
