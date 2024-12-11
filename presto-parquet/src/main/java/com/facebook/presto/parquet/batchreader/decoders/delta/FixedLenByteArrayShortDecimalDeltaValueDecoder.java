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
package com.facebook.presto.parquet.batchreader.decoders.delta;

import com.facebook.presto.common.type.Decimals;
import com.facebook.presto.parquet.batchreader.decoders.ValuesDecoder.ShortDecimalValuesDecoder;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
import org.openjdk.jol.info.ClassLayout;

import static com.facebook.presto.parquet.ParquetTypeUtils.getShortDecimalValue;
import static com.facebook.presto.parquet.batchreader.decoders.plain.FixedLenByteArrayShortDecimalPlainValuesDecoder.checkBytesFitInShortDecimal;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Note: this is not an optimized values decoder. It makes use of the existing Parquet decoder. Given that this type encoding
 * is not a common one, just use the existing one provided by Parquet library and add a wrapper around it that satisfies the
 * {@link ShortDecimalValuesDecoder} interface.
 */
public class FixedLenByteArrayShortDecimalDeltaValueDecoder
        implements ShortDecimalValuesDecoder
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(FixedLenByteArrayShortDecimalDeltaValueDecoder.class).instanceSize();

    private final ValuesReader delegate;
    private final ColumnDescriptor descriptor;
    private final int typeLength;

    public FixedLenByteArrayShortDecimalDeltaValueDecoder(ValuesReader delegate, ColumnDescriptor descriptor)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.descriptor = requireNonNull(descriptor, "descriptor is null");
        LogicalTypeAnnotation logicalTypeAnnotation = descriptor.getPrimitiveType().getLogicalTypeAnnotation();
        checkArgument(
                logicalTypeAnnotation instanceof DecimalLogicalTypeAnnotation
                        && ((DecimalLogicalTypeAnnotation) logicalTypeAnnotation).getPrecision() <= Decimals.MAX_SHORT_PRECISION,
                "Column %s is not a short decimal",
                descriptor);
        this.typeLength = descriptor.getPrimitiveType().getTypeLength();
        checkArgument(typeLength > 0 && typeLength <= 16, "Expected column %s to have type length in range (1-16)", descriptor);
    }

    @Override
    public void readNext(long[] values, int offset, int length)
    {
        int bytesOffset = 0;
        int bytesLength = typeLength;
        if (typeLength > Long.BYTES) {
            bytesOffset = typeLength - Long.BYTES;
            bytesLength = Long.BYTES;
        }
        for (int i = offset; i < offset + length; i++) {
            byte[] bytes = delegate.readBytes().getBytes();
            checkBytesFitInShortDecimal(bytes, 0, bytesOffset, descriptor);
            values[i] = getShortDecimalValue(bytes, bytesOffset, bytesLength);
        }
    }

    @Override
    public void skip(int length)
    {
        delegate.skip(length);
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE;
    }
}
