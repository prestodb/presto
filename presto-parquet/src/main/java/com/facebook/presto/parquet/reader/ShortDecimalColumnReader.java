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
package com.facebook.presto.parquet.reader;

import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.parquet.RichColumnDescriptor;
import com.facebook.presto.spi.PrestoException;

import java.math.BigDecimal;
import java.math.BigInteger;

import static com.facebook.presto.parquet.ParquetTypeUtils.getShortDecimalValue;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;

public class ShortDecimalColumnReader
        extends AbstractColumnReader
{
    private final int typeLength;

    public ShortDecimalColumnReader(RichColumnDescriptor descriptor)
    {
        super(descriptor);
        typeLength = descriptor.getPrimitiveType().getTypeLength();
        checkArgument(typeLength <= 16, "Type length %s should be <= 16 for short decimal column %s", typeLength, descriptor);
    }

    @Override
    protected void readValue(BlockBuilder blockBuilder, Type type)
    {
        if (definitionLevel == columnDescriptor.getMaxDefinitionLevel()) {
            long decimalValue;
            // When decimals are encoded with primitive types Parquet stores unscaled values
            if (columnDescriptor.getPrimitiveType().getPrimitiveTypeName().equals(INT32)) {
                decimalValue = valuesReader.readInteger();
            }
            else if (columnDescriptor.getPrimitiveType().getPrimitiveTypeName().equals(INT64)) {
                decimalValue = valuesReader.readLong();
            }
            else {
                byte[] bytes = valuesReader.readBytes().getBytes();
                if (typeLength <= Long.BYTES) {
                    decimalValue = getShortDecimalValue(bytes);
                }
                else {
                    int startOffset = bytes.length - Long.BYTES;
                    checkBytesFitInShortDecimal(bytes, startOffset, type);
                    decimalValue = getShortDecimalValue(bytes, startOffset, Long.BYTES);
                }
            }
            type.writeLong(blockBuilder, decimalValue);
        }
        else if (isValueNull()) {
            blockBuilder.appendNull();
        }
    }

    @Override
    protected void skipValue()
    {
        if (definitionLevel == columnDescriptor.getMaxDefinitionLevel()) {
            if (columnDescriptor.getType().equals(INT32)) {
                valuesReader.readInteger();
            }
            else if (columnDescriptor.getType().equals(INT64)) {
                valuesReader.readLong();
            }
            else {
                valuesReader.readBytes();
            }
        }
    }

    private void checkBytesFitInShortDecimal(byte[] bytes, int endOffset, Type type)
    {
        // Equivalent to expectedValue = bytes[endOffset] < 0 ? -1 : 0
        byte expectedValue = (byte) (bytes[endOffset] >> 7);
        for (int i = 0; i < endOffset; i++) {
            if (bytes[i] != expectedValue) {
                BigDecimal bigDecimal = type instanceof DecimalType ?
                        new BigDecimal(new BigInteger(bytes), ((DecimalType) type).getScale()) : new BigDecimal(new BigInteger(bytes));
                throw new PrestoException(NOT_SUPPORTED, format(
                        "Could not read fixed_len_byte_array(%d) value %s into %s",
                        typeLength,
                        bigDecimal,
                        type));
            }
        }
    }
}
