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
package com.facebook.presto.parquet;

import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.parquet.reader.AbstractColumnReader;
import com.facebook.presto.parquet.reader.BinaryColumnReader;
import com.facebook.presto.parquet.reader.BooleanColumnReader;
import com.facebook.presto.parquet.reader.DoubleColumnReader;
import com.facebook.presto.parquet.reader.FloatColumnReader;
import com.facebook.presto.parquet.reader.IntColumnReader;
import com.facebook.presto.parquet.reader.LongColumnReader;
import com.facebook.presto.parquet.reader.LongDecimalColumnReader;
import com.facebook.presto.parquet.reader.ShortDecimalColumnReader;
import com.facebook.presto.parquet.reader.TimestampColumnReader;
import com.facebook.presto.spi.PrestoException;

import java.util.Optional;

import static com.facebook.presto.parquet.ParquetTypeUtils.createDecimalType;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;

public class ColumnReaderFactory
{
    private ColumnReaderFactory()
    {
    }

    public static ColumnReader createReader(RichColumnDescriptor descriptor)
    {
        switch (descriptor.getPrimitiveType().getPrimitiveTypeName()) {
            case BOOLEAN:
                return new BooleanColumnReader(descriptor);
            case INT32:
                return createDecimalColumnReader(descriptor).orElse(new IntColumnReader(descriptor));
            case INT64:
                return createDecimalColumnReader(descriptor).orElse(new LongColumnReader(descriptor));
            case INT96:
                return new TimestampColumnReader(descriptor);
            case FLOAT:
                return new FloatColumnReader(descriptor);
            case DOUBLE:
                return new DoubleColumnReader(descriptor);
            case BINARY:
                return createDecimalColumnReader(descriptor).orElse(new BinaryColumnReader(descriptor));
            case FIXED_LEN_BYTE_ARRAY:
                return createDecimalColumnReader(descriptor)
                        .orElseThrow(() -> new PrestoException(NOT_SUPPORTED, " type FIXED_LEN_BYTE_ARRAY supported as DECIMAL; got " + descriptor.getPrimitiveType().getOriginalType()));
            default:
                throw new PrestoException(NOT_SUPPORTED, "Unsupported parquet type: " + descriptor.getType());
        }
    }

    private static Optional<AbstractColumnReader> createDecimalColumnReader(RichColumnDescriptor descriptor)
    {
        Optional<Type> type = createDecimalType(descriptor);
        if (type.isPresent()) {
            DecimalType decimalType = (DecimalType) type.get();
            if (decimalType.isShort()) {
                return Optional.of(new ShortDecimalColumnReader(descriptor));
            }
            else {
                return Optional.of(new LongDecimalColumnReader(descriptor));
            }
        }
        return Optional.empty();
    }
}
