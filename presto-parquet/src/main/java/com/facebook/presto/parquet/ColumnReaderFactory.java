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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.parquet.batchreader.BinaryFlatBatchReader;
import com.facebook.presto.parquet.batchreader.BinaryNestedBatchReader;
import com.facebook.presto.parquet.batchreader.BooleanFlatBatchReader;
import com.facebook.presto.parquet.batchreader.BooleanNestedBatchReader;
import com.facebook.presto.parquet.batchreader.Int32FlatBatchReader;
import com.facebook.presto.parquet.batchreader.Int32NestedBatchReader;
import com.facebook.presto.parquet.batchreader.Int64FlatBatchReader;
import com.facebook.presto.parquet.batchreader.Int64NestedBatchReader;
import com.facebook.presto.parquet.batchreader.Int64TimeAndTimestampMicrosFlatBatchReader;
import com.facebook.presto.parquet.batchreader.Int64TimeAndTimestampMicrosNestedBatchReader;
import com.facebook.presto.parquet.batchreader.LongDecimalFlatBatchReader;
import com.facebook.presto.parquet.batchreader.ShortDecimalFlatBatchReader;
import com.facebook.presto.parquet.batchreader.TimestampFlatBatchReader;
import com.facebook.presto.parquet.batchreader.TimestampNestedBatchReader;
import com.facebook.presto.parquet.reader.AbstractColumnReader;
import com.facebook.presto.parquet.reader.BinaryColumnReader;
import com.facebook.presto.parquet.reader.BooleanColumnReader;
import com.facebook.presto.parquet.reader.DoubleColumnReader;
import com.facebook.presto.parquet.reader.FloatColumnReader;
import com.facebook.presto.parquet.reader.IntColumnReader;
import com.facebook.presto.parquet.reader.LongColumnReader;
import com.facebook.presto.parquet.reader.LongDecimalColumnReader;
import com.facebook.presto.parquet.reader.LongTimeMicrosColumnReader;
import com.facebook.presto.parquet.reader.LongTimestampMicrosColumnReader;
import com.facebook.presto.parquet.reader.ShortDecimalColumnReader;
import com.facebook.presto.parquet.reader.TimestampColumnReader;
import com.facebook.presto.spi.PrestoException;
import org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;

import java.util.Optional;

import static com.facebook.presto.parquet.ParquetTypeUtils.isDecimalType;
import static com.facebook.presto.parquet.ParquetTypeUtils.isShortDecimalType;
import static com.facebook.presto.parquet.ParquetTypeUtils.isTimeMicrosType;
import static com.facebook.presto.parquet.ParquetTypeUtils.isTimeStampMicrosType;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;

public class ColumnReaderFactory
{
    private static final Logger log = Logger.get(ColumnReaderFactory.class);
    private ColumnReaderFactory()
    {
    }

    public static ColumnReader createReader(RichColumnDescriptor descriptor, boolean batchReadEnabled)
    {
        if (batchReadEnabled) {
            final boolean isNested = descriptor.getPath().length > 1;
            switch (descriptor.getPrimitiveType().getPrimitiveTypeName()) {
                case BOOLEAN:
                    return isNested ? new BooleanNestedBatchReader(descriptor) : new BooleanFlatBatchReader(descriptor);
                case INT32:
                    if (!isNested && isShortDecimalType(descriptor)) {
                        return new ShortDecimalFlatBatchReader(descriptor);
                    }
                case FLOAT:
                    return isNested ? new Int32NestedBatchReader(descriptor) : new Int32FlatBatchReader(descriptor);
                case INT64:
                    if (isTimeStampMicrosType(descriptor) || isTimeMicrosType(descriptor)) {
                        return isNested ? new Int64TimeAndTimestampMicrosNestedBatchReader(descriptor) : new Int64TimeAndTimestampMicrosFlatBatchReader(descriptor);
                    }

                    if (!isNested && isShortDecimalType(descriptor)) {
                        int precision = ((DecimalLogicalTypeAnnotation) descriptor.getPrimitiveType().getLogicalTypeAnnotation()).getPrecision();
                        if (precision < 10) {
                            log.warn("PrimitiveTypeName is INT64 but precision is less then 10.");
                        }
                        return new ShortDecimalFlatBatchReader(descriptor);
                    }
                case DOUBLE:
                    return isNested ? new Int64NestedBatchReader(descriptor) : new Int64FlatBatchReader(descriptor);
                case INT96:
                    return isNested ? new TimestampNestedBatchReader(descriptor) : new TimestampFlatBatchReader(descriptor);
                case BINARY:
                    Optional<ColumnReader> decimalBatchColumnReader = createDecimalBatchColumnReader(descriptor);
                    if (decimalBatchColumnReader.isPresent()) {
                        return decimalBatchColumnReader.get();
                    }

                    return isNested ? new BinaryNestedBatchReader(descriptor) : new BinaryFlatBatchReader(descriptor);
                case FIXED_LEN_BYTE_ARRAY:
                    if (!isNested) {
                        decimalBatchColumnReader = createDecimalBatchColumnReader(descriptor);
                        if (decimalBatchColumnReader.isPresent()) {
                            return decimalBatchColumnReader.get();
                        }
                    }
            }
        }

        switch (descriptor.getPrimitiveType().getPrimitiveTypeName()) {
            case BOOLEAN:
                return new BooleanColumnReader(descriptor);
            case INT32:
                return createDecimalColumnReader(descriptor).orElse(new IntColumnReader(descriptor));
            case INT64:
                if (isTimeStampMicrosType(descriptor)) {
                    return new LongTimestampMicrosColumnReader(descriptor);
                }
                if (isTimeMicrosType(descriptor)) {
                    return new LongTimeMicrosColumnReader(descriptor);
                }
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

    private static Optional<ColumnReader> createDecimalBatchColumnReader(RichColumnDescriptor descriptor)
    {
        if (isDecimalType(descriptor)) {
            if (isShortDecimalType(descriptor)) {
                return Optional.of(new ShortDecimalFlatBatchReader(descriptor));
            }
            return Optional.of(new LongDecimalFlatBatchReader(descriptor));
        }
        return Optional.empty();
    }

    private static Optional<AbstractColumnReader> createDecimalColumnReader(RichColumnDescriptor descriptor)
    {
        if (isDecimalType(descriptor)) {
            if (isShortDecimalType(descriptor)) {
                return Optional.of(new ShortDecimalColumnReader(descriptor));
            }
            return Optional.of(new LongDecimalColumnReader(descriptor));
        }
        return Optional.empty();
    }
}
