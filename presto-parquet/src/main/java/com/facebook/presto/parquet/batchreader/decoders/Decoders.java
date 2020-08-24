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
package com.facebook.presto.parquet.batchreader.decoders;

import com.facebook.presto.parquet.DataPage;
import com.facebook.presto.parquet.DataPageV1;
import com.facebook.presto.parquet.DataPageV2;
import com.facebook.presto.parquet.ParquetEncoding;
import com.facebook.presto.parquet.RichColumnDescriptor;
import com.facebook.presto.parquet.batchreader.decoders.delta.BinaryDeltaValuesDecoder;
import com.facebook.presto.parquet.batchreader.decoders.delta.Int32DeltaBinaryPackedValuesDecoder;
import com.facebook.presto.parquet.batchreader.decoders.delta.Int64DeltaBinaryPackedValuesDecoder;
import com.facebook.presto.parquet.batchreader.decoders.delta.Int64TimestampMicrosDeltaBinaryPackedValuesDecoder;
import com.facebook.presto.parquet.batchreader.decoders.plain.BinaryPlainValuesDecoder;
import com.facebook.presto.parquet.batchreader.decoders.plain.BooleanPlainValuesDecoder;
import com.facebook.presto.parquet.batchreader.decoders.plain.Int32PlainValuesDecoder;
import com.facebook.presto.parquet.batchreader.decoders.plain.Int64PlainValuesDecoder;
import com.facebook.presto.parquet.batchreader.decoders.plain.Int64TimestampMicrosPlainValuesDecoder;
import com.facebook.presto.parquet.batchreader.decoders.plain.TimestampPlainValuesDecoder;
import com.facebook.presto.parquet.batchreader.decoders.rle.BinaryRLEDictionaryValuesDecoder;
import com.facebook.presto.parquet.batchreader.decoders.rle.BooleanRLEValuesDecoder;
import com.facebook.presto.parquet.batchreader.decoders.rle.Int32RLEDictionaryValuesDecoder;
import com.facebook.presto.parquet.batchreader.decoders.rle.Int64RLEDictionaryValuesDecoder;
import com.facebook.presto.parquet.batchreader.decoders.rle.Int64TimestampMicrosRLEDictionaryValuesDecoder;
import com.facebook.presto.parquet.batchreader.decoders.rle.TimestampRLEDictionaryValuesDecoder;
import com.facebook.presto.parquet.batchreader.dictionary.BinaryBatchDictionary;
import com.facebook.presto.parquet.batchreader.dictionary.TimestampDictionary;
import com.facebook.presto.parquet.dictionary.Dictionary;
import com.facebook.presto.parquet.dictionary.IntegerDictionary;
import com.facebook.presto.parquet.dictionary.LongDictionary;
import com.facebook.presto.spi.PrestoException;
import io.airlift.slice.Slice;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import static com.facebook.presto.parquet.ParquetEncoding.DELTA_BINARY_PACKED;
import static com.facebook.presto.parquet.ParquetEncoding.DELTA_BYTE_ARRAY;
import static com.facebook.presto.parquet.ParquetEncoding.DELTA_LENGTH_BYTE_ARRAY;
import static com.facebook.presto.parquet.ParquetEncoding.PLAIN;
import static com.facebook.presto.parquet.ParquetEncoding.PLAIN_DICTIONARY;
import static com.facebook.presto.parquet.ParquetEncoding.RLE;
import static com.facebook.presto.parquet.ParquetEncoding.RLE_DICTIONARY;
import static com.facebook.presto.parquet.ParquetErrorCode.PARQUET_IO_READ_ERROR;
import static com.facebook.presto.parquet.ParquetErrorCode.PARQUET_UNSUPPORTED_COLUMN_TYPE;
import static com.facebook.presto.parquet.ParquetErrorCode.PARQUET_UNSUPPORTED_ENCODING;
import static com.facebook.presto.parquet.ParquetTypeUtils.isTimeStampMicrosType;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static org.apache.parquet.bytes.BytesUtils.getWidthFromMaxInt;
import static org.apache.parquet.bytes.BytesUtils.readIntLittleEndian;
import static org.apache.parquet.bytes.BytesUtils.readIntLittleEndianOnOneByte;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;

public class Decoders
{
    private Decoders()
    {
    }

    public static FlatDecoders readFlatPage(DataPage page, RichColumnDescriptor columnDescriptor, Dictionary dictionary)
    {
        try {
            if (page instanceof DataPageV1) {
                return readFlatPageV1((DataPageV1) page, columnDescriptor, dictionary);
            }
            return readFlatPageV2((DataPageV2) page, columnDescriptor, dictionary);
        }
        catch (IOException e) {
            throw new PrestoException(PARQUET_IO_READ_ERROR, "Error reading parquet page " + page + " in column " + columnDescriptor, e);
        }
    }

    private static final ValuesDecoder createValuesDecoder(ColumnDescriptor columnDescriptor, Dictionary dictionary, int valueCount, ParquetEncoding encoding, byte[] buffer, int offset, int length)
            throws IOException
    {
        final PrimitiveTypeName type = columnDescriptor.getPrimitiveType().getPrimitiveTypeName();

        if (encoding == PLAIN) {
            switch (type) {
                case BOOLEAN:
                    return new BooleanPlainValuesDecoder(buffer, offset, length);
                case INT32:
                case FLOAT:
                    return new Int32PlainValuesDecoder(buffer, offset, length);
                case INT64: {
                    if (isTimeStampMicrosType(columnDescriptor)) {
                        return new Int64TimestampMicrosPlainValuesDecoder(buffer, offset, length);
                    }
                }
                case DOUBLE:
                    return new Int64PlainValuesDecoder(buffer, offset, length);
                case INT96:
                    return new TimestampPlainValuesDecoder(buffer, offset, length);
                case BINARY:
                    return new BinaryPlainValuesDecoder(buffer, offset, length);
                case FIXED_LEN_BYTE_ARRAY:
                default:
                    throw new PrestoException(PARQUET_UNSUPPORTED_COLUMN_TYPE, format("Column: %s, Encoding: %s", columnDescriptor, encoding));
            }
        }

        if (encoding == RLE && type == BOOLEAN) {
            ByteBuffer byteBuffer = ByteBuffer.wrap(buffer, offset, length);
            byteBuffer.getInt(); // skip past the length
            return new BooleanRLEValuesDecoder(byteBuffer);
        }

        if (encoding == RLE_DICTIONARY || encoding == PLAIN_DICTIONARY) {
            InputStream inputStream = ByteBufferInputStream.wrap(ByteBuffer.wrap(buffer, offset, length));
            int bitWidth = readIntLittleEndianOnOneByte(inputStream);
            switch (type) {
                case INT32:
                case FLOAT: {
                    return new Int32RLEDictionaryValuesDecoder(bitWidth, inputStream, (IntegerDictionary) dictionary);
                }
                case INT64: {
                    if (isTimeStampMicrosType(columnDescriptor)) {
                        return new Int64TimestampMicrosRLEDictionaryValuesDecoder(bitWidth, inputStream, (LongDictionary) dictionary);
                    }
                }
                case DOUBLE: {
                    return new Int64RLEDictionaryValuesDecoder(bitWidth, inputStream, (LongDictionary) dictionary);
                }
                case INT96: {
                    return new TimestampRLEDictionaryValuesDecoder(bitWidth, inputStream, (TimestampDictionary) dictionary);
                }
                case BINARY: {
                    return new BinaryRLEDictionaryValuesDecoder(bitWidth, inputStream, (BinaryBatchDictionary) dictionary);
                }
                case FIXED_LEN_BYTE_ARRAY:
                default:
                    throw new PrestoException(PARQUET_UNSUPPORTED_COLUMN_TYPE, format("Column: %s, Encoding: %s", columnDescriptor, encoding));
            }
        }

        if (encoding == DELTA_BINARY_PACKED) {
            ByteBufferInputStream inputStream = ByteBufferInputStream.wrap(ByteBuffer.wrap(buffer, offset, length));
            switch (type) {
                case INT32:
                case FLOAT: {
                    return new Int32DeltaBinaryPackedValuesDecoder(valueCount, inputStream);
                }
                case INT64: {
                    if (isTimeStampMicrosType(columnDescriptor)) {
                        return new Int64TimestampMicrosDeltaBinaryPackedValuesDecoder(valueCount, inputStream);
                    }
                }
                case DOUBLE: {
                    return new Int64DeltaBinaryPackedValuesDecoder(valueCount, inputStream);
                }
                default:
                    throw new PrestoException(PARQUET_UNSUPPORTED_COLUMN_TYPE, format("Column: %s, Encoding: %s", columnDescriptor, encoding));
            }
        }

        if ((encoding == DELTA_BYTE_ARRAY || encoding == DELTA_LENGTH_BYTE_ARRAY) && type == PrimitiveTypeName.BINARY) {
            ByteBufferInputStream inputStream = ByteBufferInputStream.wrap(ByteBuffer.wrap(buffer, offset, length));
            return new BinaryDeltaValuesDecoder(encoding, valueCount, inputStream);
        }
        throw new PrestoException(PARQUET_UNSUPPORTED_ENCODING, format("Column: %s, Encoding: %s", columnDescriptor, encoding));
    }

    private static FlatDecoders readFlatPageV1(DataPageV1 page, RichColumnDescriptor columnDescriptor, Dictionary dictionary)
            throws IOException
    {
        byte[] bytes = page.getSlice().getBytes();
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes, 0, bytes.length);
        FlatDefinitionLevelDecoder definitionLevelDecoder = createFlatDefinitionLevelDecoder(
                page.getDefinitionLevelEncoding(),
                columnDescriptor.isRequired(),
                columnDescriptor.getMaxDefinitionLevel(),
                page.getValueCount(),
                byteBuffer);
        ValuesDecoder valuesDecoder = createValuesDecoder(
                columnDescriptor,
                dictionary,
                page.getValueCount(),
                page.getValueEncoding(),
                bytes,
                byteBuffer.position(),
                bytes.length - byteBuffer.position());
        return new FlatDecoders(definitionLevelDecoder, valuesDecoder);
    }

    private static FlatDecoders readFlatPageV2(DataPageV2 pageV2, RichColumnDescriptor columnDescriptor, Dictionary dictionary)
            throws IOException
    {
        final int valueCount = pageV2.getValueCount();
        final int maxDefinitionLevel = columnDescriptor.getMaxDefinitionLevel();
        checkArgument(maxDefinitionLevel <= 1 && maxDefinitionLevel >= 0, "Invalid max definition level: " + maxDefinitionLevel);

        FlatDefinitionLevelDecoder definitionLevelDecoder;
        if (maxDefinitionLevel == 0) {
            definitionLevelDecoder = new FlatDefinitionLevelDecoder(0, valueCount);
        }
        else {
            definitionLevelDecoder = new FlatDefinitionLevelDecoder(valueCount, new ByteArrayInputStream(pageV2.getDefinitionLevels().getBytes()));
        }
        ValuesDecoder valuesDecoder = createValuesDecoderV2(pageV2, columnDescriptor, dictionary);
        return new FlatDecoders(definitionLevelDecoder, valuesDecoder);
    }

    public static NestedDecoders readNestedPage(DataPage page, RichColumnDescriptor columnDescriptor, Dictionary dictionary)
    {
        try {
            if (page instanceof DataPageV1) {
                return readNestedPageV1((DataPageV1) page, columnDescriptor, dictionary);
            }
            return readNestedPageV2((DataPageV2) page, columnDescriptor, dictionary);
        }
        catch (IOException e) {
            throw new PrestoException(PARQUET_IO_READ_ERROR, "Error reading parquet page " + page + " in column " + columnDescriptor, e);
        }
    }

    private static NestedDecoders readNestedPageV1(DataPageV1 page, RichColumnDescriptor columnDescriptor, Dictionary dictionary)
            throws IOException
    {
        byte[] bytes = page.getSlice().getBytes();
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes, 0, bytes.length);

        RepetitionLevelDecoder repetitionLevelDecoder = createRepetitionLevelDecoder(
                page.getRepetitionLevelEncoding(),
                columnDescriptor.getMaxRepetitionLevel(),
                page.getValueCount(),
                byteBuffer);
        DefinitionLevelDecoder definitionLevelDecoder = createDefinitionLevelDecoder(
                page.getDefinitionLevelEncoding(),
                columnDescriptor.getMaxDefinitionLevel(),
                page.getValueCount(),
                byteBuffer);
        ValuesDecoder valuesDecoder = createValuesDecoder(
                columnDescriptor,
                dictionary,
                page.getValueCount(),
                page.getValueEncoding(),
                bytes,
                byteBuffer.position(),
                bytes.length - byteBuffer.position());
        return new NestedDecoders(repetitionLevelDecoder, definitionLevelDecoder, valuesDecoder);
    }

    private static NestedDecoders readNestedPageV2(DataPageV2 pageV2, RichColumnDescriptor columnDescriptor, Dictionary dictionary)
            throws IOException
    {
        final int valueCount = pageV2.getValueCount();
        return new NestedDecoders(
                createRepetitionLevelDecoderV2(valueCount, columnDescriptor, pageV2.getRepetitionLevels()),
                createDefinitionLevelDecoderV2(valueCount, columnDescriptor, pageV2.getDefinitionLevels()),
                createValuesDecoderV2(pageV2, columnDescriptor, dictionary));
    }

    private static final RepetitionLevelDecoder createRepetitionLevelDecoderV2(int valueCount, RichColumnDescriptor columnDescriptor, Slice repetitionLevelBuffer)
    {
        final int maxRepetitionLevel = columnDescriptor.getMaxRepetitionLevel();
        final int repetitionLevelBitWidth = getWidthFromMaxInt(maxRepetitionLevel);
        if (maxRepetitionLevel == 0 || repetitionLevelBitWidth == 0) {
            return new RepetitionLevelDecoder(0, valueCount);
        }
        return new RepetitionLevelDecoder(valueCount, repetitionLevelBitWidth, new ByteArrayInputStream(repetitionLevelBuffer.getBytes()));
    }

    private static final DefinitionLevelDecoder createDefinitionLevelDecoderV2(int valueCount, RichColumnDescriptor columnDescriptor, Slice definitionLevelBuffer)
    {
        final int maxDefinitionLevel = columnDescriptor.getMaxDefinitionLevel();
        final int definitionLevelBitWidth = getWidthFromMaxInt(maxDefinitionLevel);
        if (maxDefinitionLevel == 0 || definitionLevelBitWidth == 0) {
            return new DefinitionLevelDecoder(0, valueCount);
        }
        return new DefinitionLevelDecoder(valueCount, definitionLevelBitWidth, new ByteArrayInputStream(definitionLevelBuffer.getBytes()));
    }

    private static final ValuesDecoder createValuesDecoderV2(DataPageV2 pageV2, RichColumnDescriptor columnDescriptor, Dictionary dictionary)
            throws IOException
    {
        final byte[] valueBuffer = pageV2.getSlice().getBytes();
        return createValuesDecoder(columnDescriptor, dictionary, pageV2.getValueCount(), pageV2.getDataEncoding(), valueBuffer, 0, valueBuffer.length);
    }

    private static final FlatDefinitionLevelDecoder createFlatDefinitionLevelDecoder(ParquetEncoding encoding, boolean isRequired, int maxLevelValue, int valueCount, ByteBuffer buffer)
            throws IOException
    {
        if (isRequired) {
            return new FlatDefinitionLevelDecoder(1, valueCount);
        }

        if (maxLevelValue == 0) {
            return new FlatDefinitionLevelDecoder(0, valueCount);
        }

        final int bitWidth = getWidthFromMaxInt(maxLevelValue);
        if (bitWidth == 0) {
            return new FlatDefinitionLevelDecoder(0, valueCount);
        }

        checkArgument(encoding == RLE, "Invalid definition level encoding: " + encoding);
        ByteBufferInputStream bufferInputStream = ByteBufferInputStream.wrap(buffer);

        final int bufferSize = readIntLittleEndian(bufferInputStream);
        FlatDefinitionLevelDecoder definitionLevelDecoder = new FlatDefinitionLevelDecoder(valueCount, bufferInputStream.sliceStream(bufferSize));
        buffer.position(buffer.position() + bufferSize + 4);
        return definitionLevelDecoder;
    }

    public static final RepetitionLevelDecoder createRepetitionLevelDecoder(ParquetEncoding encoding, int maxLevelValue, int valueCount, ByteBuffer buffer)
            throws IOException
    {
        final int bitWidth = getWidthFromMaxInt(maxLevelValue);
        if (maxLevelValue == 0 || bitWidth == 0) {
            return new RepetitionLevelDecoder(0, valueCount);
        }

        checkArgument(encoding == RLE, "Invalid repetition level encoding: " + encoding);
        ByteBufferInputStream bufferInputStream = ByteBufferInputStream.wrap(buffer);

        final int bufferSize = readIntLittleEndian(bufferInputStream);
        RepetitionLevelDecoder repetitionLevelDecoder = new RepetitionLevelDecoder(valueCount, bitWidth, bufferInputStream.sliceStream(bufferSize));
        buffer.position(buffer.position() + bufferSize + 4);
        return repetitionLevelDecoder;
    }

    public static final DefinitionLevelDecoder createDefinitionLevelDecoder(ParquetEncoding encoding, int maxLevelValue, int valueCount, ByteBuffer buffer)
            throws IOException
    {
        final int bitWidth = getWidthFromMaxInt(maxLevelValue);
        if (maxLevelValue == 0 || bitWidth == 0) {
            return new DefinitionLevelDecoder(0, valueCount);
        }

        checkArgument(encoding == RLE, "Invalid definition level encoding: " + encoding);
        ByteBufferInputStream bufferInputStream = ByteBufferInputStream.wrap(buffer);

        final int bufferSize = readIntLittleEndian(bufferInputStream);
        DefinitionLevelDecoder definitionLevelDecoder = new DefinitionLevelDecoder(valueCount, bitWidth, bufferInputStream.sliceStream(bufferSize));
        buffer.position(buffer.position() + bufferSize + 4);
        return definitionLevelDecoder;
    }

    public static class FlatDecoders
    {
        private final FlatDefinitionLevelDecoder definitionLevelDecoder;
        private final ValuesDecoder valuesDecoder;

        private FlatDecoders(FlatDefinitionLevelDecoder dlDecoder, ValuesDecoder valuesDecoder)
        {
            this.definitionLevelDecoder = dlDecoder;
            this.valuesDecoder = valuesDecoder;
        }

        public FlatDefinitionLevelDecoder getDefinitionLevelDecoder()
        {
            return definitionLevelDecoder;
        }

        public ValuesDecoder getValuesDecoder()
        {
            return valuesDecoder;
        }
    }

    public static class NestedDecoders
    {
        private final RepetitionLevelDecoder repetitionLevelDecoder;
        private final DefinitionLevelDecoder definitionLevelDecoder;
        private final ValuesDecoder valuesDecoder;

        private NestedDecoders(RepetitionLevelDecoder repetitionLevelDecoder, DefinitionLevelDecoder definitionLevelDecoder, ValuesDecoder valuesDecoder)
        {
            this.repetitionLevelDecoder = repetitionLevelDecoder;
            this.definitionLevelDecoder = definitionLevelDecoder;
            this.valuesDecoder = valuesDecoder;
        }

        public RepetitionLevelDecoder getRepetitionLevelDecoder()
        {
            return repetitionLevelDecoder;
        }

        public DefinitionLevelDecoder getDefinitionLevelDecoder()
        {
            return definitionLevelDecoder;
        }

        public ValuesDecoder getValuesDecoder()
        {
            return valuesDecoder;
        }
    }
}
