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
package com.facebook.presto.iceberg.parquet.reader;

import com.facebook.presto.iceberg.parquet.ParquetDataPage;
import com.facebook.presto.iceberg.parquet.ParquetDataPageV1;
import com.facebook.presto.iceberg.parquet.ParquetDataPageV2;
import com.facebook.presto.iceberg.parquet.ParquetDictionaryPage;
import com.facebook.presto.iceberg.parquet.ParquetEncoding;
import com.facebook.presto.iceberg.parquet.ParquetTypeUtils;
import com.facebook.presto.iceberg.parquet.RichColumnDescriptor;
import com.facebook.presto.iceberg.parquet.dictionary.ParquetDictionary;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.PageBuilderStatus;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.Slice;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridDecoder;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.schema.DecimalMetadata;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.function.Consumer;

import static com.facebook.presto.iceberg.parquet.ParquetValidationUtils.validateParquet;
import static com.facebook.presto.iceberg.parquet.ParquetValuesType.DEFINITION_LEVEL;
import static com.facebook.presto.iceberg.parquet.ParquetValuesType.REPETITION_LEVEL;
import static com.facebook.presto.iceberg.parquet.ParquetValuesType.VALUES;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static org.apache.parquet.schema.OriginalType.DECIMAL;

public abstract class ParquetColumnReader
{
    private static final int EMPTY_LEVEL_VALUE = -1;
    protected final RichColumnDescriptor columnDescriptor;
    protected int definitionLevel = EMPTY_LEVEL_VALUE;
    protected ValuesReader valuesReader;
    protected int nextBatchSize;

    private ParquetLevelReader repetitionReader;
    private ParquetLevelReader definitionReader;
    private int repetitionLevel = EMPTY_LEVEL_VALUE;
    private long totalValueCount;
    private ParquetPageReader pageReader;
    private ParquetDictionary dictionary;
    private int currentValueCount;
    private ParquetDataPage page;
    private int remainingValueCountInPage;
    private int readOffset;

    protected abstract void readValue(BlockBuilder blockBuilder, Type type);

    protected abstract void skipValue();

    protected boolean isValueNull()
    {
        return ParquetTypeUtils.isValueNull(columnDescriptor.isRequired(), definitionLevel, columnDescriptor.getMaxDefinitionLevel());
    }

    public static ParquetColumnReader createReader(RichColumnDescriptor descriptor)
    {
        switch (descriptor.getType()) {
            case BOOLEAN:
                return new ParquetBooleanColumnReader(descriptor);
            case INT32:
                return createDecimalColumnReader(descriptor).orElse(new ParquetIntColumnReader(descriptor));
            case INT64:
                return createDecimalColumnReader(descriptor).orElse(new ParquetLongColumnReader(descriptor));
            case INT96:
                return new ParquetTimestampColumnReader(descriptor);
            case FLOAT:
                return new ParquetFloatColumnReader(descriptor);
            case DOUBLE:
                return new ParquetDoubleColumnReader(descriptor);
            case BINARY:
                return createDecimalColumnReader(descriptor).orElse(new ParquetBinaryColumnReader(descriptor));
            case FIXED_LEN_BYTE_ARRAY:
                return createDecimalColumnReader(descriptor)
                        .orElseThrow(() -> new PrestoException(NOT_SUPPORTED, "Parquet type FIXED_LEN_BYTE_ARRAY supported as DECIMAL; got " + descriptor.getPrimitiveType().getOriginalType()));
            default:
                throw new PrestoException(NOT_SUPPORTED, "Unsupported parquet type: " + descriptor.getType());
        }
    }

    private static Optional<ParquetColumnReader> createDecimalColumnReader(RichColumnDescriptor descriptor)
    {
        if (descriptor.getPrimitiveType().getOriginalType() != DECIMAL) {
            return Optional.empty();
        }
        DecimalMetadata decimalMetadata = descriptor.getPrimitiveType().getDecimalMetadata();
        return Optional.of(ParquetDecimalColumnReaderFactory.createReader(descriptor, decimalMetadata.getPrecision(), decimalMetadata.getScale()));
    }

    public ParquetColumnReader(RichColumnDescriptor columnDescriptor)
    {
        this.columnDescriptor = requireNonNull(columnDescriptor, "columnDescriptor");
        pageReader = null;
    }

    public ParquetPageReader getPageReader()
    {
        return pageReader;
    }

    public void setPageReader(ParquetPageReader pageReader)
    {
        this.pageReader = requireNonNull(pageReader, "pageReader");
        ParquetDictionaryPage dictionaryPage = pageReader.readDictionaryPage();

        if (dictionaryPage != null) {
            try {
                dictionary = dictionaryPage.getEncoding().initDictionary(columnDescriptor, dictionaryPage);
            }
            catch (IOException e) {
                throw new ParquetDecodingException("could not decode the dictionary for " + columnDescriptor, e);
            }
        }
        else {
            dictionary = null;
        }
        checkArgument(pageReader.getTotalValueCount() > 0, "page is empty");
        totalValueCount = pageReader.getTotalValueCount();
    }

    public void prepareNextRead(int batchSize)
    {
        readOffset = readOffset + nextBatchSize;
        nextBatchSize = batchSize;
    }

    public ColumnDescriptor getDescriptor()
    {
        return columnDescriptor;
    }

    public Block readPrimitive(Type type, IntList definitionLevels, IntList repetitionLevels)
            throws IOException
    {
        seek();
        BlockBuilder blockBuilder = type.createBlockBuilder(new PageBuilderStatus().createBlockBuilderStatus(), nextBatchSize);
        int valueCount = 0;
        while (valueCount < nextBatchSize) {
            if (page == null) {
                readNextPage();
            }
            int valuesToRead = Math.min(remainingValueCountInPage, nextBatchSize - valueCount);
            readValues(blockBuilder, valuesToRead, type, definitionLevels, repetitionLevels);
            valueCount += valuesToRead;
        }
        checkArgument(valueCount == nextBatchSize, "valueCount " + valueCount + " not equals to batchSize " + nextBatchSize);

        readOffset = 0;
        nextBatchSize = 0;
        return blockBuilder.build();
    }

    private void readValues(BlockBuilder blockBuilder, int valuesToRead, Type type, IntList definitionLevels, IntList repetitionLevels)
            throws IOException
    {
        processValues(valuesToRead, ignored -> {
            readValue(blockBuilder, type);
            definitionLevels.add(definitionLevel);
            repetitionLevels.add(repetitionLevel);
        });
    }

    private void skipValues(int valuesToRead)
            throws IOException
    {
        processValues(valuesToRead, ignored -> skipValue());
    }

    private void processValues(int valuesToRead, Consumer<Void> valueConsumer)
            throws IOException
    {
        if (definitionLevel == EMPTY_LEVEL_VALUE && repetitionLevel == EMPTY_LEVEL_VALUE) {
            definitionLevel = definitionReader.readLevel();
            repetitionLevel = repetitionReader.readLevel();
        }
        int valueCount = 0;
        for (int i = 0; i < valuesToRead; i++) {
            do {
                valueConsumer.accept(null);
                valueCount++;
                if (valueCount == remainingValueCountInPage) {
                    updatePosition(valueCount);
                    if (!pageReader.available()) {
                        return;
                    }
                    valueCount = 0;
                    readNextPage();
                }
                repetitionLevel = repetitionReader.readLevel();
                definitionLevel = definitionReader.readLevel();
            }
            while (repetitionLevel != 0);
        }
        updatePosition(valueCount);
    }

    private void seek()
            throws IOException
    {
        checkArgument(currentValueCount <= totalValueCount, "Already read all values in column chunk");
        if (readOffset == 0) {
            return;
        }
        int valuePosition = 0;
        while (valuePosition < readOffset) {
            if (page == null) {
                readNextPage();
            }
            int offset = Math.min(remainingValueCountInPage, readOffset - valuePosition);
            skipValues(offset);
            valuePosition = valuePosition + offset;
        }
        checkArgument(valuePosition == readOffset, "valuePosition " + valuePosition + " must be equal to readOffset " + readOffset);
    }

    private void readNextPage()
            throws IOException
    {
        page = pageReader.readPage();
        validateParquet(page != null, "Not enough values to read in column chunk");
        remainingValueCountInPage = page.getValueCount();

        if (page instanceof ParquetDataPageV1) {
            valuesReader = readPageV1((ParquetDataPageV1) page);
        }
        else {
            valuesReader = readPageV2((ParquetDataPageV2) page);
        }
    }

    private void updatePosition(int valuesToRead)
    {
        if (valuesToRead == remainingValueCountInPage) {
            page = null;
            valuesReader = null;
        }
        remainingValueCountInPage = remainingValueCountInPage - valuesToRead;
        currentValueCount += valuesToRead;
    }

    private ValuesReader readPageV1(ParquetDataPageV1 page)
    {
        ValuesReader rlReader = page.getRepetitionLevelEncoding().getValuesReader(columnDescriptor, REPETITION_LEVEL);
        ValuesReader dlReader = page.getDefinitionLevelEncoding().getValuesReader(columnDescriptor, DEFINITION_LEVEL);
        repetitionReader = new ParquetLevelValuesReader(rlReader);
        definitionReader = new ParquetLevelValuesReader(dlReader);
        try {
//            byte[] bytes = page.getSlice().getBytes();
//            rlReader.initFromPage(page.getValueCount(), bytes, 0);
//            int offset = rlReader.getNextOffset();
//            dlReader.initFromPage(page.getValueCount(), bytes, offset);
//            offset = dlReader.getNextOffset();
//            return initDataReader(page.getValueEncoding(), bytes, offset, page.getValueCount());
            byte[] bytes = page.getSlice().getBytes();
            final ByteBufferInputStream bufferInputStream = ByteBufferInputStream.wrap(ByteBuffer.wrap(bytes));
            rlReader.initFromPage(page.getValueCount(), bufferInputStream);
            int offset = bytes.length - bufferInputStream.available();
            // Assumes ByteBufferInputStream.remainingStream makes a copy of the underlying bytebuffer.
            final ByteBufferInputStream dlbyteBufferInputStream = ByteBufferInputStream.wrap(ByteBuffer.wrap(bytes, offset, bytes.length - offset));
            dlReader.initFromPage(page.getValueCount(), dlbyteBufferInputStream);
            offset = bytes.length - dlbyteBufferInputStream.available();
            return initDataReader(page.getValueEncoding(), ByteBufferInputStream.wrap(ByteBuffer.wrap(bytes, offset, bytes.length - offset)), page.getValueCount());
        }
        catch (IOException e) {
            throw new ParquetDecodingException("Error reading parquet page " + page + " in column " + columnDescriptor, e);
        }
    }

    private ValuesReader readPageV2(ParquetDataPageV2 page)
    {
        repetitionReader = buildLevelRLEReader(columnDescriptor.getMaxRepetitionLevel(), page.getRepetitionLevels());
        definitionReader = buildLevelRLEReader(columnDescriptor.getMaxDefinitionLevel(), page.getDefinitionLevels());
        return initDataReader(page.getDataEncoding(), ByteBufferInputStream.wrap(ByteBuffer.wrap(page.getSlice().getBytes())), page.getValueCount());
    }

    private ParquetLevelReader buildLevelRLEReader(int maxLevel, Slice slice)
    {
        if (maxLevel == 0) {
            return new ParquetLevelNullReader();
        }
        return new ParquetLevelRLEReader(new RunLengthBitPackingHybridDecoder(BytesUtils.getWidthFromMaxInt(maxLevel), new ByteArrayInputStream(slice.getBytes())));
    }

    private ValuesReader initDataReader(ParquetEncoding dataEncoding, ByteBufferInputStream bufferInputStream, int valueCount)
    {
        ValuesReader valuesReader;
        if (dataEncoding.usesDictionary()) {
            if (dictionary == null) {
                throw new ParquetDecodingException("Dictionary is missing for Page");
            }
            valuesReader = dataEncoding.getDictionaryBasedValuesReader(columnDescriptor, VALUES, dictionary);
        }
        else {
            valuesReader = dataEncoding.getValuesReader(columnDescriptor, VALUES);
        }

        try {
            valuesReader.initFromPage(valueCount, bufferInputStream);
            return valuesReader;
        }
        catch (IOException e) {
            throw new ParquetDecodingException("Error reading parquet page in column " + columnDescriptor, e);
        }
    }
}
