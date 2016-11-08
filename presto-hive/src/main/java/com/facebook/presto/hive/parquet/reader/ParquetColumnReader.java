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
package com.facebook.presto.hive.parquet.reader;

import com.facebook.presto.hive.parquet.ParquetDataPage;
import com.facebook.presto.hive.parquet.ParquetDataPageV1;
import com.facebook.presto.hive.parquet.ParquetDataPageV2;
import com.facebook.presto.hive.parquet.ParquetDictionaryPage;
import com.facebook.presto.hive.parquet.ParquetEncoding;
import com.facebook.presto.hive.parquet.RichColumnDescriptor;
import com.facebook.presto.hive.parquet.dictionary.ParquetDictionary;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.Slice;
import parquet.bytes.BytesUtils;
import parquet.column.ColumnDescriptor;
import parquet.column.values.ValuesReader;
import parquet.column.values.rle.RunLengthBitPackingHybridDecoder;
import parquet.io.ParquetDecodingException;
import parquet.schema.DecimalMetadata;
import parquet.schema.OriginalType;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import static com.facebook.presto.hive.parquet.ParquetValidationUtils.validateParquet;
import static com.facebook.presto.hive.parquet.ParquetValuesType.DEFINITION_LEVEL;
import static com.facebook.presto.hive.parquet.ParquetValuesType.REPETITION_LEVEL;
import static com.facebook.presto.hive.parquet.ParquetValuesType.VALUES;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public abstract class ParquetColumnReader
{
    protected final ColumnDescriptor columnDescriptor;

    protected ParquetLevelReader repetitionReader;
    protected ParquetLevelReader definitionReader;
    protected ValuesReader valuesReader;
    protected int nextBatchSize;

    private long totalValueCount;
    private ParquetPageReader pageReader;
    private ParquetDictionary dictionary;
    private int repetitionLevel;
    private int definitionLevel;
    private int currentValueCount;
    private ParquetDataPage page;
    private int remainingValueCountInPage;
    private int readOffset;

    public abstract BlockBuilder createBlockBuilder(Type type);

    public abstract void readValues(BlockBuilder blockBuilder, int valueNumber, Type type);

    public abstract void skipValues(int offsetNumber);

    public static ParquetColumnReader createReader(RichColumnDescriptor descriptor)
    {
        switch (descriptor.getType()) {
            case BOOLEAN:
                return new ParquetBooleanColumnReader(descriptor);
            case INT32:
                return new ParquetIntColumnReader(descriptor);
            case INT64:
                return new ParquetLongColumnReader(descriptor);
            case INT96:
                return new ParquetTimestampColumnReader(descriptor);
            case FLOAT:
                return new ParquetFloatColumnReader(descriptor);
            case DOUBLE:
                return new ParquetDoubleColumnReader(descriptor);
            case BINARY:
                return new ParquetBinaryColumnReader(descriptor);
            case FIXED_LEN_BYTE_ARRAY:
                if (descriptor.getPrimitiveType().getOriginalType() == OriginalType.DECIMAL) {
                    DecimalMetadata decimalMetadata = descriptor.getPrimitiveType().getDecimalMetadata();
                    return ParquetDecimalColumnReaderFactory.createReader(descriptor, decimalMetadata.getPrecision(), decimalMetadata.getScale());
                }
                else {
                    throw new PrestoException(NOT_SUPPORTED, "Parquet type FIXED_LEN_BYTE_ARRAY supported as DECIMAL; got " + descriptor.getPrimitiveType().getOriginalType());
                }
            default:
                throw new PrestoException(NOT_SUPPORTED, "Unsupported parquet type: " + descriptor.getType());
        }
    }

    public ParquetColumnReader(ColumnDescriptor columnDescriptor)
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

    public int getCurrentRepetitionLevel()
    {
        return repetitionLevel;
    }

    public int getCurrentDefinitionLevel()
    {
        return definitionLevel;
    }

    public ColumnDescriptor getDescriptor()
    {
        return columnDescriptor;
    }

    public long getTotalValueCount()
    {
        return totalValueCount;
    }

    public Block readPrimitive(Type type)
            throws IOException
    {
        checkArgument(currentValueCount <= totalValueCount, "Already read all values in column chunk");
        // Parquet does not have api to skip in datastream, have to skip values
        // TODO skip in datastream
        if (readOffset != 0) {
            int valuePosition = 0;
            while (valuePosition < readOffset) {
                if (page == null) {
                    readNextPage();
                }
                int offsetNumber = Math.min(remainingValueCountInPage, readOffset - valuePosition);
                skipValues(offsetNumber);
                valuePosition = valuePosition + offsetNumber;
                updatePosition(offsetNumber);
            }
            checkArgument(valuePosition == readOffset, "valuePosition " + valuePosition + " not equals to readOffset " + readOffset);
        }

        BlockBuilder blockBuilder = createBlockBuilder(type);
        int valueCount = 0;
        while (valueCount < nextBatchSize) {
            if (page == null) {
                readNextPage();
            }
            int valueNumber = Math.min(remainingValueCountInPage, nextBatchSize - valueCount);
            readValues(blockBuilder, valueNumber, type);
            valueCount = valueCount + valueNumber;
            updatePosition(valueNumber);
        }
        checkArgument(valueCount == nextBatchSize, "valueCount " + valueCount + " not equals to batchSize " + nextBatchSize);

        readOffset = 0;
        nextBatchSize = 0;
        return blockBuilder.build();
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

    private void updatePosition(int valueNumber)
    {
        if (valueNumber == remainingValueCountInPage) {
            page = null;
            valuesReader = null;
        }
        remainingValueCountInPage = remainingValueCountInPage - valueNumber;
        currentValueCount += valueNumber;
    }

    private ValuesReader readPageV1(ParquetDataPageV1 page)
    {
        ValuesReader rlReader = page.getRepetitionLevelEncoding().getValuesReader(columnDescriptor, REPETITION_LEVEL);
        ValuesReader dlReader = page.getDefinitionLevelEncoding().getValuesReader(columnDescriptor, DEFINITION_LEVEL);
        repetitionReader = new ParquetLevelValuesReader(rlReader);
        definitionReader = new ParquetLevelValuesReader(dlReader);
        try {
            byte[] bytes = page.getSlice().getBytes();
            rlReader.initFromPage(page.getValueCount(), bytes, 0);
            int offset = rlReader.getNextOffset();
            dlReader.initFromPage(page.getValueCount(), bytes, offset);
            offset = dlReader.getNextOffset();
            return initDataReader(page.getValueEncoding(), bytes, offset, page.getValueCount());
        }
        catch (IOException e) {
            throw new ParquetDecodingException("Error reading parquet page " + page + " in column " + columnDescriptor, e);
        }
    }

    private ValuesReader readPageV2(ParquetDataPageV2 page)
    {
        repetitionReader = buildLevelRLEReader(columnDescriptor.getMaxRepetitionLevel(), page.getRepetitionLevels());
        definitionReader = buildLevelRLEReader(columnDescriptor.getMaxDefinitionLevel(), page.getDefinitionLevels());
        return initDataReader(page.getDataEncoding(), page.getSlice().getBytes(), 0, page.getValueCount());
    }

    private ParquetLevelReader buildLevelRLEReader(int maxLevel, Slice slice)
    {
        if (maxLevel == 0) {
            return new ParquetLevelNullReader();
        }
        return new ParquetLevelRLEReader(new RunLengthBitPackingHybridDecoder(BytesUtils.getWidthFromMaxInt(maxLevel), new ByteArrayInputStream(slice.getBytes())));
    }

    private ValuesReader initDataReader(ParquetEncoding dataEncoding, byte[] bytes, int offset, int valueCount)
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
            valuesReader.initFromPage(valueCount, bytes, offset);
            return valuesReader;
        }
        catch (IOException e) {
            throw new ParquetDecodingException("Error reading parquet page in column " + columnDescriptor, e);
        }
    }
}
