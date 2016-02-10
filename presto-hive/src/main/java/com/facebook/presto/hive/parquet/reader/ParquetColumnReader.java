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

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import parquet.bytes.BytesInput;
import parquet.bytes.BytesUtils;
import parquet.column.ColumnDescriptor;
import parquet.column.Dictionary;
import parquet.column.Encoding;
import parquet.column.ValuesType;
import parquet.column.page.DataPage;
import parquet.column.page.DataPageV1;
import parquet.column.page.DataPageV2;
import parquet.column.page.DictionaryPage;
import parquet.column.page.PageReader;
import parquet.column.values.ValuesReader;
import parquet.column.values.rle.RunLengthBitPackingHybridDecoder;
import parquet.io.ParquetDecodingException;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import static com.facebook.presto.hive.parquet.ParquetValidationUtils.validateParquet;
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
    private PageReader pageReader;
    private Dictionary dictionary;
    private int repetitionLevel;
    private int definitionLevel;
    private int currentValueCount;
    private int pageValueCount;
    private DataPage page;
    private int remainingValueCountInPage;
    private int readOffset;

    public abstract BlockBuilder createBlockBuilder();

    public abstract void readValues(BlockBuilder blockBuilder, int valueNumber);

    public abstract void skipValues(int offsetNumber);

    public static ParquetColumnReader createReader(ColumnDescriptor descriptor)
    {
        switch (descriptor.getType()) {
            case BOOLEAN:
                return new ParquetBooleanColumnReader(descriptor);
            case INT32:
                return new ParquetIntColumnReader(descriptor);
            case INT64:
                return new ParquetLongColumnReader(descriptor);
            case FLOAT:
                return new ParquetFloatColumnReader(descriptor);
            case DOUBLE:
                return new ParquetDoubleColumnReader(descriptor);
            case BINARY:
                return new ParquetBinaryColumnReader(descriptor);
            default:
                throw new PrestoException(NOT_SUPPORTED, "Unsupported parquet type: " + descriptor.getType());
        }
    }

    public ParquetColumnReader(ColumnDescriptor columnDescriptor)
    {
        this.columnDescriptor = requireNonNull(columnDescriptor, "columnDescriptor");
        pageReader = null;
    }

    public PageReader getPageReader()
    {
        return pageReader;
    }

    public void setPageReader(PageReader pageReader)
    {
        this.pageReader = requireNonNull(pageReader, "pageReader");
        DictionaryPage dictionaryPage = pageReader.readDictionaryPage();

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

    public Block readBlock(Type type)
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

        BlockBuilder blockBuilder = createBlockBuilder();
        int valueCount = 0;
        while (valueCount < nextBatchSize) {
            if (page == null) {
                readNextPage();
            }
            int valueNumber = Math.min(remainingValueCountInPage, nextBatchSize - valueCount);
            readValues(blockBuilder, valueNumber);
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
        pageValueCount = page.getValueCount();
        remainingValueCountInPage = page.getValueCount();

        if (page instanceof DataPageV1) {
            valuesReader = readPageV1((DataPageV1) page);
        }
        else {
            valuesReader = readPageV2((DataPageV2) page);
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

    private ValuesReader readPageV1(DataPageV1 page)
    {
        ValuesReader rlReader = page.getRlEncoding().getValuesReader(columnDescriptor, ValuesType.REPETITION_LEVEL);
        ValuesReader dlReader = page.getDlEncoding().getValuesReader(columnDescriptor, ValuesType.DEFINITION_LEVEL);
        repetitionReader = new ParquetLevelValuesReader(rlReader);
        definitionReader = new ParquetLevelValuesReader(dlReader);
        try {
            byte[] bytes = page.getBytes().toByteArray();
            rlReader.initFromPage(pageValueCount, bytes, 0);
            int offset = rlReader.getNextOffset();
            dlReader.initFromPage(pageValueCount, bytes, offset);
            offset = dlReader.getNextOffset();
            return initDataReader(page.getValueEncoding(), bytes, offset, page.getValueCount());
        }
        catch (IOException e) {
            throw new ParquetDecodingException("Error reading parquet page " + page + " in column " + columnDescriptor, e);
        }
    }

    private ValuesReader readPageV2(DataPageV2 page)
    {
        repetitionReader = buildLevelRLEReader(columnDescriptor.getMaxRepetitionLevel(), page.getRepetitionLevels());
        definitionReader = buildLevelRLEReader(columnDescriptor.getMaxDefinitionLevel(), page.getDefinitionLevels());
        try {
            return initDataReader(page.getDataEncoding(), page.getData().toByteArray(), 0, page.getValueCount());
        }
        catch (IOException e) {
            throw new ParquetDecodingException("could not read page " + page + " in col " + columnDescriptor, e);
        }
    }

    private ParquetLevelReader buildLevelRLEReader(int maxLevel, BytesInput bytes)
    {
        try {
            if (maxLevel == 0) {
                return new ParquetLevelNullReader();
            }
            return new ParquetLevelRLEReader(new RunLengthBitPackingHybridDecoder(BytesUtils.getWidthFromMaxInt(maxLevel), new ByteArrayInputStream(bytes.toByteArray())));
        }
        catch (IOException e) {
            throw new ParquetDecodingException("could not read levels in page for col " + columnDescriptor, e);
        }
    }

    private ValuesReader initDataReader(Encoding dataEncoding, byte[] bytes, int offset, int valueCount)
    {
        ValuesReader valuesReader;
        if (dataEncoding.usesDictionary()) {
            if (dictionary == null) {
                throw new ParquetDecodingException("Dictionary is missing for Page");
            }
            valuesReader = dataEncoding.getDictionaryBasedValuesReader(columnDescriptor, ValuesType.VALUES, dictionary);
        }
        else {
            valuesReader = dataEncoding.getValuesReader(columnDescriptor, ValuesType.VALUES);
        }

        try {
            valuesReader.initFromPage(pageValueCount, bytes, offset);
            return valuesReader;
        }
        catch (IOException e) {
            throw new ParquetDecodingException("Error reading parquet page in column " + columnDescriptor, e);
        }
    }
}
