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

import com.facebook.presto.parquet.DataPage;
import com.facebook.presto.parquet.DataPageV1;
import com.facebook.presto.parquet.DataPageV2;
import com.facebook.presto.parquet.DictionaryPage;
import com.facebook.presto.parquet.Field;
import com.facebook.presto.parquet.ParquetEncoding;
import com.facebook.presto.parquet.ParquetTypeUtils;
import com.facebook.presto.parquet.RichColumnDescriptor;
import com.facebook.presto.parquet.dictionary.Dictionary;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.Slice;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import parquet.bytes.BytesUtils;
import parquet.column.ColumnDescriptor;
import parquet.column.values.ValuesReader;
import parquet.column.values.rle.RunLengthBitPackingHybridDecoder;
import parquet.io.ParquetDecodingException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Optional;
import java.util.function.Consumer;

import static com.facebook.presto.parquet.ParquetTypeUtils.createDecimalType;
import static com.facebook.presto.parquet.ValuesType.DEFINITION_LEVEL;
import static com.facebook.presto.parquet.ValuesType.REPETITION_LEVEL;
import static com.facebook.presto.parquet.ValuesType.VALUES;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

public abstract class PrimitiveColumnReader
{
    private static final int EMPTY_LEVEL_VALUE = -1;
    protected final RichColumnDescriptor columnDescriptor;

    protected int definitionLevel = EMPTY_LEVEL_VALUE;
    protected int repetitionLevel = EMPTY_LEVEL_VALUE;
    protected ValuesReader valuesReader;

    private int nextBatchSize;
    private LevelReader repetitionReader;
    private LevelReader definitionReader;
    private long totalValueCount;
    private PageReader pageReader;
    private Dictionary dictionary;
    private int currentValueCount;
    private DataPage page;
    private int remainingValueCountInPage;
    private int readOffset;

    protected abstract void readValue(BlockBuilder blockBuilder, Type type);

    protected abstract void skipValue();

    protected boolean isValueNull()
    {
        return ParquetTypeUtils.isValueNull(columnDescriptor.isRequired(), definitionLevel, columnDescriptor.getMaxDefinitionLevel());
    }

    public static PrimitiveColumnReader createReader(RichColumnDescriptor descriptor)
    {
        switch (descriptor.getType()) {
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

    private static Optional<PrimitiveColumnReader> createDecimalColumnReader(RichColumnDescriptor descriptor)
    {
        Optional<Type> type = createDecimalType(descriptor);
        if (type.isPresent()) {
            DecimalType decimalType = (DecimalType) type.get();
            return Optional.of(DecimalColumnReaderFactory.createReader(descriptor, decimalType.getPrecision(), decimalType.getScale()));
        }
        return Optional.empty();
    }

    public PrimitiveColumnReader(RichColumnDescriptor columnDescriptor)
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

    public ColumnDescriptor getDescriptor()
    {
        return columnDescriptor;
    }

    public ColumnChunk readPrimitive(Field field)
            throws IOException
    {
        IntList definitionLevels = new IntArrayList();
        IntList repetitionLevels = new IntArrayList();
        seek();
        BlockBuilder blockBuilder = field.getType().createBlockBuilder(null, nextBatchSize);
        int valueCount = 0;
        while (valueCount < nextBatchSize) {
            if (page == null) {
                readNextPage();
            }
            int valuesToRead = Math.min(remainingValueCountInPage, nextBatchSize - valueCount);
            readValues(blockBuilder, valuesToRead, field.getType(), definitionLevels, repetitionLevels);
            valueCount += valuesToRead;
        }
        checkArgument(valueCount == nextBatchSize, "valueCount %s not equals to batchSize %s", valueCount, nextBatchSize);

        readOffset = 0;
        nextBatchSize = 0;
        return new ColumnChunk(blockBuilder.build(), definitionLevels.toIntArray(), repetitionLevels.toIntArray());
    }

    private void readValues(BlockBuilder blockBuilder, int valuesToRead, Type type, IntList definitionLevels, IntList repetitionLevels)
    {
        processValues(valuesToRead, ignored -> {
            readValue(blockBuilder, type);
            definitionLevels.add(definitionLevel);
            repetitionLevels.add(repetitionLevel);
        });
    }

    private void skipValues(int valuesToRead)
    {
        processValues(valuesToRead, ignored -> skipValue());
    }

    private void processValues(int valuesToRead, Consumer<Void> valueConsumer)
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
                    updateValueCounts(valueCount);
                    if (!readNextPage()) {
                        return;
                    }
                    valueCount = 0;
                }
                repetitionLevel = repetitionReader.readLevel();
                definitionLevel = definitionReader.readLevel();
            }
            while (repetitionLevel != 0);
        }
        updateValueCounts(valueCount);
    }

    private void seek()
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
        checkArgument(valuePosition == readOffset, "valuePosition %s must be equal to readOffset %s", valuePosition, readOffset);
    }

    private boolean readNextPage()
    {
        verify(page == null, "readNextPage has to be called when page is null");
        page = pageReader.readPage();
        if (page == null) {
            // we have read all pages
            return false;
        }
        remainingValueCountInPage = page.getValueCount();
        if (page instanceof DataPageV1) {
            valuesReader = readPageV1((DataPageV1) page);
        }
        else {
            valuesReader = readPageV2((DataPageV2) page);
        }
        return true;
    }

    private void updateValueCounts(int valuesRead)
    {
        if (valuesRead == remainingValueCountInPage) {
            page = null;
            valuesReader = null;
        }
        remainingValueCountInPage -= valuesRead;
        currentValueCount += valuesRead;
    }

    private ValuesReader readPageV1(DataPageV1 page)
    {
        ValuesReader rlReader = page.getRepetitionLevelEncoding().getValuesReader(columnDescriptor, REPETITION_LEVEL);
        ValuesReader dlReader = page.getDefinitionLevelEncoding().getValuesReader(columnDescriptor, DEFINITION_LEVEL);
        repetitionReader = new LevelValuesReader(rlReader);
        definitionReader = new LevelValuesReader(dlReader);
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

    private ValuesReader readPageV2(DataPageV2 page)
    {
        repetitionReader = buildLevelRLEReader(columnDescriptor.getMaxRepetitionLevel(), page.getRepetitionLevels());
        definitionReader = buildLevelRLEReader(columnDescriptor.getMaxDefinitionLevel(), page.getDefinitionLevels());
        return initDataReader(page.getDataEncoding(), page.getSlice().getBytes(), 0, page.getValueCount());
    }

    private LevelReader buildLevelRLEReader(int maxLevel, Slice slice)
    {
        if (maxLevel == 0) {
            return new LevelNullReader();
        }
        return new LevelRLEReader(new RunLengthBitPackingHybridDecoder(BytesUtils.getWidthFromMaxInt(maxLevel), new ByteArrayInputStream(slice.getBytes())));
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
