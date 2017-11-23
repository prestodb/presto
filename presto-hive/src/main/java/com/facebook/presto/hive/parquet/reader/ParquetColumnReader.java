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
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.Slice;
import it.unimi.dsi.fastutil.ints.IntList;
import parquet.bytes.BytesUtils;
import parquet.column.ColumnDescriptor;
import parquet.column.values.ValuesReader;
import parquet.column.values.rle.RunLengthBitPackingHybridDecoder;
import parquet.io.ParquetDecodingException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Optional;

import static com.facebook.presto.hive.parquet.ParquetTypeUtils.createDecimalType;
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

    protected int definitionLevel;
    protected ValuesReader valuesReader;
    protected int nextBatchSize;

    private ParquetLevelReader repetitionReader;
    private ParquetLevelReader definitionReader;
    private int repetitionLevel;
    private long totalValueCount;
    private ParquetPageReader pageReader;
    private ParquetDictionary dictionary;
    private int currentValueCount;
    private ParquetDataPage page;
    private int remainingValueCountInPage;
    private int readOffset;

    protected abstract void readValue(BlockBuilder blockBuilder, Type type, Optional<boolean[]> isNullAtRowNum, boolean isMapKey, boolean isMapVal, int mapRowNum);

    protected abstract void skipValue();

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
        Optional<Type> type = createDecimalType(descriptor);
        if (type.isPresent()) {
            DecimalType decimalType = (DecimalType) type.get();
            return Optional.of(ParquetDecimalColumnReaderFactory.createReader(descriptor, decimalType.getPrecision(), decimalType.getScale()));
        }
        return Optional.empty();
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

    public ColumnDescriptor getDescriptor()
    {
        return columnDescriptor;
    }

    public Block readPrimitive(Type type, IntList positions, Optional<boolean[]> isNullAtRowNum, boolean isMapKey, boolean isMapVal)
            throws IOException
    {
        seek();
        BlockBuilder blockBuilder = type.createBlockBuilder(new BlockBuilderStatus(), nextBatchSize);
        int valueCount = 0;
        while (valueCount < nextBatchSize) {
            if (page == null) {
                readNextPage();
            }
            int numValues = Math.min(remainingValueCountInPage, nextBatchSize - valueCount);
            int valuesRead = readValues(blockBuilder, numValues, type, positions, isNullAtRowNum, isMapKey, isMapVal);
            valueCount += numValues;
            updatePosition(valuesRead);
        }
        checkArgument(valueCount == nextBatchSize, "valueCount %s not equals to batchSize %s", valueCount, nextBatchSize);

        readOffset = 0;
        nextBatchSize = 0;
        return blockBuilder.build();
    }

    private int readValues(BlockBuilder blockBuilder, int numValues, Type type, IntList positions, Optional<boolean[]> isNullAtRowNum, boolean isMapKey, boolean isMapVal)
    {
        int totalValsReadFromPage = 0;
        for (int i = 0; i < numValues; i++) {
            int positionCount = blockBuilder.getPositionCount();
            do {
                definitionLevel = definitionReader.readLevel();
                repetitionLevel = repetitionReader.readLevel();
                readValue(blockBuilder, type, isNullAtRowNum, isMapKey, isMapVal, positions.size());
                int pCount = blockBuilder.getPositionCount();
                try {
                    totalValsReadFromPage++;
                    // If we have reached the end of the page then we should not peek the repetitionLevel but load the next page.
                    // This is done to handle the case where the a repetitive structure with multiple columns(like map or row type)
                    // has repetition that spans across page boundaries i.e. a map with n key-vals pairs where the data for
                    // key-val 1 to m is on first page and m+1 to n is on second page.
                    if (remainingValueCountInPage - totalValsReadFromPage == 0 && pageReader.hasMorePages()) {
                        readNextPage();
                        currentValueCount += totalValsReadFromPage;
                        totalValsReadFromPage = 0;
                    }
                    if (remainingValueCountInPage - totalValsReadFromPage == 0) {
                        positions.add(pCount - positionCount);
                        return totalValsReadFromPage;
                    }
                    repetitionLevel = repetitionReader.peekLevel();
                    if (repetitionLevel == 0) {
                        positions.add(pCount - positionCount);
                    }
                }
                catch (IllegalArgumentException expected) {
                    // Reading past repetition stream, RunLengthBitPackingHybridDecoder throws IllegalArgumentException
                    positions.add(pCount - positionCount);
                    // It's unclear why would we ever read past the stream ideally we should not have to handle the exception.
                    return totalValsReadFromPage;
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            while (repetitionLevel != 0);
        }
        return totalValsReadFromPage;
    }

    private void skipValues(int offset)
    {
        for (int i = 0; i < offset; i++) {
            do {
                definitionLevel = definitionReader.readLevel();
                repetitionLevel = repetitionReader.readLevel();
                skipValue();
                try {
                    repetitionLevel = repetitionReader.peekLevel();
                    if (i == offset - 1 && repetitionLevel == 0) {
                        return;
                    }
                }
                catch (IllegalArgumentException expected) {
                    // Reading past repetition stream, RunLengthBitPackingHybridDecoder throws IllegalArgumentException
                    return;
                }
            }
            while (repetitionLevel != 0);
        }
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
            updatePosition(offset);
        }
        checkArgument(valuePosition == readOffset, "valuePosition %s must be equal to readOffset %s", valuePosition, readOffset);
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

    private void updatePosition(int numValues)
    {
        if (numValues == remainingValueCountInPage) {
            page = null;
            valuesReader = null;
        }
        remainingValueCountInPage = remainingValueCountInPage - numValues;
        currentValueCount += numValues;
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

    protected void handleNull(BlockBuilder blockBuilder, Optional<boolean[]> isNullAtRowNum, boolean isMapKey, boolean isMapVal, int position)
    {
        // if isNullAtRowNum is already set to true for a position that indicates we are in call tree of a map'/s
        // processing and the key is null. to keep the key and val length equal we make sure we do not append null
        // for both the key and value. we only want to set the is NullAtRowNum to true if the key is null as null
        // values for maps are not allowed.
        if (isMapKey) {
            isNullAtRowNum.map((arr) -> arr[position] = true);
        }
        // if this not a map processing and definitionLeve is 0, it inidicates a complex structure(list or row)
        // is it self null.
        else if (!isMapKey && !isMapVal && definitionLevel == 0 && isNullAtRowNum.isPresent()) {
            isNullAtRowNum.map((arr) -> arr[position] = true);
            blockBuilder.appendNull();
        }
        // if isNullAtRowNum is not present, that means this is not call tree of a map's value processing
        // if it is present , then this is call tree of map's value and in that case we only append null
        // if the key was not null.
        else if (!isNullAtRowNum.isPresent() || !isNullAtRowNum.get()[position]) {
            // we only append null if either this is not a map value processing call tree
            // or if in case this is a map value call tree, the key at this position is not set to null.
            blockBuilder.appendNull();
        }
    }
}
