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
import com.facebook.presto.common.type.Type;
import com.facebook.presto.parquet.ColumnReader;
import com.facebook.presto.parquet.DataPage;
import com.facebook.presto.parquet.DataPageV1;
import com.facebook.presto.parquet.DataPageV2;
import com.facebook.presto.parquet.DictionaryPage;
import com.facebook.presto.parquet.Field;
import com.facebook.presto.parquet.ParquetEncoding;
import com.facebook.presto.parquet.ParquetTypeUtils;
import com.facebook.presto.parquet.RichColumnDescriptor;
import com.facebook.presto.parquet.dictionary.Dictionary;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridDecoder;
import org.apache.parquet.internal.filter2.columnindex.RowRanges;
import org.apache.parquet.io.ParquetDecodingException;

import java.io.IOException;
import java.util.PrimitiveIterator;
import java.util.function.Consumer;

import static com.facebook.presto.parquet.ValuesType.DEFINITION_LEVEL;
import static com.facebook.presto.parquet.ValuesType.REPETITION_LEVEL;
import static com.facebook.presto.parquet.ValuesType.VALUES;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

public abstract class AbstractColumnReader
        implements ColumnReader
{
    private static final int EMPTY_LEVEL_VALUE = -1;
    protected final RichColumnDescriptor columnDescriptor;

    protected int definitionLevel = EMPTY_LEVEL_VALUE;
    protected int repetitionLevel = EMPTY_LEVEL_VALUE;
    protected ValuesReader valuesReader;

    private Field field;
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
    private PrimitiveIterator.OfLong indexIterator;
    private long currentRow;
    private long targetRow;

    protected abstract void readValue(BlockBuilder blockBuilder, Type type);

    protected abstract void skipValue();

    protected boolean isValueNull()
    {
        return ParquetTypeUtils.isValueNull(columnDescriptor.isRequired(), definitionLevel, columnDescriptor.getMaxDefinitionLevel());
    }

    public AbstractColumnReader(RichColumnDescriptor columnDescriptor)
    {
        this.columnDescriptor = requireNonNull(columnDescriptor, "columnDescriptor");
        pageReader = null;
        this.targetRow = Long.MIN_VALUE;
        this.indexIterator = null;
    }

    @Override
    public boolean isInitialized()
    {
        return pageReader != null && field != null;
    }

    @Override
    public void init(PageReader pageReader, Field field, RowRanges rowRanges)
    {
        this.pageReader = requireNonNull(pageReader, "pageReader is null");
        this.field = requireNonNull(field, "field is null");
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
        indexIterator = (rowRanges == null) ? null : rowRanges.iterator();
    }

    @Override
    public void prepareNextRead(int batchSize)
    {
        readOffset = readOffset + nextBatchSize;
        nextBatchSize = batchSize;
    }

    @Override
    public ColumnChunk readNext()
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
            if (valuesToRead == 0) {
                // When we break here, we could end up with valueCount < nextBatchSize, this is because we may skip reading values in readValues()
                break;
            }
            readValues(blockBuilder, valuesToRead, field.getType(), definitionLevels, repetitionLevels);
            valueCount += valuesToRead;
        }

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
        }, indexIterator != null);
    }

    private void skipValues(int valuesToRead)
    {
        processValues(valuesToRead, ignored -> skipValue(), false);
    }

    /**
     * When filtering using column indexes we might skip reading some pages for different columns. Because the rows are
     * not aligned between the pages of the different columns it might be required to skip some values. The values (and the
     * related rl and dl) are skipped based on the iterator of the required row indexes and the first row index of each
     * page.
     * For example:
     *
     * rows   col1   col2   col3
     *      ┌──────┬──────┬──────┐
     *   0  │  p0  │      │      │
     *      ╞══════╡  p0  │  p0  │
     *  20  │ p1(X)│------│------│
     *      ╞══════╪══════╡      │
     *  40  │ p2(X)│      │------│
     *      ╞══════╡ p1(X)╞══════╡
     *  60  │ p3(X)│      │------│
     *      ╞══════╪══════╡      │
     *  80  │  p4  │      │  p1  │
     *      ╞══════╡  p2  │      │
     * 100  │  p5  │      │      │
     *      └──────┴──────┴──────┘
     *
     * The pages 1, 2, 3 in col1 are skipped so we have to skip the rows [20, 79]. Because page 1 in col2 contains values
     * only for the rows [40, 79] we skip this entire page as well. To synchronize the row reading we have to skip the
     * values (and the related rl and dl) for the rows [20, 39] in the end of the page 0 for col2. Similarly, we have to
     * skip values while reading page0 and page1 for col3.
     */
    private void processValues(int valuesToRead, Consumer<Void> valueConsumer, boolean indexEnabled)
    {
        if (definitionLevel == EMPTY_LEVEL_VALUE && repetitionLevel == EMPTY_LEVEL_VALUE) {
            definitionLevel = definitionReader.readLevel();
            repetitionLevel = repetitionReader.readLevel();
        }
        int valueCount = 0;
        int skipCount = 0;
        for (int i = 0; i < valuesToRead; ) {
            boolean consumed = false;
            do {
                if (skipRL(repetitionLevel, indexEnabled)) {
                    skipValue();
                    skipCount++;
                }
                else {
                    valueConsumer.accept(null);
                    valueCount++;
                    consumed = true;
                }

                if (valueCount + skipCount == remainingValueCountInPage) {
                    updateValueCounts(valueCount, skipCount);
                    if (!readNextPage()) {
                        return;
                    }
                    valueCount = 0;
                    skipCount = 0;
                }

                repetitionLevel = repetitionReader.readLevel();
                definitionLevel = definitionReader.readLevel();
            }
            while (repetitionLevel != 0);

            if (consumed) {
                i++;
            }
        }
        updateValueCounts(valueCount, skipCount);
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

    private void updateValueCounts(int valuesRead, int skipCount)
    {
        int totalCount = valuesRead + skipCount;
        if (totalCount == remainingValueCountInPage) {
            page = null;
            valuesReader = null;
        }
        remainingValueCountInPage -= totalCount;
        currentValueCount += valuesRead;
    }

    private ValuesReader readPageV1(DataPageV1 page)
    {
        ValuesReader repetitionLevelReader = page.getRepetitionLevelEncoding().getValuesReader(columnDescriptor, REPETITION_LEVEL);
        ValuesReader definitionLevelReader = page.getDefinitionLevelEncoding().getValuesReader(columnDescriptor, DEFINITION_LEVEL);
        repetitionReader = new LevelValuesReader(repetitionLevelReader);
        definitionReader = new LevelValuesReader(definitionLevelReader);
        try {
            ByteBufferInputStream bufferInputStream = ByteBufferInputStream.wrap(page.getSlice().toByteBuffer());
            repetitionLevelReader.initFromPage(page.getValueCount(), bufferInputStream);
            definitionLevelReader.initFromPage(page.getValueCount(), bufferInputStream);
            long firstRowIndex = page.getFirstRowIndex().orElse(-1L);
            return initDataReader(page.getValueEncoding(), bufferInputStream, page.getValueCount(), firstRowIndex);
        }
        catch (IOException e) {
            throw new ParquetDecodingException("Error reading parquet page " + page + " in column " + columnDescriptor, e);
        }
    }

    private ValuesReader readPageV2(DataPageV2 page)
    {
        repetitionReader = buildLevelRLEReader(columnDescriptor.getMaxRepetitionLevel(), page.getRepetitionLevels());
        definitionReader = buildLevelRLEReader(columnDescriptor.getMaxDefinitionLevel(), page.getDefinitionLevels());
        long firstRowIndex = page.getFirstRowIndex().orElse(-1L);
        return initDataReader(page.getDataEncoding(), ByteBufferInputStream.wrap(ImmutableList.of(page.getSlice().toByteBuffer())), page.getValueCount(), firstRowIndex);
    }

    private LevelReader buildLevelRLEReader(int maxLevel, Slice slice)
    {
        if (maxLevel == 0) {
            return new LevelNullReader();
        }

        return new LevelRLEReader(new RunLengthBitPackingHybridDecoder(BytesUtils.getWidthFromMaxInt(maxLevel), slice.getInput()));
    }

    private ValuesReader initDataReader(ParquetEncoding dataEncoding, ByteBufferInputStream inputStream, int valueCount, long firstRowIndex)
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
            valuesReader.initFromPage(valueCount, inputStream);
            if (firstRowIndex != -1) {
                currentRow = firstRowIndex - 1;
            }
            else {
                currentRow = -1;
            }
            return valuesReader;
        }
        catch (IOException e) {
            throw new ParquetDecodingException("Error reading parquet page in column " + columnDescriptor, e);
        }
    }

    private boolean skipRL(int repetitionLevel, boolean indexEnabled)
    {
        if (!indexEnabled || indexIterator == null) {
            return false;
        }

        if (repetitionLevel == 0) {
            currentRow = currentRow + 1;
            if (currentRow > targetRow) {
                targetRow = indexIterator.hasNext() ? indexIterator.nextLong() : Long.MAX_VALUE;
            }
        }

        return currentRow < targetRow;
    }
}
