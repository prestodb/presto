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
package com.facebook.presto.parquet.batchreader;

import com.facebook.presto.parquet.ColumnReader;
import com.facebook.presto.parquet.DataPage;
import com.facebook.presto.parquet.DictionaryPage;
import com.facebook.presto.parquet.Field;
import com.facebook.presto.parquet.RichColumnDescriptor;
import com.facebook.presto.parquet.batchreader.decoders.Decoders.NestedDecoders;
import com.facebook.presto.parquet.batchreader.decoders.DefinitionLevelDecoder;
import com.facebook.presto.parquet.batchreader.decoders.RepetitionLevelDecoder;
import com.facebook.presto.parquet.batchreader.decoders.ValuesDecoder;
import com.facebook.presto.parquet.batchreader.dictionary.Dictionaries;
import com.facebook.presto.parquet.dictionary.Dictionary;
import com.facebook.presto.parquet.reader.ColumnChunk;
import com.facebook.presto.parquet.reader.PageReader;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.parquet.Preconditions;
import org.apache.parquet.io.ParquetDecodingException;

import java.io.IOException;
import java.util.List;

import static com.facebook.presto.parquet.batchreader.decoders.Decoders.readNestedPage;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public abstract class AbstractNestedBatchReader
        implements ColumnReader
{
    protected final RichColumnDescriptor columnDescriptor;

    protected Field field;
    protected int nextBatchSize;
    protected int readOffset;

    private Dictionary dictionary;
    private RepetitionLevelDecoder repetitionLevelDecoder;
    private DefinitionLevelDecoder definitionLevelDecoder;
    private ValuesDecoder valuesDecoder;
    private int remainingCountInPage;
    private PageReader pageReader;
    private int lastRepetitionLevel = -1;

    protected abstract ColumnChunk readNestedNoNull()
            throws IOException;

    protected abstract ColumnChunk readNestedWithNull()
            throws IOException;

    protected abstract void seek()
            throws IOException;

    public AbstractNestedBatchReader(RichColumnDescriptor columnDescriptor)
    {
        checkArgument(columnDescriptor.getPath().length > 1, "expected to read a nested column");
        this.columnDescriptor = requireNonNull(columnDescriptor, "columnDescriptor is null");
    }

    @Override
    public boolean isInitialized()
    {
        return pageReader != null && field != null;
    }

    @Override
    public void init(PageReader pageReader, Field field)
    {
        Preconditions.checkState(!isInitialized(), "already initialized");
        this.pageReader = requireNonNull(pageReader, "pageReader is null");
        checkArgument(pageReader.getTotalValueCount() > 0, "page is empty");

        this.field = requireNonNull(field, "field is null");

        DictionaryPage dictionaryPage = pageReader.readDictionaryPage();
        if (dictionaryPage != null) {
            dictionary = Dictionaries.createDictionary(columnDescriptor, dictionaryPage);
        }
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
        ColumnChunk columnChunk = null;
        try {
            seek();
            if (field.isRequired()) {
                columnChunk = readNestedNoNull();
            }
            else {
                columnChunk = readNestedWithNull();
            }
        }
        catch (IOException ex) {
            throw new ParquetDecodingException("Failed to decode.", ex);
        }

        readOffset = 0;
        nextBatchSize = 0;
        return columnChunk;
    }

    protected void readNextPage()
    {
        remainingCountInPage = 0;

        DataPage page = pageReader.readPage();
        if (page == null) {
            return;
        }

        NestedDecoders nestedDecoders = readNestedPage(page, columnDescriptor, dictionary);
        repetitionLevelDecoder = nestedDecoders.getRepetitionLevelDecoder();
        definitionLevelDecoder = nestedDecoders.getDefinitionLevelDecoder();
        valuesDecoder = nestedDecoders.getValuesDecoder();
        remainingCountInPage = page.getValueCount();
    }

    protected final RepetitionLevelDecodingContext readRepetitionLevels(int batchSize)
            throws IOException
    {
        IntList repetitionLevels = new IntArrayList(batchSize);
        RepetitionLevelDecodingContext repetitionLevelDecodingContext = new RepetitionLevelDecodingContext();
        int remainingInBatch = batchSize + 1;

        if (remainingCountInPage == 0) {
            readNextPage();
        }

        int startOffset = 0;

        if (lastRepetitionLevel != -1) {
            repetitionLevels.add(lastRepetitionLevel);
            lastRepetitionLevel = -1;
            remainingInBatch--;
        }

        while (remainingInBatch > 0) {
            int valueCount = repetitionLevelDecoder.readNext(repetitionLevels, remainingInBatch);
            if (valueCount == 0) {
                int endOffset = repetitionLevels.size();
                repetitionLevelDecodingContext.add(new DefinitionLevelValuesDecoderContext(definitionLevelDecoder, valuesDecoder, startOffset, endOffset));
                remainingCountInPage -= (endOffset - startOffset);
                startOffset = endOffset;
                readNextPage();
                if (remainingCountInPage == 0) {
                    break;
                }
            }
            else {
                remainingInBatch -= valueCount;
            }
        }

        if (remainingInBatch == 0) {
            lastRepetitionLevel = 0;
            repetitionLevels.remove(repetitionLevels.size() - 1);
        }

        if (repetitionLevelDecoder != null) {
            repetitionLevelDecodingContext.add(new DefinitionLevelValuesDecoderContext(definitionLevelDecoder, valuesDecoder, startOffset, repetitionLevels.size()));
        }
        repetitionLevelDecodingContext.setRepetitionLevels(repetitionLevels.toIntArray());
        return repetitionLevelDecodingContext;
    }

    protected final DefinitionLevelDecodingContext readDefinitionLevels(List<DefinitionLevelValuesDecoderContext> decoderInfos, int batchSize)
            throws IOException
    {
        DefinitionLevelDecodingContext definitionLevelDecodingContext = new DefinitionLevelDecodingContext();

        int[] definitionLevels = new int[batchSize];
        int remainingInBatch = batchSize;
        for (DefinitionLevelValuesDecoderContext decoderInfo : decoderInfos) {
            int readChunkSize = decoderInfo.getEnd() - decoderInfo.getStart();
            decoderInfo.getDefinitionLevelDecoder().readNext(definitionLevels, decoderInfo.getStart(), readChunkSize);
            definitionLevelDecodingContext.add(new ValuesDecoderContext(decoderInfo.getValuesDecoder(), decoderInfo.getStart(), decoderInfo.getEnd()));
            remainingInBatch -= readChunkSize;
        }

        if (remainingInBatch != 0) {
            throw new IllegalStateException("We didn't read correct number of definitionLevels");
        }

        definitionLevelDecodingContext.setDefinitionLevels(definitionLevels);
        return definitionLevelDecodingContext;
    }
}
