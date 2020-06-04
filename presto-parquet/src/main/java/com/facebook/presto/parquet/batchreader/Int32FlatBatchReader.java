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

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.IntArrayBlock;
import com.facebook.presto.common.block.RunLengthEncodedBlock;
import com.facebook.presto.parquet.ColumnReader;
import com.facebook.presto.parquet.DataPage;
import com.facebook.presto.parquet.DictionaryPage;
import com.facebook.presto.parquet.Field;
import com.facebook.presto.parquet.RichColumnDescriptor;
import com.facebook.presto.parquet.batchreader.decoders.Decoders.FlatDecoders;
import com.facebook.presto.parquet.batchreader.decoders.FlatDefinitionLevelDecoder;
import com.facebook.presto.parquet.batchreader.decoders.ValuesDecoder.Int32ValuesDecoder;
import com.facebook.presto.parquet.batchreader.dictionary.Dictionaries;
import com.facebook.presto.parquet.dictionary.Dictionary;
import com.facebook.presto.parquet.reader.ColumnChunk;
import com.facebook.presto.parquet.reader.PageReader;
import com.facebook.presto.spi.PrestoException;
import org.apache.parquet.io.ParquetDecodingException;

import java.io.IOException;
import java.util.Optional;

import static com.facebook.presto.parquet.ParquetErrorCode.PARQUET_IO_READ_ERROR;
import static com.facebook.presto.parquet.batchreader.decoders.Decoders.readFlatPage;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class Int32FlatBatchReader
        implements ColumnReader
{
    private final RichColumnDescriptor columnDescriptor;

    protected Field field;
    protected int nextBatchSize;
    protected FlatDefinitionLevelDecoder definitionLevelDecoder;
    protected Int32ValuesDecoder valuesDecoder;
    protected int remainingCountInPage;

    private Dictionary dictionary;
    private int readOffset;
    private PageReader pageReader;

    public Int32FlatBatchReader(RichColumnDescriptor columnDescriptor)
    {
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
        checkArgument(!isInitialized(), "Parquet batch reader already initialized");
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
                columnChunk = readWithoutNull();
            }
            else {
                columnChunk = readWithNull();
            }
        }
        catch (IOException exception) {
            throw new PrestoException(PARQUET_IO_READ_ERROR, "Error reading Parquet column " + columnDescriptor, exception);
        }

        readOffset = 0;
        nextBatchSize = 0;
        return columnChunk;
    }

    protected boolean readNextPage()
    {
        definitionLevelDecoder = null;
        valuesDecoder = null;
        remainingCountInPage = 0;

        DataPage page = pageReader.readPage();
        if (page == null) {
            return false;
        }

        FlatDecoders flatDecoders = readFlatPage(page, columnDescriptor, dictionary);
        definitionLevelDecoder = flatDecoders.getDefinitionLevelDecoder();
        valuesDecoder = (Int32ValuesDecoder) flatDecoders.getValuesDecoder();

        remainingCountInPage = page.getValueCount();
        return true;
    }

    private ColumnChunk readWithNull()
            throws IOException
    {
        int[] values = new int[nextBatchSize];
        boolean[] isNull = new boolean[nextBatchSize];

        int totalNonNullCount = 0;
        int remainingInBatch = nextBatchSize;
        int startOffset = 0;
        while (remainingInBatch > 0) {
            if (remainingCountInPage == 0) {
                if (!readNextPage()) {
                    break;
                }
            }

            int chunkSize = Math.min(remainingCountInPage, remainingInBatch);
            int nonNullCount = definitionLevelDecoder.readNext(isNull, startOffset, chunkSize);
            totalNonNullCount += nonNullCount;

            if (nonNullCount > 0) {
                valuesDecoder.readNext(values, startOffset, nonNullCount);

                int valueDestinationIndex = startOffset + chunkSize - 1;
                int valueSourceIndex = startOffset + nonNullCount - 1;

                while (valueDestinationIndex >= startOffset) {
                    if (!isNull[valueDestinationIndex]) {
                        values[valueDestinationIndex] = values[valueSourceIndex];
                        valueSourceIndex--;
                    }
                    valueDestinationIndex--;
                }
            }

            startOffset += chunkSize;
            remainingInBatch -= chunkSize;
            remainingCountInPage -= chunkSize;
        }

        if (remainingInBatch != 0) {
            throw new ParquetDecodingException("Still remaining to be read in current batch.");
        }

        if (totalNonNullCount == 0) {
            Block block = RunLengthEncodedBlock.create(field.getType(), null, nextBatchSize);
            return new ColumnChunk(block, new int[0], new int[0]);
        }

        boolean hasNoNull = totalNonNullCount == nextBatchSize;
        Block block = new IntArrayBlock(nextBatchSize, hasNoNull ? Optional.empty() : Optional.of(isNull), values);
        return new ColumnChunk(block, new int[0], new int[0]);
    }

    private ColumnChunk readWithoutNull()
            throws IOException
    {
        int[] values = new int[nextBatchSize];
        int remainingInBatch = nextBatchSize;
        int startOffset = 0;
        while (remainingInBatch > 0) {
            if (remainingCountInPage == 0) {
                if (!readNextPage()) {
                    break;
                }
            }

            int chunkSize = Math.min(remainingCountInPage, remainingInBatch);

            valuesDecoder.readNext(values, startOffset, chunkSize);
            startOffset += chunkSize;
            remainingInBatch -= chunkSize;
            remainingCountInPage -= chunkSize;
        }

        if (remainingInBatch != 0) {
            throw new ParquetDecodingException(format("Corrupted Parquet file: extra %d values to be consumed when scanning current batch", remainingInBatch));
        }

        Block block = new IntArrayBlock(nextBatchSize, Optional.empty(), values);
        return new ColumnChunk(block, new int[0], new int[0]);
    }

    private void seek()
            throws IOException
    {
        if (readOffset == 0) {
            return;
        }

        int remainingInBatch = readOffset;
        int startOffset = 0;
        while (remainingInBatch > 0) {
            if (remainingCountInPage == 0) {
                if (!readNextPage()) {
                    break;
                }
            }

            int chunkSize = Math.min(remainingCountInPage, remainingInBatch);
            int skipSize = chunkSize;
            if (!columnDescriptor.isRequired()) {
                boolean[] isNull = new boolean[readOffset];
                int nonNullCount = definitionLevelDecoder.readNext(isNull, startOffset, chunkSize);
                skipSize = nonNullCount;
                startOffset += chunkSize;
            }
            valuesDecoder.skip(skipSize);
            remainingInBatch -= chunkSize;
            remainingCountInPage -= chunkSize;
        }
    }
}
