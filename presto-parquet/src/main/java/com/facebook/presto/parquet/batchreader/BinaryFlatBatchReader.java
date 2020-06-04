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
import com.facebook.presto.common.block.RunLengthEncodedBlock;
import com.facebook.presto.common.block.VariableWidthBlock;
import com.facebook.presto.parquet.ColumnReader;
import com.facebook.presto.parquet.DataPage;
import com.facebook.presto.parquet.DictionaryPage;
import com.facebook.presto.parquet.Field;
import com.facebook.presto.parquet.RichColumnDescriptor;
import com.facebook.presto.parquet.batchreader.decoders.Decoders.FlatDecoders;
import com.facebook.presto.parquet.batchreader.decoders.FlatDefinitionLevelDecoder;
import com.facebook.presto.parquet.batchreader.decoders.ValuesDecoder.BinaryValuesDecoder;
import com.facebook.presto.parquet.batchreader.decoders.ValuesDecoder.BinaryValuesDecoder.ReadChunk;
import com.facebook.presto.parquet.batchreader.dictionary.Dictionaries;
import com.facebook.presto.parquet.dictionary.Dictionary;
import com.facebook.presto.parquet.reader.ColumnChunk;
import com.facebook.presto.parquet.reader.PageReader;
import com.facebook.presto.spi.PrestoException;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.parquet.ParquetErrorCode.PARQUET_IO_READ_ERROR;
import static com.facebook.presto.parquet.batchreader.decoders.Decoders.readFlatPage;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class BinaryFlatBatchReader
        implements ColumnReader
{
    private final RichColumnDescriptor columnDescriptor;

    protected Field field;
    protected int nextBatchSize;
    protected FlatDefinitionLevelDecoder definitionLevelDecoder;
    protected BinaryValuesDecoder valuesDecoder;
    protected int remainingCountInPage;

    private Dictionary dictionary;
    private int readOffset;
    private PageReader pageReader;

    public BinaryFlatBatchReader(RichColumnDescriptor columnDescriptor)
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
        catch (IOException ex) {
            throw new PrestoException(PARQUET_IO_READ_ERROR, "Error reading Parquet column " + columnDescriptor, ex);
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
        valuesDecoder = (BinaryValuesDecoder) flatDecoders.getValuesDecoder();

        remainingCountInPage = page.getValueCount();
        return true;
    }

    private ColumnChunk readWithNull()
            throws IOException
    {
        boolean[] isNull = new boolean[nextBatchSize];

        List<ReadChunk> readChunkList = new ArrayList<>();
        List<ValuesDecoderInfo> valuesDecoderInfos = new ArrayList<>();
        int bufferSize = 0;

        int totalNonNullCount = 0;
        int remainingInBatch = nextBatchSize;
        int startOffset = 0;
        while (remainingInBatch > 0) {
            if (remainingCountInPage == 0) {
                if (!readNextPage()) {
                    break;
                }
            }

            int readChunkSize = Math.min(remainingCountInPage, remainingInBatch);
            int nonNullCount = definitionLevelDecoder.readNext(isNull, startOffset, readChunkSize);
            totalNonNullCount += nonNullCount;

            ReadChunk readChunk = valuesDecoder.readNext(nonNullCount);

            bufferSize += readChunk.getBufferSize();
            readChunkList.add(readChunk);

            ValuesDecoderInfo<BinaryValuesDecoder> valuesDecoderInfo = new ValuesDecoderInfo(valuesDecoder, startOffset, startOffset + readChunkSize);
            valuesDecoderInfo.setValueCount(readChunkSize);
            valuesDecoderInfo.setNonNullCount(nonNullCount);
            valuesDecoderInfos.add(valuesDecoderInfo);

            startOffset += readChunkSize;
            remainingInBatch -= readChunkSize;
            remainingCountInPage -= readChunkSize;
        }

        if (totalNonNullCount == 0) {
            Block block = RunLengthEncodedBlock.create(field.getType(), null, nextBatchSize);
            return new ColumnChunk(block, new int[0], new int[0]);
        }

        byte[] byteBuffer = new byte[bufferSize];
        int[] offsets = new int[nextBatchSize + 1];

        int i = 0;
        int bufferIndex = 0;
        int offsetIndex = 0;
        for (ValuesDecoderInfo<BinaryValuesDecoder> valuesDecoderInfo : valuesDecoderInfos) {
            BinaryValuesDecoder binaryValuesDecoder = valuesDecoderInfo.getValuesDecoder();
            ReadChunk readChunk = readChunkList.get(i);
            bufferIndex = binaryValuesDecoder.readIntoBuffer(byteBuffer, bufferIndex, offsets, offsetIndex, readChunk);
            offsetIndex += valuesDecoderInfo.getValueCount();
            i++;
        }

        Collections.reverse(valuesDecoderInfos);
        for (ValuesDecoderInfo valuesDecoderInfo : valuesDecoderInfos) {
            int destinationIndex = valuesDecoderInfo.getEnd() - 1;
            int sourceIndex = valuesDecoderInfo.getStart() + valuesDecoderInfo.getNonNullCount() - 1;

            offsets[destinationIndex + 1] = offsets[sourceIndex + 1];
            while (destinationIndex >= valuesDecoderInfo.getStart()) {
                if (!isNull[destinationIndex]) {
                    offsets[destinationIndex] = offsets[sourceIndex];
                    sourceIndex--;
                }
                else {
                    offsets[destinationIndex] = offsets[sourceIndex + 1];
                }
                destinationIndex--;
            }
        }

        Slice buffer = Slices.wrappedBuffer(byteBuffer, 0, bufferSize);
        boolean hasNoNull = totalNonNullCount == nextBatchSize;
        Block block = new VariableWidthBlock(nextBatchSize, buffer, offsets, hasNoNull ? Optional.empty() : Optional.of(isNull));
        return new ColumnChunk(block, new int[0], new int[0]);
    }

    private ColumnChunk readWithoutNull()
            throws IOException
    {
        boolean[] isNull = new boolean[nextBatchSize];
        List<ReadChunk> readChunkList = new ArrayList<>();
        List<ValuesDecoderInfo> valuesDecoderInfos = new ArrayList<>();
        int bufferSize = 0;

        int remainingInBatch = nextBatchSize;
        int startOffset = 0;
        while (remainingInBatch > 0) {
            if (remainingCountInPage == 0) {
                if (!readNextPage()) {
                    break;
                }
            }

            int readChunkSize = Math.min(remainingCountInPage, remainingInBatch);

            ReadChunk readChunk = valuesDecoder.readNext(readChunkSize);
            bufferSize += readChunk.getBufferSize();
            readChunkList.add(readChunk);

            ValuesDecoderInfo<BinaryValuesDecoder> valuesDecoderInfo = new ValuesDecoderInfo(valuesDecoder, startOffset, startOffset + readChunkSize);
            valuesDecoderInfo.setValueCount(readChunkSize);
            valuesDecoderInfo.setNonNullCount(readChunkSize);
            valuesDecoderInfos.add(valuesDecoderInfo);

            startOffset += readChunkSize;
            remainingInBatch -= readChunkSize;
            remainingCountInPage -= readChunkSize;
        }

        byte[] byteBuffer = new byte[bufferSize];
        int[] offsets = new int[nextBatchSize + 1];

        int i = 0;
        int bufferIndex = 0;
        int offsetIndex = 0;
        for (ValuesDecoderInfo<BinaryValuesDecoder> valuesDecoderInfo : valuesDecoderInfos) {
            BinaryValuesDecoder binaryValuesDecoder = valuesDecoderInfo.getValuesDecoder();
            ReadChunk readChunk = readChunkList.get(i);
            bufferIndex = binaryValuesDecoder.readIntoBuffer(byteBuffer, bufferIndex, offsets, offsetIndex, readChunk);
            offsetIndex += valuesDecoderInfo.getValueCount();
            i++;
        }

        Slice buffer = Slices.wrappedBuffer(byteBuffer, 0, bufferSize);
        Block block = new VariableWidthBlock(nextBatchSize, buffer, offsets, Optional.of(isNull));
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
