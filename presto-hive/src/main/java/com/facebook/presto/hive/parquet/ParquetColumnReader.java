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
package com.facebook.presto.hive.parquet;

import parquet.bytes.BytesInput;
import parquet.bytes.BytesUtils;
import parquet.column.ColumnDescriptor;
import parquet.column.Dictionary;
import parquet.column.Encoding;
import parquet.column.page.DictionaryPage;
import parquet.column.page.DataPage;
import parquet.column.page.DataPageV1;
import parquet.column.page.DataPageV2;
import parquet.column.page.PageReader;
import parquet.column.values.ValuesReader;
import parquet.column.values.rle.RunLengthBitPackingHybridDecoder;
import parquet.column.ValuesType;
import parquet.io.ParquetDecodingException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class ParquetColumnReader
{
    private final ColumnDescriptor columnDescriptor;
    private final long totalValueCount;
    private final PageReader pageReader;
    private final Dictionary dictionary;

    private IntReader repetitionReader;
    private IntReader definitionReader;
    private ParquetValuesReader parquetValuesReader;
    private int repetitionLevel;
    private int definitionLevel;
    private int currentValueCount;
    private int pageValueCount;
    private boolean dataReady;
    private List<DataPage> pages;
    private List<ValuesReader> valuesReaders;

    public ParquetColumnReader(ColumnDescriptor columnDescriptor, PageReader pageReader)
    {
        this.columnDescriptor = checkNotNull(columnDescriptor, "columnDescriptor");
        this.pageReader = checkNotNull(pageReader, "pageReader");
        DictionaryPage dictionaryPage = pageReader.readDictionaryPage();

        if (dictionaryPage != null) {
            try {
                this.dictionary = dictionaryPage.getEncoding().initDictionary(columnDescriptor, dictionaryPage);
            }
            catch (IOException e) {
                throw new ParquetDecodingException("could not decode the dictionary for " + columnDescriptor, e);
            }
        }
        else {
            this.dictionary = null;
        }
        checkArgument(pageReader.getTotalValueCount() > 0, "page is empty");
        this.totalValueCount = pageReader.getTotalValueCount();
        loadPages();
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

    public void readVector(ColumnVector vector)
    {
        if (dataReady) {
            parquetValuesReader.readVector(vector);
            long valuesInThisBatch = 0;
            for (DataPage page : pages) {
                valuesInThisBatch += page.getValueCount();
            }

            if (vector.size() == valuesInThisBatch) {
                this.pages = null;
            }
            dataReady = false;
        }
    }

    public void loadPages()
    {
        if (this.pages == null) {
            if (currentValueCount >= totalValueCount) {
                repetitionLevel = 0;
                return;
            }
            readPages();
        }
        repetitionLevel = repetitionReader.readInt();
        definitionLevel = definitionReader.readInt();
        dataReady = true;
    }

    private void readPages()
    {
        pages = new ArrayList();
        valuesReaders = new ArrayList();
        parquetValuesReader = new ParquetValuesReader();
        while (currentValueCount < totalValueCount) {
            DataPage page = pageReader.readPage();
            if (page == null) {
                break;
            }
            if (page instanceof DataPageV1) {
                valuesReaders.add(readPageV1((DataPageV1) page));
            }
            else {
                valuesReaders.add(readPageV2((DataPageV2) page));
            }
            pages.add(page);
            currentValueCount += page.getValueCount();
        }
        parquetValuesReader.setPages(pages.toArray(new DataPage[pages.size()]));
        parquetValuesReader.setReaders(valuesReaders.toArray(new ValuesReader[valuesReaders.size()]));
    }

    private ValuesReader readPageV1(DataPageV1 page)
    {
        ValuesReader rlReader = page.getRlEncoding().getValuesReader(columnDescriptor, ValuesType.REPETITION_LEVEL);
        ValuesReader dlReader = page.getDlEncoding().getValuesReader(columnDescriptor, ValuesType.DEFINITION_LEVEL);
        repetitionReader = new ValuesIntReader(rlReader);
        definitionReader = new ValuesIntReader(dlReader);
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
        repetitionReader = buildRLEIntReader(columnDescriptor.getMaxRepetitionLevel(), page.getRepetitionLevels());
        definitionReader = buildRLEIntReader(columnDescriptor.getMaxDefinitionLevel(), page.getDefinitionLevels());
        try {
            return initDataReader(page.getDataEncoding(), page.getData().toByteArray(), 0, page.getValueCount());
        }
        catch (IOException e) {
            throw new ParquetDecodingException("could not read page " + page + " in col " + columnDescriptor, e);
        }
    }

    private IntReader buildRLEIntReader(int maxLevel, BytesInput bytes)
    {
        try {
            if (maxLevel == 0) {
                return new NullIntReader();
            }
            return new RLEIntReader(new RunLengthBitPackingHybridDecoder(BytesUtils.getWidthFromMaxInt(maxLevel),
                                                                        new ByteArrayInputStream(bytes.toByteArray())));
        }
        catch (IOException e) {
            throw new ParquetDecodingException("could not read levels in page for col " + columnDescriptor, e);
        }
    }

    private ValuesReader initDataReader(Encoding dataEncoding, byte[] bytes, int offset, int valueCount)
    {
        pageValueCount = valueCount;
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

    abstract static class IntReader
    {
        abstract int readInt();
    }

    private static class ValuesIntReader
        extends IntReader
    {
        ValuesReader delegate;

        public ValuesIntReader(ValuesReader delegate)
        {
            super();
            this.delegate = delegate;
        }

        @Override
        int readInt()
        {
            return delegate.readInteger();
        }
    }

    private static class RLEIntReader
        extends IntReader
    {
        RunLengthBitPackingHybridDecoder delegate;

        public RLEIntReader(RunLengthBitPackingHybridDecoder delegate)
        {
            this.delegate = delegate;
        }

        @Override
        int readInt()
        {
            try {
                return delegate.readInt();
            }
            catch (IOException e) {
            throw new ParquetDecodingException(e);
            }
        }
    }

    private static final class NullIntReader
        extends IntReader
    {
        @Override
        int readInt()
        {
            return 0;
        }
    }
}
