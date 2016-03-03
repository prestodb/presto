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

import com.facebook.presto.hive.parquet.ParquetCodecFactory.BytesDecompressor;
import com.google.common.primitives.Ints;
import parquet.column.page.DataPage;
import parquet.column.page.DataPageV1;
import parquet.column.page.DataPageV2;
import parquet.column.page.DictionaryPage;
import parquet.column.page.PageReader;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

class ParquetColumnChunkPageReader
        implements PageReader
{
    private final BytesDecompressor decompressor;
    private final long valueCount;
    private final List<DataPage> compressedPages;
    private final DictionaryPage compressedDictionaryPage;

    public ParquetColumnChunkPageReader(BytesDecompressor decompressor,
            List<DataPage> compressedPages,
            DictionaryPage compressedDictionaryPage)
    {
        this.decompressor = decompressor;
        this.compressedPages = new LinkedList<>(compressedPages);
        this.compressedDictionaryPage = compressedDictionaryPage;
        int count = 0;
        for (DataPage page : compressedPages) {
            count += page.getValueCount();
        }
        this.valueCount = count;
    }

    @Override
    public long getTotalValueCount()
    {
        return valueCount;
    }

    @Override
    public DataPage readPage()
    {
        if (compressedPages.isEmpty()) {
            return null;
        }
        DataPage compressedPage = compressedPages.remove(0);
        try {
            if (compressedPage instanceof DataPageV1) {
                DataPageV1 dataPageV1 = (DataPageV1) compressedPage;
                return new DataPageV1(
                        decompressor.decompress(dataPageV1.getBytes(), dataPageV1.getUncompressedSize()),
                        dataPageV1.getValueCount(),
                        dataPageV1.getUncompressedSize(),
                        dataPageV1.getStatistics(),
                        dataPageV1.getRlEncoding(),
                        dataPageV1.getDlEncoding(),
                        dataPageV1.getValueEncoding());
            }
            else {
                DataPageV2 dataPageV2 = (DataPageV2) compressedPage;
                if (!dataPageV2.isCompressed()) {
                    return dataPageV2;
                }
                int uncompressedSize = Ints.checkedCast(dataPageV2.getUncompressedSize() - dataPageV2.getDefinitionLevels().size() - dataPageV2.getRepetitionLevels().size());
                return DataPageV2.uncompressed(
                        dataPageV2.getRowCount(),
                        dataPageV2.getNullCount(),
                        dataPageV2.getValueCount(),
                        dataPageV2.getRepetitionLevels(),
                        dataPageV2.getDefinitionLevels(),
                        dataPageV2.getDataEncoding(),
                        decompressor.decompress(dataPageV2.getData(), uncompressedSize),
                        dataPageV2.getStatistics());
            }
        }
        catch (IOException e) {
            throw new RuntimeException("Could not decompress page", e);
        }
    }

    @Override
    public DictionaryPage readDictionaryPage()
    {
        if (compressedDictionaryPage == null) {
            return null;
        }
        try {
            return new DictionaryPage(
                    decompressor.decompress(compressedDictionaryPage.getBytes(), compressedDictionaryPage.getUncompressedSize()),
                    compressedDictionaryPage.getDictionarySize(),
                    compressedDictionaryPage.getEncoding());
        }
        catch (IOException e) {
            throw new RuntimeException("Error reading dictionary page", e);
        }
    }
}
