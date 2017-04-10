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
import parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import static com.facebook.presto.hive.parquet.ParquetCompressionUtils.decompress;
import static java.lang.Math.toIntExact;

class ParquetPageReader
{
    private final CompressionCodecName codec;
    private final long valueCount;
    private final List<ParquetDataPage> compressedPages;
    private final ParquetDictionaryPage compressedDictionaryPage;

    public ParquetPageReader(CompressionCodecName codec,
            List<ParquetDataPage> compressedPages,
            ParquetDictionaryPage compressedDictionaryPage)
    {
        this.codec = codec;
        this.compressedPages = new LinkedList<>(compressedPages);
        this.compressedDictionaryPage = compressedDictionaryPage;
        int count = 0;
        for (ParquetDataPage page : compressedPages) {
            count += page.getValueCount();
        }
        this.valueCount = count;
    }

    public long getTotalValueCount()
    {
        return valueCount;
    }

    public ParquetDataPage readPage()
    {
        if (compressedPages.isEmpty()) {
            return null;
        }
        ParquetDataPage compressedPage = compressedPages.remove(0);
        try {
            if (compressedPage instanceof ParquetDataPageV1) {
                ParquetDataPageV1 dataPageV1 = (ParquetDataPageV1) compressedPage;
                return new ParquetDataPageV1(
                        decompress(codec, dataPageV1.getSlice(), dataPageV1.getUncompressedSize()),
                        dataPageV1.getValueCount(),
                        dataPageV1.getUncompressedSize(),
                        dataPageV1.getStatistics(),
                        dataPageV1.getRepetitionLevelEncoding(),
                        dataPageV1.getDefinitionLevelEncoding(),
                        dataPageV1.getValueEncoding());
            }
            else {
                ParquetDataPageV2 dataPageV2 = (ParquetDataPageV2) compressedPage;
                if (!dataPageV2.isCompressed()) {
                    return dataPageV2;
                }
                int uncompressedSize = toIntExact(dataPageV2.getUncompressedSize()
                        - dataPageV2.getDefinitionLevels().length()
                        - dataPageV2.getRepetitionLevels().length());
                return new ParquetDataPageV2(
                        dataPageV2.getRowCount(),
                        dataPageV2.getNullCount(),
                        dataPageV2.getValueCount(),
                        dataPageV2.getRepetitionLevels(),
                        dataPageV2.getDefinitionLevels(),
                        dataPageV2.getDataEncoding(),
                        decompress(codec, dataPageV2.getSlice(), uncompressedSize),
                        dataPageV2.getUncompressedSize(),
                        dataPageV2.getStatistics(),
                        false);
            }
        }
        catch (IOException e) {
            throw new RuntimeException("Could not decompress page", e);
        }
    }

    public ParquetDictionaryPage readDictionaryPage()
    {
        if (compressedDictionaryPage == null) {
            return null;
        }
        try {
            return new ParquetDictionaryPage(
                    decompress(codec, compressedDictionaryPage.getSlice(), compressedDictionaryPage.getUncompressedSize()),
                    compressedDictionaryPage.getDictionarySize(),
                    compressedDictionaryPage.getEncoding());
        }
        catch (IOException e) {
            throw new RuntimeException("Error reading dictionary page", e);
        }
    }
}
