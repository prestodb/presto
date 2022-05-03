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
import io.airlift.slice.Slice;
import org.apache.parquet.crypto.AesCipher;
import org.apache.parquet.crypto.ModuleCipherFactory;
import org.apache.parquet.format.BlockCipher;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Optional;

import static com.facebook.presto.parquet.ParquetCompressionUtils.decompress;
import static io.airlift.slice.Slices.wrappedBuffer;
import static java.lang.Math.toIntExact;

public class PageReader
{
    private final long valueCount;
    private final LinkedList<DataPage> compressedPages;
    private final DictionaryPage compressedDictionaryPage;
    private final OffsetIndex offsetIndex;
    private int pageIndex;
    private final Optional<BlockCipher.Decryptor> blockDecryptor;
    private byte[] dataPageAAD;
    private byte[] dictionaryPageAAD;

    protected final CompressionCodecName codec;

    /**
     * @param compressedPages This parameter will be mutated destructively as {@link DataPage} entries are removed as part of {@link #readPage()}. The caller
     * should not retain a reference to this list after passing it in as a constructor argument.
     */
    public PageReader(CompressionCodecName codec,
            LinkedList<DataPage> compressedPages,
            DictionaryPage compressedDictionaryPage)
            throws IOException
    {
        this(codec, compressedPages, compressedDictionaryPage, null, Optional.empty(), null, -1, -1);
    }

    public PageReader(CompressionCodecName codec,
                      LinkedList<DataPage> compressedPages,
                      DictionaryPage compressedDictionaryPage,
                      OffsetIndex offsetIndex,
                      Optional<BlockCipher.Decryptor> blockDecryptor,
                      byte[] fileAAD,
                      int rowGroupOrdinal,
                      int columnOrdinal)
    {
        this.codec = codec;
        this.compressedPages = compressedPages;
        this.compressedDictionaryPage = compressedDictionaryPage;
        int count = 0;
        for (DataPage page : compressedPages) {
            count += page.getValueCount();
        }
        this.valueCount = count;
        this.offsetIndex = offsetIndex;
        this.pageIndex = 0;
        this.blockDecryptor = blockDecryptor;
        if (blockDecryptor.isPresent()) {
            dataPageAAD = AesCipher.createModuleAAD(fileAAD, ModuleCipherFactory.ModuleType.DataPage, rowGroupOrdinal, columnOrdinal, 0);
            dictionaryPageAAD = AesCipher.createModuleAAD(fileAAD, ModuleCipherFactory.ModuleType.DictionaryPage, rowGroupOrdinal, columnOrdinal, -1);
        }
    }

    public long getTotalValueCount()
    {
        return valueCount;
    }

    public DataPage readPage()
    {
        if (compressedPages.isEmpty()) {
            return null;
        }
        if (blockDecryptor.isPresent()) {
            AesCipher.quickUpdatePageAAD(dataPageAAD, pageIndex);
        }
        DataPage compressedPage = compressedPages.remove(0);
        try {
            Slice slice = decryptSliceIfNeeded(compressedPage.getSlice(), dataPageAAD);
            long firstRowIndex = getFirstRowIndex(pageIndex, offsetIndex);
            pageIndex = pageIndex + 1;
            if (compressedPage instanceof DataPageV1) {
                DataPageV1 dataPageV1 = (DataPageV1) compressedPage;
                slice = decompress(codec, slice, dataPageV1.getUncompressedSize());
                return new DataPageV1(
                        slice,
                        dataPageV1.getValueCount(),
                        dataPageV1.getUncompressedSize(),
                        firstRowIndex,
                        dataPageV1.getStatistics(),
                        dataPageV1.getRepetitionLevelEncoding(),
                        dataPageV1.getDefinitionLevelEncoding(),
                        dataPageV1.getValueEncoding());
            }
            else {
                DataPageV2 dataPageV2 = (DataPageV2) compressedPage;
                if (!dataPageV2.isCompressed()) {
                    return dataPageV2;
                }
                int uncompressedSize = toIntExact(dataPageV2.getUncompressedSize()
                        - dataPageV2.getDefinitionLevels().length()
                        - dataPageV2.getRepetitionLevels().length());
                slice = decompress(codec, slice, uncompressedSize);
                return new DataPageV2(
                        dataPageV2.getRowCount(),
                        dataPageV2.getNullCount(),
                        dataPageV2.getValueCount(),
                        firstRowIndex,
                        dataPageV2.getRepetitionLevels(),
                        dataPageV2.getDefinitionLevels(),
                        dataPageV2.getDataEncoding(),
                        slice,
                        dataPageV2.getUncompressedSize(),
                        dataPageV2.getStatistics(),
                        false);
            }
        }
        catch (IOException e) {
            throw new RuntimeException("Could not decompress page", e);
        }
    }

    public DictionaryPage readDictionaryPage()
    {
        if (compressedDictionaryPage == null) {
            return null;
        }
        try {
            Slice slice = decryptSliceIfNeeded(compressedDictionaryPage.getSlice(), dictionaryPageAAD);
            return new DictionaryPage(
                    decompress(codec, slice, compressedDictionaryPage.getUncompressedSize()),
                    compressedDictionaryPage.getDictionarySize(),
                    compressedDictionaryPage.getEncoding());
        }
        catch (IOException e) {
            throw new RuntimeException("Error reading dictionary page", e);
        }
    }

    public static long getFirstRowIndex(int pageIndex, OffsetIndex offsetIndex)
    {
        return offsetIndex == null ? -1 : offsetIndex.getFirstRowIndex(pageIndex);
    }

    private Slice decryptSliceIfNeeded(Slice slice, byte[] aad) throws IOException
    {
        if (!blockDecryptor.isPresent()) {
            return slice;
        }
        byte[] plainText = blockDecryptor.get().decrypt(slice.getBytes(), aad);
        return wrappedBuffer(plainText);
    }
}
