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
import com.facebook.presto.parquet.crypto.AesCipher;
import com.facebook.presto.parquet.crypto.ModuleCipherFactory;
import com.facebook.presto.parquet.format.BlockCipher;
import io.airlift.slice.Slice;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.util.LinkedList;

import static com.facebook.presto.parquet.ParquetCompressionUtils.decompress;
import static io.airlift.slice.Slices.wrappedBuffer;
import static java.lang.Math.toIntExact;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;

public class PageReader
{
    static final int INT_LENGTH = 4;

    private final long valueCount;
    private final LinkedList<DataPage> compressedPages;
    private final DictionaryPage compressedDictionaryPage;
    private final OffsetIndex offsetIndex;
    private int pageIndex;
    protected final CompressionCodecName codec;

    private final BlockCipher.Decryptor blockDecryptor;
    private final String[] columnPath;

    private short pageOrdinal; // TODO replace ordinal with pageIndex
    private byte[] dataPageAAD;
    private byte[] dictionaryPageAAD;

    public PageReader(CompressionCodecName codec,
                      List<DataPage> compressedPages,
                      DictionaryPage compressedDictionaryPage,
                      String[] columnPath,
                      BlockCipher.Decryptor blockDecryptor,
                      byte[] fileAAD,
                      short rowGroupOrdinal,
                      short columnOrdinal)
    {
        this(codec, compressedPages, compressedDictionaryPage, null, columnPath, blockDecryptor, fileAAD, rowGroupOrdinal, columnOrdinal);
    }

    /**
     * @param compressedPages This parameter will be mutated destructively as {@link DataPage} entries are removed as part of {@link #readPage()}. The caller
     * should not retain a reference to this list after passing it in as a constructor argument.
     */
    public PageReader(CompressionCodecName codec,
                      LinkedList<DataPage> compressedPages,
                      DictionaryPage compressedDictionaryPage,
                      String[] columnPath,
                      BlockCipher.Decryptor blockDecryptor,
                      byte[] fileAAD,
                      short rowGroupOrdinal,
                      short columnOrdinal)
    {
        this.codec = codec;
        this.compressedPages = compressedPages;
        this.compressedDictionaryPage = compressedDictionaryPage;
        int count = 0;
        for (DataPage page : compressedPages) {
            count += page.getValueCount();
        }
        this.valueCount = count;
        this.pageIndex = 0;
        this.blockDecryptor = blockDecryptor;
        this.columnPath = columnPath;
        this.pageOrdinal = 0;
        if (null != blockDecryptor) {
            dataPageAAD = AesCipher.createModuleAAD(fileAAD, ModuleCipherFactory.ModuleType.DataPage, rowGroupOrdinal, columnOrdinal, pageOrdinal);
            dictionaryPageAAD = AesCipher.createModuleAAD(fileAAD, ModuleCipherFactory.ModuleType.DictionaryPage, rowGroupOrdinal, columnOrdinal, (short) -1);
        }
    }

    public long getTotalValueCount()
    {
        return valueCount;
    }

    public DataPage readPage()
    {
        pageOrdinal++;

        if (compressedPages.isEmpty()) {
            return null;
        }
        if (null != blockDecryptor) {
            AesCipher.quickUpdatePageAad(dataPageAAD, pageOrdinal);
        }

        DataPage compressedPage = compressedPages.remove(0);
        try {
            Slice slice = decryptSlice(compressedPage.getSlice(), dataPageAAD);
            if (compressedPage instanceof DataPageV1) {
                DataPageV1 dataPageV1 = (DataPageV1) compressedPage;
                return new DataPageV1(
                        decompress(codec, slice, dataPageV1.getUncompressedSize()),
                        dataPageV1.getValueCount(),
                        dataPageV1.getUncompressedSize(),
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
                return new DataPageV2(
                        dataPageV2.getRowCount(),
                        dataPageV2.getNullCount(),
                        dataPageV2.getValueCount(),
                        dataPageV2.getRepetitionLevels(),
                        dataPageV2.getDefinitionLevels(),
                        dataPageV2.getDataEncoding(),
                        decompress(codec, slice, uncompressedSize),
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
            Slice slice = decryptSlice(compressedDictionaryPage.getSlice(), dictionaryPageAAD);
            return new DictionaryPage(
                    decompress(codec, slice, compressedDictionaryPage.getUncompressedSize()),
                    compressedDictionaryPage.getDictionarySize(),
                    compressedDictionaryPage.getEncoding());
        }
        catch (IOException e) {
            throw new RuntimeException("Error reading dictionary page", e);
        }
    }

    /**
     * Cast value to a an int, or throw an exception
     * if there is an overflow.
     *
     * @param value a long to be casted to an int
     * @return an int that is == to value
     * @throws IllegalArgumentException if value can't be casted to an int
     * @deprecated replaced by {@link java.lang.Math#toIntExact(long)}
     */
    public static int checkedCast(long value)
    {
        int valueI = (int) value;
        if (valueI != value) {
            throw new IllegalArgumentException(String.format("Overflow casting %d to an int", value));
        }
        return valueI;
    }

    private Slice decryptSlice(Slice slice, byte[] aad) throws IOException
    {
        if (null != blockDecryptor) {
            int offset = (int) slice.getAddress() - ARRAY_BYTE_BASE_OFFSET + INT_LENGTH;
            int length = slice.length() - INT_LENGTH;
            byte[] plainText = blockDecryptor.decrypt((byte[]) slice.getBase(), offset, length, aad);
            return wrappedBuffer(plainText);
        }
        else {
            return slice;
        }
    }
}
