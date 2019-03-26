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
import com.facebook.presto.parquet.ParquetCorruptionException;
import com.facebook.presto.parquet.cache.MetadataReader;
import com.facebook.presto.parquet.crypto.AesCipher;
import com.facebook.presto.parquet.crypto.InternalColumnDecryptionSetup;
import com.facebook.presto.parquet.crypto.InternalFileDecryptor;
import com.facebook.presto.parquet.crypto.ModuleCipherFactory.ModuleType;
import com.facebook.presto.parquet.format.BlockCipher;
import com.facebook.presto.parquet.format.Util;
import io.airlift.slice.Slice;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.EncodingStats;
import org.apache.parquet.format.DataPageHeader;
import org.apache.parquet.format.DataPageHeaderV2;
import org.apache.parquet.format.DictionaryPageHeader;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.Util;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import static com.facebook.presto.parquet.ParquetTypeUtils.getParquetEncoding;
import static io.airlift.slice.Slices.wrappedBuffer;

public class ParquetColumnChunk
        extends ByteArrayInputStream
{
    private final ColumnChunkDescriptor descriptor;

    public ParquetColumnChunk(
            ColumnChunkDescriptor descriptor,
            byte[] data,
            int offset)
    {
        super(data);
        this.descriptor = descriptor;
        this.pos = offset;
    }

    public ColumnChunkDescriptor getDescriptor()
    {
        return descriptor;
    }

    protected PageHeader readPageHeader(BlockCipher.Decryptor headerBlockDecryptor, byte[] pageHeaderAAD)
            throws IOException
    {
        return Util.readPageHeader(this, headerBlockDecryptor, pageHeaderAAD);
    }

    public PageReader readAllPages(InternalFileDecryptor fileDecryptor, byte[] aadPrefix, int rowGroupOrdinal, int columnOrdinal)
            throws IOException
    {
        LinkedList<DataPage> pages = new LinkedList<>();
        DictionaryPage dictionaryPage = null;
        long valueCount = 0;
        int dataPageCount = 0;
        short pageOrdinal = 0;
        byte[] dataPageHeaderAAD = null;
        BlockCipher.Decryptor headerBlockDecryptor = null;
        InternalColumnDecryptionSetup columnDecryptionSetup = null;
        if (fileDecryptor != null) {
            ColumnPath columnPath = ColumnPath.get(descriptor.getColumnDescriptor().getPath());
            columnDecryptionSetup = fileDecryptor.getColumnSetup(columnPath);
            headerBlockDecryptor = columnDecryptionSetup.getMetaDataDecryptor();
            if (null != headerBlockDecryptor) {
                dataPageHeaderAAD = AesCipher.createModuleAAD(fileDecryptor.getFileAAD(), ModuleType.DataPageHeader, rowGroupOrdinal, columnOrdinal, pageOrdinal);
            }
        }
        while (hasMorePages(valueCount, dataPageCount)) {
            byte[] pageHeaderAAD = dataPageHeaderAAD;
            if (null != headerBlockDecryptor) {
                // Important: this verifies file integrity (makes sure dictionary page had not been removed)
                if (null == dictionaryPage && hasDictionaryPage(descriptor.getColumnChunkMetaData())) {
                    pageHeaderAAD = AesCipher.createModuleAAD(fileDecryptor.getFileAAD(), ModuleType.DictionaryPageHeader, rowGroupOrdinal, columnOrdinal, (short) -1);
                }
                else {
                    AesCipher.quickUpdatePageAad(dataPageHeaderAAD, pageOrdinal);
                }
            }

            PageHeader pageHeader = readPageHeader(headerBlockDecryptor, pageHeaderAAD);
            int uncompressedPageSize = pageHeader.getUncompressed_page_size();
            int compressedPageSize = pageHeader.getCompressed_page_size();
            switch (pageHeader.type) {
                case DICTIONARY_PAGE:
                    if (dictionaryPage != null) {
                        throw new ParquetCorruptionException("%s has more than one dictionary page in column chunk", descriptor.getColumnDescriptor());
                    }
                    dictionaryPage = readDictionaryPage(pageHeader, uncompressedPageSize, compressedPageSize);
                    break;
                case DATA_PAGE:
                    valueCount += readDataPageV1(pageHeader, uncompressedPageSize, compressedPageSize, pages);
                    break;
                case DATA_PAGE_V2:
                    valueCount += readDataPageV2(pageHeader, uncompressedPageSize, compressedPageSize, pages);
                    break;
                default:
                    skip(compressedPageSize);
                    break;
            }
        }
        return new PageReader(descriptor.getColumnChunkMetaData().getCodec(), pages, dictionaryPage, valueCount);
    }

    public int getPosition()
    {
        byte[] fileAad = (fileDecryptor == null) ? null : fileDecryptor.getFileAAD();
        BlockCipher.Decryptor dataDecryptor = (columnDecryptionSetup == null) ? null : columnDecryptionSetup.getDataDecryptor();
        return new PageReader(descriptor.getColumnChunkMetaData().getCodec(), pages, dictionaryPage, offsetIndex, descriptor.getColumnDescriptor().getPath(),
                dataDecryptor, fileAad, (short) rowGroupOrdinal, (short) columnOrdinal);
    }

    public boolean hasDictionaryPage(ColumnChunkMetaData columnChunkMetaData)
    {
        EncodingStats stats = columnChunkMetaData.getEncodingStats();
        if (stats != null) {
            return stats.hasDictionaryPages() && stats.hasDictionaryEncodedPages();
        }
        else {
            Set<Encoding> encodings = columnChunkMetaData.getEncodings();
            return encodings.contains(Encoding.PLAIN_DICTIONARY) || encodings.contains(Encoding.RLE_DICTIONARY);
        }
    }

    private Slice getSlice(int size)
    {
        Slice slice = wrappedBuffer(buf, pos, size);
        pos += size;
        return slice;
    }

    protected DictionaryPage readDictionaryPage(PageHeader pageHeader, int uncompressedPageSize, int compressedPageSize)
            throws IOException
    {
        DictionaryPageHeader dicHeader = pageHeader.getDictionary_page_header();
        return new DictionaryPage(
                getSlice(compressedPageSize),
                uncompressedPageSize,
                dicHeader.getNum_values(),
                getParquetEncoding(Encoding.valueOf(dicHeader.getEncoding().name())));
    }

    protected long readDataPageV1(PageHeader pageHeader,
                                int uncompressedPageSize,
                                int compressedPageSize,
                                List<DataPage> pages)
            throws IOException
    {
        DataPageHeader dataHeaderV1 = pageHeader.getData_page_header();
        pages.add(new DataPageV1(
                getSlice(compressedPageSize),
                dataHeaderV1.getNum_values(),
                uncompressedPageSize,
                MetadataReader.readStats(
                        dataHeaderV1.getStatistics(),
                        descriptor.getColumnDescriptor().getType()),
                getParquetEncoding(Encoding.valueOf(dataHeaderV1.getRepetition_level_encoding().name())),
                getParquetEncoding(Encoding.valueOf(dataHeaderV1.getDefinition_level_encoding().name())),
                getParquetEncoding(Encoding.valueOf(dataHeaderV1.getEncoding().name()))));
        return dataHeaderV1.getNum_values();
    }

    protected long readDataPageV2(PageHeader pageHeader,
                                int uncompressedPageSize,
                                int compressedPageSize,
                                List<DataPage> pages)
            throws IOException
    {
        DataPageHeaderV2 dataHeaderV2 = pageHeader.getData_page_header_v2();
        int dataSize = compressedPageSize - dataHeaderV2.getRepetition_levels_byte_length() - dataHeaderV2.getDefinition_levels_byte_length();
        pages.add(new DataPageV2(
                dataHeaderV2.getNum_rows(),
                dataHeaderV2.getNum_nulls(),
                dataHeaderV2.getNum_values(),
                getSlice(dataHeaderV2.getRepetition_levels_byte_length()),
                getSlice(dataHeaderV2.getDefinition_levels_byte_length()),
                getParquetEncoding(Encoding.valueOf(dataHeaderV2.getEncoding().name())),
                getSlice(dataSize),
                uncompressedPageSize,
                MetadataReader.readStats(
                        dataHeaderV2.getStatistics(),
                        descriptor.getColumnDescriptor().getType()),
                dataHeaderV2.isIs_compressed()));
        return dataHeaderV2.getNum_values();
    }
}
