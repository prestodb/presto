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

import com.facebook.presto.hive.parquet.ParquetCorruptionException;
import com.facebook.presto.hive.parquet.ParquetDataPage;
import com.facebook.presto.hive.parquet.ParquetDataPageV1;
import com.facebook.presto.hive.parquet.ParquetDataPageV2;
import com.facebook.presto.hive.parquet.ParquetDictionaryPage;
import io.airlift.slice.Slice;
import parquet.column.Encoding;
import parquet.format.DataPageHeader;
import parquet.format.DataPageHeaderV2;
import parquet.format.DictionaryPageHeader;
import parquet.format.PageHeader;
import parquet.format.Util;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.hive.parquet.ParquetTypeUtils.getParquetEncoding;
import static io.airlift.slice.Slices.wrappedBuffer;

public class ParquetColumnChunk
        extends ByteArrayInputStream
{
    private final ParquetColumnChunkDescriptor descriptor;

    public ParquetColumnChunk(
            ParquetColumnChunkDescriptor descriptor,
            byte[] data,
            int offset)
    {
        super(data);
        this.descriptor = descriptor;
        this.pos = offset;
    }

    public ParquetColumnChunkDescriptor getDescriptor()
    {
        return descriptor;
    }

    protected PageHeader readPageHeader()
            throws IOException
    {
        return Util.readPageHeader(this);
    }

    public ParquetPageReader readAllPages()
            throws IOException
    {
        List<ParquetDataPage> pages = new ArrayList<>();
        ParquetDictionaryPage dictionaryPage = null;
        long valueCount = 0;
        while (valueCount < descriptor.getColumnChunkMetaData().getValueCount()) {
            PageHeader pageHeader = readPageHeader();
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
        return new ParquetPageReader(descriptor.getColumnChunkMetaData().getCodec(), pages, dictionaryPage);
    }

    public int getPosition()
    {
        return pos;
    }

    private Slice getSlice(int size)
    {
        Slice slice = wrappedBuffer(buf, pos, size);
        pos += size;
        return slice;
    }

    private ParquetDictionaryPage readDictionaryPage(PageHeader pageHeader, int uncompressedPageSize, int compressedPageSize)
            throws IOException
    {
        DictionaryPageHeader dicHeader = pageHeader.getDictionary_page_header();
        return new ParquetDictionaryPage(
                getSlice(compressedPageSize),
                uncompressedPageSize,
                dicHeader.getNum_values(),
                getParquetEncoding(Encoding.valueOf(dicHeader.getEncoding().name())));
    }

    private long readDataPageV1(PageHeader pageHeader,
            int uncompressedPageSize,
            int compressedPageSize,
            List<ParquetDataPage> pages)
            throws IOException
    {
        DataPageHeader dataHeaderV1 = pageHeader.getData_page_header();
        pages.add(new ParquetDataPageV1(
                getSlice(compressedPageSize),
                dataHeaderV1.getNum_values(),
                uncompressedPageSize,
                ParquetMetadataReader.readStats(
                        dataHeaderV1.getStatistics(),
                        descriptor.getColumnDescriptor().getType()),
                getParquetEncoding(Encoding.valueOf(dataHeaderV1.getRepetition_level_encoding().name())),
                getParquetEncoding(Encoding.valueOf(dataHeaderV1.getDefinition_level_encoding().name())),
                getParquetEncoding(Encoding.valueOf(dataHeaderV1.getEncoding().name()))));
        return dataHeaderV1.getNum_values();
    }

    private long readDataPageV2(PageHeader pageHeader,
            int uncompressedPageSize,
            int compressedPageSize,
            List<ParquetDataPage> pages)
            throws IOException
    {
        DataPageHeaderV2 dataHeaderV2 = pageHeader.getData_page_header_v2();
        int dataSize = compressedPageSize - dataHeaderV2.getRepetition_levels_byte_length() - dataHeaderV2.getDefinition_levels_byte_length();
        pages.add(new ParquetDataPageV2(
                dataHeaderV2.getNum_rows(),
                dataHeaderV2.getNum_nulls(),
                dataHeaderV2.getNum_values(),
                getSlice(dataHeaderV2.getRepetition_levels_byte_length()),
                getSlice(dataHeaderV2.getDefinition_levels_byte_length()),
                getParquetEncoding(Encoding.valueOf(dataHeaderV2.getEncoding().name())),
                getSlice(dataSize),
                uncompressedPageSize,
                ParquetMetadataReader.readStats(
                        dataHeaderV2.getStatistics(),
                        descriptor.getColumnDescriptor().getType()),
                dataHeaderV2.isIs_compressed()));
        return dataHeaderV2.getNum_values();
    }
}
