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

import com.facebook.presto.hive.parquet.ParquetCodecFactory;
import com.facebook.presto.hive.parquet.ParquetCodecFactory.BytesDecompressor;
import com.facebook.presto.hive.parquet.ParquetCorruptionException;
import parquet.bytes.BytesInput;
import parquet.column.Encoding;
import parquet.column.page.DataPage;
import parquet.column.page.DataPageV1;
import parquet.column.page.DataPageV2;
import parquet.column.page.DictionaryPage;
import parquet.format.DataPageHeader;
import parquet.format.DataPageHeaderV2;
import parquet.format.DictionaryPageHeader;
import parquet.format.PageHeader;
import parquet.format.Util;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ParquetColumnChunk
        extends ByteArrayInputStream
{
    private final ParquetColumnChunkDescriptor descriptor;
    private final ParquetCodecFactory codecFactory;

    public ParquetColumnChunk(
            ParquetColumnChunkDescriptor descriptor,
            byte[] data,
            int offset,
            ParquetCodecFactory codecFactory)
    {
        super(data);
        this.descriptor = descriptor;
        this.pos = offset;
        this.codecFactory = codecFactory;
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

    public ParquetColumnChunkPageReader readAllPages()
            throws IOException
    {
        List<DataPage> pages = new ArrayList<>();
        DictionaryPage dictionaryPage = null;
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
        BytesDecompressor decompressor = codecFactory.getDecompressor(descriptor.getColumnChunkMetaData().getCodec());
        return new ParquetColumnChunkPageReader(decompressor, pages, dictionaryPage);
    }

    public int getPosition()
    {
        return pos;
    }

    public BytesInput getBytesInput(int size)
            throws IOException
    {
        BytesInput bytesInput = BytesInput.from(buf, pos, size);
        pos += size;
        return bytesInput;
    }

    private DictionaryPage readDictionaryPage(PageHeader pageHeader, int uncompressedPageSize, int compressedPageSize)
            throws IOException
    {
        DictionaryPageHeader dicHeader = pageHeader.getDictionary_page_header();
        return new DictionaryPage(
                getBytesInput(compressedPageSize),
                uncompressedPageSize,
                dicHeader.getNum_values(),
                Encoding.valueOf(dicHeader.getEncoding().name()));
    }

    private long readDataPageV1(PageHeader pageHeader,
            int uncompressedPageSize,
            int compressedPageSize,
            List<DataPage> pages)
            throws IOException
    {
        DataPageHeader dataHeaderV1 = pageHeader.getData_page_header();
        pages.add(new DataPageV1(
                getBytesInput(compressedPageSize),
                dataHeaderV1.getNum_values(),
                uncompressedPageSize,
                ParquetMetadataReader.readStats(
                        dataHeaderV1.getStatistics(),
                        descriptor.getColumnDescriptor().getType()),
                Encoding.valueOf(dataHeaderV1.getRepetition_level_encoding().name()),
                Encoding.valueOf(dataHeaderV1.getDefinition_level_encoding().name()),
                Encoding.valueOf(dataHeaderV1.getEncoding().name())));
        return dataHeaderV1.getNum_values();
    }

    private long readDataPageV2(PageHeader pageHeader,
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
                getBytesInput(dataHeaderV2.getRepetition_levels_byte_length()),
                getBytesInput(dataHeaderV2.getDefinition_levels_byte_length()),
                Encoding.valueOf(dataHeaderV2.getEncoding().name()),
                getBytesInput(dataSize),
                uncompressedPageSize,
                ParquetMetadataReader.readStats(
                        dataHeaderV2.getStatistics(),
                        descriptor.getColumnDescriptor().getType()),
                dataHeaderV2.isIs_compressed()));
        return dataHeaderV2.getNum_values();
    }
}
