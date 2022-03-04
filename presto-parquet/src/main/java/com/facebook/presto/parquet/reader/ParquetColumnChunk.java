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
import io.airlift.slice.Slice;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.format.DataPageHeader;
import org.apache.parquet.format.DataPageHeaderV2;
import org.apache.parquet.format.DictionaryPageHeader;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.Util;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

import static com.facebook.presto.parquet.ParquetTypeUtils.getParquetEncoding;
import static io.airlift.slice.Slices.wrappedBuffer;

public class ParquetColumnChunk
{
    private final ColumnChunkDescriptor descriptor;
    private final ByteBufferInputStream stream;
    private final OffsetIndex offsetIndex;

    public ParquetColumnChunk(
            ColumnChunkDescriptor descriptor,
            byte[] data,
            int offset)
    {
        this.stream = ByteBufferInputStream.wrap(ByteBuffer.wrap(data, offset, data.length - offset));
        this.descriptor = descriptor;
        this.offsetIndex = null;
    }

    public ParquetColumnChunk(
            ColumnChunkDescriptor descriptor,
            List<ByteBuffer> data,
            OffsetIndex offsetIndex)
    {
        this.stream = ByteBufferInputStream.wrap(data);
        this.descriptor = descriptor;
        this.offsetIndex = offsetIndex;
    }

    public ColumnChunkDescriptor getDescriptor()
    {
        return descriptor;
    }

    protected PageHeader readPageHeader()
            throws IOException
    {
        return Util.readPageHeader(stream);
    }

    public PageReader readAllPages()
            throws IOException
    {
        LinkedList<DataPage> pages = new LinkedList<>();
        DictionaryPage dictionaryPage = null;
        long valueCount = 0;
        int dataPageCount = 0;
        while (hasMorePages(valueCount, dataPageCount)) {
            PageHeader pageHeader = readPageHeader();
            int uncompressedPageSize = pageHeader.getUncompressed_page_size();
            int compressedPageSize = pageHeader.getCompressed_page_size();
            long firstRowIndex = -1;
            switch (pageHeader.type) {
                case DICTIONARY_PAGE:
                    if (dictionaryPage != null) {
                        throw new ParquetCorruptionException("%s has more than one dictionary page in column chunk", descriptor.getColumnDescriptor());
                    }
                    dictionaryPage = readDictionaryPage(pageHeader, uncompressedPageSize, compressedPageSize);
                    break;
                case DATA_PAGE:
                    firstRowIndex = PageReader.getFirstRowIndex(dataPageCount, offsetIndex);
                    valueCount += readDataPageV1(pageHeader, uncompressedPageSize, compressedPageSize, firstRowIndex, pages);
                    dataPageCount = dataPageCount + 1;
                    break;
                case DATA_PAGE_V2:
                    firstRowIndex = PageReader.getFirstRowIndex(dataPageCount, offsetIndex);
                    valueCount += readDataPageV2(pageHeader, uncompressedPageSize, compressedPageSize, firstRowIndex, pages);
                    dataPageCount = dataPageCount + 1;
                    break;
                default:
                    stream.skipFully(compressedPageSize);
                    break;
            }
        }
        return new PageReader(descriptor.getColumnChunkMetaData().getCodec(), pages, dictionaryPage, offsetIndex);
    }

    private Slice getSlice(int size) throws IOException
    {
        //Todo: 1) The stream.slice() in both MultiBufferInputStream and SingleBufferInputStream will clone the memory.
        //         Need to check how much the memory consumption goes up. Since we skip reading pages, that would reduce
        //         a lot of memory consumption and compensate.
        //      2) It adds exception IOException. It is OK because eventually it rewinds to readAllPages() which
        //         already has IOException
        ByteBuffer buffer = stream.slice(size);
        return wrappedBuffer(buffer.array(), buffer.position(), size);
    }

    private DictionaryPage readDictionaryPage(PageHeader pageHeader, int uncompressedPageSize, int compressedPageSize)
            throws IOException
    {
        DictionaryPageHeader dictHeader = pageHeader.getDictionary_page_header();
        return new DictionaryPage(
                getSlice(compressedPageSize),
                uncompressedPageSize,
                dictHeader.getNum_values(),
                getParquetEncoding(Encoding.valueOf(dictHeader.getEncoding().name())));
    }

    private long readDataPageV1(PageHeader pageHeader,
            int uncompressedPageSize,
            int compressedPageSize,
            long firstRowIndex,
            List<DataPage> pages)
            throws IOException
    {
        DataPageHeader dataHeaderV1 = pageHeader.getData_page_header();
        pages.add(new DataPageV1(
                getSlice(compressedPageSize),
                dataHeaderV1.getNum_values(),
                uncompressedPageSize,
                firstRowIndex,
                MetadataReader.readStats(
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
            long firstRowIndex,
            List<DataPage> pages)
            throws IOException
    {
        DataPageHeaderV2 dataHeaderV2 = pageHeader.getData_page_header_v2();
        int dataSize = compressedPageSize - dataHeaderV2.getRepetition_levels_byte_length() - dataHeaderV2.getDefinition_levels_byte_length();
        pages.add(new DataPageV2(
                dataHeaderV2.getNum_rows(),
                dataHeaderV2.getNum_nulls(),
                dataHeaderV2.getNum_values(),
                firstRowIndex,
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

    private boolean hasMorePages(long valuesCount, int pagesCount)
    {
        return offsetIndex == null ? valuesCount < descriptor.getColumnChunkMetaData().getValueCount()
                : pagesCount < offsetIndex.getPageCount();
    }
}
