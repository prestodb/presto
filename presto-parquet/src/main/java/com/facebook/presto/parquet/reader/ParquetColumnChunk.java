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

import com.facebook.presto.memory.context.LocalMemoryContext;
import com.facebook.presto.parquet.DataPage;
import com.facebook.presto.parquet.DataPageV1;
import com.facebook.presto.parquet.DataPageV2;
import com.facebook.presto.parquet.DictionaryPage;
import com.facebook.presto.parquet.ParquetCorruptionException;
import com.facebook.presto.parquet.cache.MetadataReader;
import io.airlift.slice.Slice;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.EncodingStats;
import org.apache.parquet.crypto.AesCipher;
import org.apache.parquet.crypto.InternalColumnDecryptionSetup;
import org.apache.parquet.crypto.InternalFileDecryptor;
import org.apache.parquet.crypto.ModuleCipherFactory.ModuleType;
import org.apache.parquet.format.BlockCipher;
import org.apache.parquet.format.DataPageHeader;
import org.apache.parquet.format.DataPageHeaderV2;
import org.apache.parquet.format.DictionaryPageHeader;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.Util;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.openjdk.jol.info.ClassLayout;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.parquet.ParquetTypeUtils.getParquetEncoding;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.airlift.slice.Slices.wrappedBuffer;
import static java.util.Objects.requireNonNull;

public class ParquetColumnChunk
        implements Closeable
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(ParquetColumnChunk.class).instanceSize();

    private final ColumnChunkDescriptor descriptor;
    private final ColumnChunkBufferedInputStream stream;
    private final OffsetIndex offsetIndex;
    private final LocalMemoryContext memoryContext;

    public ParquetColumnChunk(
            ColumnChunkDescriptor descriptor,
            ColumnChunkBufferedInputStream stream,
            Optional<OffsetIndex> offsetIndex,
            LocalMemoryContext memoryContext)
    {
        this.descriptor = requireNonNull(descriptor);
        this.stream = stream;
        this.offsetIndex = requireNonNull(offsetIndex).orElse(null);
        this.memoryContext = requireNonNull(memoryContext, "ParquetColumnChunk memoryContext is null");
    }

    public ColumnChunkDescriptor getDescriptor()
    {
        return descriptor;
    }

    protected PageHeader readPageHeader(BlockCipher.Decryptor headerBlockDecryptor, byte[] pageHeaderAAD)
            throws IOException
    {
        return Util.readPageHeader(stream, headerBlockDecryptor, pageHeaderAAD);
    }

    public PageReader buildPageReader(
            Optional<InternalFileDecryptor> fileDecryptor,
            int rowGroupOrdinal,
            int columnOrdinal)
            throws IOException
    {
        byte[] dataPageHeaderAdditionalAuthenticationData = null;
        BlockCipher.Decryptor headerBlockDecryptor = null;
        InternalColumnDecryptionSetup columnDecryptionSetup = null;
        if (fileDecryptor.isPresent()) {
            ColumnPath columnPath = ColumnPath.get(descriptor.getColumnDescriptor().getPath());
            columnDecryptionSetup = fileDecryptor.get().getColumnSetup(columnPath);
            headerBlockDecryptor = columnDecryptionSetup.getMetaDataDecryptor();
            if (headerBlockDecryptor != null) {
                dataPageHeaderAdditionalAuthenticationData = AesCipher.createModuleAAD(fileDecryptor.get().getFileAAD(), ModuleType.DataPageHeader, rowGroupOrdinal, columnOrdinal, 0);
            }
        }

        //Read the dictionary page if it exists
        //The dictionary page **must be the very first page in the column chunk**
        //See https://github.com/apache/parquet-format/pull/177 for details
        DictionaryPage dictionaryPage = null;
        if (hasDictionaryPage(descriptor.getColumnChunkMetaData())) {
            byte[] pageHeaderAAD = headerBlockDecryptor != null ?
                    AesCipher.createModuleAAD(fileDecryptor.get().getFileAAD(), ModuleType.DictionaryPageHeader, rowGroupOrdinal, columnOrdinal, -1)
                    : null;
            PageHeader pageHeader = readPageHeader(headerBlockDecryptor, pageHeaderAAD);
            int uncompressedPageSize = pageHeader.getUncompressed_page_size();
            int compressedPageSize = pageHeader.getCompressed_page_size();
            dictionaryPage = readDictionaryPage(pageHeader, uncompressedPageSize, compressedPageSize);
        }

        final Iterator<DataPage> dataPageIterator = getDataPageIterator(dataPageHeaderAdditionalAuthenticationData, headerBlockDecryptor, dictionaryPage);

        byte[] fileAdditionalAuthenticationData = fileDecryptor.map(InternalFileDecryptor::getFileAAD).orElse(null);
        Optional<BlockCipher.Decryptor> dataDecryptor = getDataDecryptor(columnDecryptionSetup);

        long totalValueCount = descriptor.getColumnChunkMetaData().getValueCount();
        return new PageReader(descriptor.getColumnChunkMetaData().getCodec(), dataPageIterator, totalValueCount,
                dictionaryPage, offsetIndex, dataDecryptor, fileAdditionalAuthenticationData, rowGroupOrdinal, columnOrdinal);
    }

    @Override
    public void close()
            throws IOException
    {
        stream.close();
        memoryContext.close();
    }

    private Iterator<DataPage> getDataPageIterator(byte[] dataPageHeaderAdditionalAuthenticationData, BlockCipher.Decryptor headerBlockDecryptor, DictionaryPage dictionaryPage)
    {
        return new Iterator<DataPage>()
        {
            /**
             * Running count of values read from this column chunk
             * These include NULL values
             *
             * @see DataPageHeader#getNum_values()
             */
            private int valueCount;
            private int dataPageCount;
            private int pageOrdinal;

            @Override
            public boolean hasNext()
            {
                return hasMorePages(valueCount, dataPageCount);
            }

            @Override
            public DataPage next()
            {
                DataPage readPage = null;
                try {
                    byte[] pageHeaderAadditionalAuthenticationData = dataPageHeaderAdditionalAuthenticationData;
                    if (headerBlockDecryptor != null) {
                        // Important: this verifies file integrity (makes sure dictionary page had not been removed)
                        AesCipher.quickUpdatePageAAD(dataPageHeaderAdditionalAuthenticationData, pageOrdinal);
                    }
                    PageHeader pageHeader = readPageHeader(headerBlockDecryptor, pageHeaderAadditionalAuthenticationData);
                    int uncompressedPageSize = pageHeader.getUncompressed_page_size();
                    int compressedPageSize = pageHeader.getCompressed_page_size();
                    long firstRowIndex;
                    switch (pageHeader.type) {
                        case DICTIONARY_PAGE:
                            if (dictionaryPage != null) {
                                throw new ParquetCorruptionException("%s has more than one dictionary page in column chunk", descriptor.getColumnDescriptor());
                            }
                            break;
                        case DATA_PAGE:
                            firstRowIndex = PageReader.getFirstRowIndex(dataPageCount, offsetIndex);
                            readPage = readDataPageV1(pageHeader, uncompressedPageSize, compressedPageSize, firstRowIndex);
                            valueCount += pageHeader.getData_page_header().getNum_values();
                            dataPageCount = dataPageCount + 1;
                            pageOrdinal = pageOrdinal + 1;
                            break;
                        case DATA_PAGE_V2:
                            firstRowIndex = PageReader.getFirstRowIndex(dataPageCount, offsetIndex);
                            readPage = readDataPageV2(pageHeader, uncompressedPageSize, compressedPageSize, firstRowIndex);
                            valueCount += pageHeader.getData_page_header_v2().getNum_values();
                            dataPageCount = dataPageCount + 1;
                            pageOrdinal = pageOrdinal + 1;
                            break;
                        default:
                            stream.skip(compressedPageSize);
                            break;
                    }

                    memoryContext.setBytes(getRetainedSizeInBytes() +
                            // The pageHeaderAdditionalAuthenticationData is used to decrypt the stream and create a ByteArrayInputStream to read the PageHeader in the Apache
                            // Parquet library. The memory allocated in that library cannot be measured but pageHeaderAdditionalAuthenticationData can be. Although they are not
                            // retained by this class, it still makes sense to count them because they are actual being used in the PageDataPage's lifetime.
                            sizeOf(pageHeaderAadditionalAuthenticationData) +
                            // The memory to hold the ParquetDataPage data is allocated in getSlice() with size compressedPageSize. This slice will be read, uncompressed and
                            // decoded while the PageDataPage is alive, therefore it's better to also count it.
                            readPage.getRetainedSizeInBytes());
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }
                return readPage;
            }
        };
    }

    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + stream.getRetainedSizeInBytes();
    }

    private Optional<BlockCipher.Decryptor> getDataDecryptor(InternalColumnDecryptionSetup columnDecryptionSetup)
    {
        if (columnDecryptionSetup == null || columnDecryptionSetup.getDataDecryptor() == null) {
            return Optional.empty();
        }
        return Optional.of(columnDecryptionSetup.getDataDecryptor());
    }

    private Slice getSlice(int size)
            throws IOException
    {
        byte[] buffer = new byte[size];
        stream.read(buffer);
        return wrappedBuffer(buffer, 0, size);
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

    private DataPageV1 readDataPageV1(
            PageHeader pageHeader,
            int uncompressedPageSize,
            int compressedPageSize,
            long firstRowIndex)
            throws IOException
    {
        DataPageHeader dataHeaderV1 = pageHeader.getData_page_header();
        return new DataPageV1(
                getSlice(compressedPageSize),
                dataHeaderV1.getNum_values(),
                uncompressedPageSize,
                firstRowIndex,
                MetadataReader.readStats(
                        dataHeaderV1.getStatistics(),
                        descriptor.getColumnDescriptor().getType()),
                getParquetEncoding(Encoding.valueOf(dataHeaderV1.getRepetition_level_encoding().name())),
                getParquetEncoding(Encoding.valueOf(dataHeaderV1.getDefinition_level_encoding().name())),
                getParquetEncoding(Encoding.valueOf(dataHeaderV1.getEncoding().name())));
    }

    private DataPageV2 readDataPageV2(
            PageHeader pageHeader,
            int uncompressedPageSize,
            int compressedPageSize,
            long firstRowIndex)
            throws IOException
    {
        DataPageHeaderV2 dataHeaderV2 = pageHeader.getData_page_header_v2();
        int dataSize = compressedPageSize - dataHeaderV2.getRepetition_levels_byte_length() - dataHeaderV2.getDefinition_levels_byte_length();
        return new DataPageV2(
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
                dataHeaderV2.isIs_compressed());
    }

    private boolean hasMorePages(long valuesCount, int pagesCount)
    {
        return offsetIndex == null ? valuesCount < descriptor.getColumnChunkMetaData().getValueCount()
                : pagesCount < offsetIndex.getPageCount();
    }

    private boolean hasDictionaryPage(ColumnChunkMetaData columnChunkMetaData)
    {
        EncodingStats stats = columnChunkMetaData.getEncodingStats();
        if (stats != null) {
            return stats.hasDictionaryPages() && stats.hasDictionaryEncodedPages();
        }

        Set<Encoding> encodings = columnChunkMetaData.getEncodings();
        return encodings.contains(Encoding.PLAIN_DICTIONARY) || encodings.contains(Encoding.RLE_DICTIONARY);
    }

    static class ColumnChunkBufferedInputStream
            extends BufferedInputStream
    {
        private int bufferSize;

        ColumnChunkBufferedInputStream(InputStream inputStream, int bufferSize)
        {
            super(requireNonNull(inputStream), bufferSize);
            this.bufferSize = bufferSize;
        }

        long getRetainedSizeInBytes()
        {
            return bufferSize;
        }
    }
}
