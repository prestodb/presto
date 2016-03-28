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
package com.facebook.presto.hive.parquet.predicate;

import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.parquet.ParquetDataSource;
import com.facebook.presto.hive.parquet.ParquetDictionaryPage;
import com.facebook.presto.hive.parquet.ParquetEncoding;
import com.facebook.presto.hive.parquet.predicate.TupleDomainParquetPredicate.ColumnReference;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import io.airlift.slice.Slice;
import parquet.column.ColumnDescriptor;
import parquet.column.Encoding;
import parquet.column.statistics.Statistics;
import parquet.format.DictionaryPageHeader;
import parquet.format.PageHeader;
import parquet.format.PageType;
import parquet.format.Util;
import parquet.hadoop.metadata.BlockMetaData;
import parquet.hadoop.metadata.ColumnChunkMetaData;
import parquet.hadoop.metadata.ColumnPath;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.schema.MessageType;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.hive.parquet.ParquetCompressionUtils.decompress;
import static com.facebook.presto.hive.parquet.ParquetTypeUtils.getParquetEncoding;
import static io.airlift.slice.Slices.wrappedBuffer;
import static parquet.column.Encoding.BIT_PACKED;
import static parquet.column.Encoding.PLAIN_DICTIONARY;
import static parquet.column.Encoding.RLE;

public final class ParquetPredicateUtils
{
    private ParquetPredicateUtils()
    {
    }

    public static ParquetPredicate buildParquetPredicate(
            List<HiveColumnHandle> columns,
            TupleDomain<HiveColumnHandle> effectivePredicate,
            MessageType fileSchema,
            TypeManager typeManager)
    {
        ImmutableList.Builder<ColumnReference<HiveColumnHandle>> columnReferences = ImmutableList.builder();
        for (HiveColumnHandle column : columns) {
            if (!column.isPartitionKey()) {
                int parquetFieldIndex = lookupParquetColumn(column, fileSchema);
                Type type = typeManager.getType(column.getTypeSignature());
                columnReferences.add(new ColumnReference<>(column, parquetFieldIndex, type));
            }
        }

        return new TupleDomainParquetPredicate<>(effectivePredicate, columnReferences.build());
    }

    private static int lookupParquetColumn(HiveColumnHandle column, MessageType fileSchema)
    {
        // map column has more than one primitive columns in parquet file
        // the column ordinal number does not always equal to hive column index
        // need to do a look up in parquet file schema columns
        int parquetFieldIndex = 0;
        for (; parquetFieldIndex < fileSchema.getColumns().size(); parquetFieldIndex++) {
            String[] path = fileSchema.getColumns().get(parquetFieldIndex).getPath();
            String columnName = path[path.length - 1];
            if (column.getName().equals(columnName)) {
                break;
            }
        }
        return parquetFieldIndex;
    }

    public static boolean predicateMatches(ParquetPredicate parquetPredicate,
            BlockMetaData block,
            ParquetDataSource dataSource,
            MessageType requestedSchema,
            TupleDomain<HiveColumnHandle> effectivePredicate)
    {
        Map<Integer, Statistics<?>> columnStatistics = getStatisticsByColumnOrdinal(block);
        if (!parquetPredicate.matches(block.getRowCount(), columnStatistics)) {
            return false;
        }

        Map<Integer, ParquetDictionaryDescriptor> dictionaries = getDictionariesByColumnOrdinal(block, dataSource, requestedSchema, effectivePredicate);
        return parquetPredicate.matches(dictionaries);
    }

    private static Map<Integer, Statistics<?>> getStatisticsByColumnOrdinal(BlockMetaData blockMetadata)
    {
        ImmutableMap.Builder<Integer, Statistics<?>> statistics = ImmutableMap.builder();
        for (int ordinal = 0; ordinal < blockMetadata.getColumns().size(); ordinal++) {
            Statistics<?> columnStatistics = blockMetadata.getColumns().get(ordinal).getStatistics();
            if (columnStatistics != null) {
                statistics.put(ordinal, columnStatistics);
            }
        }
        return statistics.build();
    }

    private static Map<Integer, ParquetDictionaryDescriptor> getDictionariesByColumnOrdinal(
            BlockMetaData blockMetadata,
            ParquetDataSource dataSource,
            MessageType requestedSchema,
            TupleDomain<HiveColumnHandle> effectivePredicate)
    {
        ImmutableMap.Builder<Integer, ParquetDictionaryDescriptor> dictionaries = ImmutableMap.builder();
        for (int ordinal = 0; ordinal < blockMetadata.getColumns().size(); ordinal++) {
            ColumnChunkMetaData columnChunkMetaData = blockMetadata.getColumns().get(ordinal);

            for (int i = 0; i < requestedSchema.getColumns().size(); i++) {
                ColumnDescriptor columnDescriptor = requestedSchema.getColumns().get(i);
                if (isColumnPredicate(columnDescriptor, effectivePredicate) &&
                        columnChunkMetaData.getPath().equals(ColumnPath.get(columnDescriptor.getPath())) &&
                        isOnlyDictionaryEncodingPages(columnChunkMetaData.getEncodings())) {
                    try {
                        int totalSize = Ints.checkedCast(columnChunkMetaData.getTotalSize());
                        byte[] buffer = new byte[totalSize];
                        dataSource.readFully(columnChunkMetaData.getStartingPos(), buffer);
                        Optional<ParquetDictionaryPage> dictionaryPage = readDictionaryPage(buffer, columnChunkMetaData.getCodec());
                        dictionaries.put(ordinal, new ParquetDictionaryDescriptor(columnDescriptor, dictionaryPage));
                    }
                    catch (IOException ignored) {
                    }
                    break;
                }
            }
        }
        return dictionaries.build();
    }

    private static Optional<ParquetDictionaryPage> readDictionaryPage(byte[] data, CompressionCodecName codecName)
    {
        try {
            ByteArrayInputStream inputStream = new ByteArrayInputStream(data);
            PageHeader pageHeader = Util.readPageHeader(inputStream);

            if (pageHeader.type != PageType.DICTIONARY_PAGE) {
                return Optional.empty();
            }

            Slice compressedData = wrappedBuffer(data, data.length - inputStream.available(), pageHeader.getCompressed_page_size());
            DictionaryPageHeader dicHeader = pageHeader.getDictionary_page_header();
            ParquetEncoding encoding = getParquetEncoding(Encoding.valueOf(dicHeader.getEncoding().name()));
            int dictionarySize = dicHeader.getNum_values();

            return Optional.of(new ParquetDictionaryPage(decompress(codecName, compressedData, pageHeader.getUncompressed_page_size()), dictionarySize, encoding));
        }
        catch (IOException ignored) {
            return Optional.empty();
        }
    }

    private static boolean isColumnPredicate(ColumnDescriptor columnDescriptor, TupleDomain<HiveColumnHandle> effectivePredicate)
    {
        String[] columnPath = columnDescriptor.getPath();
        String columnName = columnPath[columnPath.length - 1];
        return effectivePredicate.getDomains().get().keySet().stream()
                .map(HiveColumnHandle::getName)
                .anyMatch(columnName::equals);
    }

    @VisibleForTesting
    @SuppressWarnings("deprecation")
    static boolean isOnlyDictionaryEncodingPages(Set<Encoding> encodings)
    {
        // TODO: update to use EncodingStats in ColumnChunkMetaData when available
        if (encodings.contains(PLAIN_DICTIONARY)) {
            // PLAIN_DICTIONARY was present, which means at least one page was
            // dictionary-encoded and 1.0 encodings are used
            // The only other allowed encodings are RLE and BIT_PACKED which are used for repetition or definition levels
            return Sets.difference(encodings, ImmutableSet.of(PLAIN_DICTIONARY, RLE, BIT_PACKED)).isEmpty();
        }

        // if PLAIN_DICTIONARY wasn't present, then either the column is not
        // dictionary-encoded, or the 2.0 encoding, RLE_DICTIONARY, was used.
        // for 2.0, this cannot determine whether a page fell back without
        // page encoding stats
        return false;
    }
}
