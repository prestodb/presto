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
import com.facebook.presto.hive.parquet.RichColumnDescriptor;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
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
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.schema.MessageType;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.hive.parquet.ParquetCompressionUtils.decompress;
import static com.facebook.presto.hive.parquet.ParquetTypeUtils.getParquetEncoding;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.google.common.base.Verify.verify;
import static io.airlift.slice.Slices.wrappedBuffer;
import static java.lang.Math.toIntExact;
import static java.util.Map.Entry;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category.PRIMITIVE;
import static parquet.column.Encoding.BIT_PACKED;
import static parquet.column.Encoding.PLAIN_DICTIONARY;
import static parquet.column.Encoding.RLE;

public final class ParquetPredicateUtils
{
    private ParquetPredicateUtils()
    {
    }

    public static boolean isStatisticsOverflow(Type type, ParquetIntegerStatistics parquetIntegerStatistics)
    {
        long min = parquetIntegerStatistics.getMin();
        long max = parquetIntegerStatistics.getMax();
        return (type.equals(TINYINT) && (min < Byte.MIN_VALUE || max > Byte.MAX_VALUE)) ||
                (type.equals(SMALLINT) && (min < Short.MIN_VALUE || max > Short.MAX_VALUE)) ||
                (type.equals(INTEGER) && (min < Integer.MIN_VALUE || max > Integer.MAX_VALUE));
    }

    public static TupleDomain<ColumnDescriptor> getParquetTupleDomain(Map<List<String>, RichColumnDescriptor> descriptorsByPath, TupleDomain<HiveColumnHandle> effectivePredicate)
    {
        if (effectivePredicate.isNone()) {
            return TupleDomain.none();
        }

        ImmutableMap.Builder<ColumnDescriptor, Domain> predicate = ImmutableMap.builder();
        for (Entry<HiveColumnHandle, Domain> entry : effectivePredicate.getDomains().get().entrySet()) {
            HiveColumnHandle columnHandle = entry.getKey();
            // skip looking up predicates for complex types as Parquet only stores stats for primitives
            if (!columnHandle.getHiveType().getCategory().equals(PRIMITIVE)) {
                continue;
            }

            RichColumnDescriptor descriptor = descriptorsByPath.get(ImmutableList.of(columnHandle.getName()));
            if (descriptor != null) {
                predicate.put(descriptor, entry.getValue());
            }
        }
        return TupleDomain.withColumnDomains(predicate.build());
    }

    public static ParquetPredicate buildParquetPredicate(MessageType requestedSchema, TupleDomain<ColumnDescriptor> parquetTupleDomain, Map<List<String>, RichColumnDescriptor> descriptorsByPath)
    {
        ImmutableList.Builder<RichColumnDescriptor> columnReferences = ImmutableList.builder();
        for (String[] paths : requestedSchema.getPaths()) {
            RichColumnDescriptor descriptor = descriptorsByPath.get(Arrays.asList(paths));
            if (descriptor != null) {
                columnReferences.add(descriptor);
            }
        }
        return new TupleDomainParquetPredicate(parquetTupleDomain, columnReferences.build());
    }

    public static boolean predicateMatches(ParquetPredicate parquetPredicate, BlockMetaData block, ParquetDataSource dataSource, Map<List<String>, RichColumnDescriptor> descriptorsByPath, TupleDomain<ColumnDescriptor> parquetTupleDomain)
    {
        Map<ColumnDescriptor, Statistics<?>> columnStatistics = getStatistics(block, descriptorsByPath);
        if (!parquetPredicate.matches(block.getRowCount(), columnStatistics)) {
            return false;
        }

        Map<ColumnDescriptor, ParquetDictionaryDescriptor> dictionaries = getDictionaries(block, dataSource, descriptorsByPath, parquetTupleDomain);
        return parquetPredicate.matches(dictionaries);
    }

    private static Map<ColumnDescriptor, Statistics<?>> getStatistics(BlockMetaData blockMetadata, Map<List<String>, RichColumnDescriptor> descriptorsByPath)
    {
        ImmutableMap.Builder<ColumnDescriptor, Statistics<?>> statistics = ImmutableMap.builder();
        for (ColumnChunkMetaData columnMetaData : blockMetadata.getColumns()) {
            Statistics<?> columnStatistics = columnMetaData.getStatistics();
            if (columnStatistics != null) {
                RichColumnDescriptor descriptor = descriptorsByPath.get(Arrays.asList(columnMetaData.getPath().toArray()));
                if (descriptor != null) {
                    statistics.put(descriptor, columnStatistics);
                }
            }
        }
        return statistics.build();
    }

    private static Map<ColumnDescriptor, ParquetDictionaryDescriptor> getDictionaries(BlockMetaData blockMetadata, ParquetDataSource dataSource, Map<List<String>, RichColumnDescriptor> descriptorsByPath, TupleDomain<ColumnDescriptor> parquetTupleDomain)
    {
        ImmutableMap.Builder<ColumnDescriptor, ParquetDictionaryDescriptor> dictionaries = ImmutableMap.builder();
        for (ColumnChunkMetaData columnMetaData : blockMetadata.getColumns()) {
            RichColumnDescriptor descriptor = descriptorsByPath.get(Arrays.asList(columnMetaData.getPath().toArray()));
            if (descriptor != null) {
                if (isOnlyDictionaryEncodingPages(columnMetaData.getEncodings()) && isColumnPredicate(descriptor, parquetTupleDomain)) {
                    int totalSize = toIntExact(columnMetaData.getTotalSize());
                    byte[] buffer = new byte[totalSize];
                    dataSource.readFully(columnMetaData.getStartingPos(), buffer);
                    Optional<ParquetDictionaryPage> dictionaryPage = readDictionaryPage(buffer, columnMetaData.getCodec());
                    dictionaries.put(descriptor, new ParquetDictionaryDescriptor(descriptor, dictionaryPage));
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

    private static boolean isColumnPredicate(ColumnDescriptor columnDescriptor, TupleDomain<ColumnDescriptor> parquetTupleDomain)
    {
        verify(parquetTupleDomain.getDomains().isPresent(), "parquetTupleDomain is empty");
        return parquetTupleDomain.getDomains().get().keySet().contains(columnDescriptor);
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
