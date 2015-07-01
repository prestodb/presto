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
package com.facebook.presto.hive.parquet;

import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.parquet.ParquetCodecFactory.BytesDecompressor;
import com.facebook.presto.hive.parquet.TupleDomainParquetPredicate.ColumnReference;
import com.facebook.presto.spi.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import parquet.column.ColumnDescriptor;
import parquet.column.Encoding;
import parquet.column.page.DictionaryPage;
import parquet.column.statistics.Statistics;
import parquet.hadoop.metadata.BlockMetaData;
import parquet.hadoop.metadata.ColumnChunkMetaData;
import parquet.hadoop.metadata.ColumnPath;
import parquet.schema.MessageType;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class ParquetUtils
{
    // definition level, repetition level, value
    private static final int PARQUET_DATA_TRIPLE = 3;

    private ParquetUtils()
    {
    }

    public static parquet.schema.Type getParquetType(HiveColumnHandle column,
                                                    MessageType messageType,
                                                    boolean useParquetColumnNames)
    {
        if (useParquetColumnNames) {
            if (messageType.containsField(column.getName())) {
                return messageType.getType(column.getName());
            }
            return null;
        }

        if (column.getHiveColumnIndex() < messageType.getFieldCount()) {
            return messageType.getType(column.getHiveColumnIndex());
        }
        return null;
    }

    public static Map<Integer, Statistics> getStatisticsByColumnOrdinal(BlockMetaData blockMetadata)
    {
        ImmutableMap.Builder<Integer, Statistics> statistics = ImmutableMap.builder();
        for (int ordinal = 0; ordinal < blockMetadata.getColumns().size(); ordinal++) {
            Statistics columnStatistics = blockMetadata.getColumns().get(ordinal).getStatistics();
            if (columnStatistics != null) {
                statistics.put(ordinal, columnStatistics);
            }
        }
        return statistics.build();
    }

    public static Map<Integer, ParquetDictionaryDescriptor> getDictionariesByColumnOrdinal(BlockMetaData blockMetadata,
                                                                                            Path path,
                                                                                            Configuration configuration,
                                                                                            MessageType requestedSchema,
                                                                                            TupleDomain<HiveColumnHandle> effectivePredicate)
        throws IOException
    {
        ImmutableMap.Builder<Integer, ParquetDictionaryDescriptor> dictionaries = ImmutableMap.builder();
        for (int ordinal = 0; ordinal < blockMetadata.getColumns().size(); ordinal++) {
            ColumnChunkMetaData columnChunkMetaData = blockMetadata.getColumns().get(ordinal);

            for (int i = 0; i < requestedSchema.getColumns().size(); i++) {
                ColumnDescriptor columnDescriptor = requestedSchema.getColumns().get(i);
                if (isColumnPredicate(columnDescriptor, effectivePredicate) &&
                    columnChunkMetaData.getPath().equals(ColumnPath.get(columnDescriptor.getPath())) &&
                    isOnlyDictionaryEncodingPages(columnChunkMetaData.getEncodings())) {
                    long startingPosition = columnChunkMetaData.getStartingPos();
                    FSDataInputStream inputStream = path.getFileSystem(configuration).open(path);
                    inputStream.seek(startingPosition);

                    int totalSize = (int) columnChunkMetaData.getTotalSize();
                    byte[] buffer = new byte[totalSize];
                    inputStream.readFully(buffer);

                    ParquetDictionaryStream parquetDictionaryStream = new ParquetDictionaryStream(buffer, 0);
                    DictionaryPage compressedPage = parquetDictionaryStream.readDictionaryPage();
                    inputStream.close();

                    if (compressedPage == null) {
                        dictionaries.put(ordinal, new ParquetDictionaryDescriptor(columnDescriptor, null));
                        break;
                    }

                    ParquetCodecFactory codecFactory = new ParquetCodecFactory(configuration);
                    BytesDecompressor decompressor = codecFactory.getDecompressor(columnChunkMetaData.getCodec());
                    DictionaryPage dictionaryPage = new DictionaryPage(decompressor.decompress(compressedPage.getBytes(),
                                                                                                compressedPage.getUncompressedSize()),
                                                                        compressedPage.getDictionarySize(),
                                                                        compressedPage.getEncoding());
                    dictionaries.put(ordinal, new ParquetDictionaryDescriptor(columnDescriptor, dictionaryPage));
                    break;
                }
            }
        }
        return dictionaries.build();
    }

    public static ParquetPredicate buildParquetPredicate(List<HiveColumnHandle> columns,
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

    public static int readIntLittleEndian(InputStream in)
        throws IOException
    {
        int ch1 = in.read();
        int ch2 = in.read();
        int ch3 = in.read();
        int ch4 = in.read();
        if ((ch1 | ch2 | ch3 | ch4) < 0) {
            throw new EOFException();
        }
        return ((ch4 << 24) + (ch3 << 16) + (ch2 << 8) + (ch1 << 0));
    }

    private static boolean isColumnPredicate(ColumnDescriptor columnDescriptor,
                                            TupleDomain<HiveColumnHandle> effectivePredicate)
    {
        String[] columnPath = columnDescriptor.getPath();
        return effectivePredicate.getDomains().keySet().stream()
                .map(HiveColumnHandle::getName)
                .anyMatch(name -> columnPath[columnPath.length - 1].equals(name));
    }

    private static int lookupParquetColumn(HiveColumnHandle column, MessageType fileSchema)
    {
        // map column has more than one primitive columns in parquet file
        // the column ordinal number does not always equal to hive column index
        // need to do a look up in parquet fileschema columns
        int parquetFieldIndex = 0;
        for (; parquetFieldIndex < fileSchema.getColumns().size(); parquetFieldIndex++) {
            String[] path = fileSchema.getColumns().get(parquetFieldIndex).getPath();
            if (column.getName().equals(path[path.length - 1])) {
                break;
            }
        }
        return parquetFieldIndex;
    }

    private static boolean isOnlyDictionaryEncodingPages(Set<Encoding> encodings)
    {
        // more than 1 encodings for values
        if (encodings.size() > PARQUET_DATA_TRIPLE) {
            return false;
        }
        // definition level, repetition level never have dictionary encoding
        // TODO: add PageEncodingStats in ColumnChunkMetaData
        return encodings.stream().anyMatch(Encoding::usesDictionary);
    }
}
