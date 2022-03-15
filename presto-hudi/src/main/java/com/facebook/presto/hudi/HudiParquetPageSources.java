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

package com.facebook.presto.hudi;

import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.hive.FileFormatDataSourceStats;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveFileContext;
import com.facebook.presto.hive.filesystem.ExtendedFileSystem;
import com.facebook.presto.hive.parquet.ParquetPageSource;
import com.facebook.presto.memory.context.AggregatedMemoryContext;
import com.facebook.presto.parquet.Field;
import com.facebook.presto.parquet.ParquetDataSource;
import com.facebook.presto.parquet.RichColumnDescriptor;
import com.facebook.presto.parquet.cache.MetadataReader;
import com.facebook.presto.parquet.predicate.Predicate;
import com.facebook.presto.parquet.reader.ParquetReader;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.crypto.DecryptionPropertiesFactory;
import org.apache.parquet.crypto.FileDecryptionProperties;
import org.apache.parquet.crypto.InternalFileDecryptor;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.internal.filter2.columnindex.ColumnIndexStore;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

import static com.facebook.presto.hive.CacheQuota.NO_CACHE_CONSTRAINTS;
import static com.facebook.presto.hive.parquet.HdfsParquetDataSource.buildHdfsParquetDataSource;
import static com.facebook.presto.hudi.HudiErrorCode.HUDI_CANNOT_OPEN_SPLIT;
import static com.facebook.presto.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static com.facebook.presto.parquet.ParquetTypeUtils.getColumnIO;
import static com.facebook.presto.parquet.ParquetTypeUtils.getDescriptors;
import static com.facebook.presto.parquet.ParquetTypeUtils.getParquetTypeByName;
import static com.facebook.presto.parquet.cache.MetadataReader.findFirstNonHiddenColumnId;
import static com.facebook.presto.parquet.predicate.PredicateUtils.buildPredicate;
import static com.facebook.presto.parquet.predicate.PredicateUtils.predicateMatches;
import static com.facebook.presto.parquet.reader.ColumnIndexFilterUtils.getColumnIndexStore;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static org.apache.parquet.io.ColumnIOConverter.constructField;

class HudiParquetPageSources
{
    private HudiParquetPageSources() {}

    public static ConnectorPageSource createParquetPageSource(
            TypeManager typeManager,
            HdfsEnvironment hdfsEnvironment,
            String user,
            Configuration configuration,
            Path path,
            long start,
            long length,
            List<HudiColumnHandle> regularColumns,
            DataSize maxReadBlockSize,
            boolean batchReaderEnabled,
            boolean verificationEnabled,
            TupleDomain<HudiColumnHandle> effectivePredicate,
            FileFormatDataSourceStats fileFormatDataSourceStats,
            boolean columnIndexFilterEnabled)
    {
        AggregatedMemoryContext systemMemoryContext = newSimpleAggregatedMemoryContext();

        ParquetDataSource dataSource = null;
        try {
            ExtendedFileSystem filesystem = hdfsEnvironment.getFileSystem(user, path, configuration);
            FileStatus fileStatus = filesystem.getFileStatus(path);
            long fileSize = fileStatus.getLen();
            long modificationTime = fileStatus.getModificationTime();
            HiveFileContext hiveFileContext = new HiveFileContext(
                    true,
                    NO_CACHE_CONSTRAINTS,
                    Optional.empty(),
                    OptionalLong.of(fileSize),
                    OptionalLong.of(start),
                    OptionalLong.of(length),
                    modificationTime,
                    false);
            FSDataInputStream inputStream = filesystem.openFile(path, hiveFileContext);
            // Lambda expression below requires final variable, so we define a new variable parquetDataSource.
            final ParquetDataSource parquetDataSource = buildHdfsParquetDataSource(inputStream, path, fileFormatDataSourceStats);
            dataSource = parquetDataSource;
            DecryptionPropertiesFactory cryptoFactory = DecryptionPropertiesFactory.loadFactory(configuration);
            FileDecryptionProperties fileDecryptionProperties = (cryptoFactory == null) ?
                    null : cryptoFactory.getFileDecryptionProperties(configuration, path);
            Optional<InternalFileDecryptor> fileDecryptor = (fileDecryptionProperties == null) ?
                    Optional.empty() : Optional.of(new InternalFileDecryptor(fileDecryptionProperties));
            ParquetMetadata parquetMetadata = hdfsEnvironment.doAs(user, () -> MetadataReader.readFooter(parquetDataSource, fileSize, fileDecryptor).getParquetMetadata());
            FileMetaData fileMetaData = parquetMetadata.getFileMetaData();
            MessageType fileSchema = fileMetaData.getSchema();
            List<Type> parquetFields = regularColumns.stream()
                    .map(column -> getParquetTypeByName(column.getName(), fileSchema))
                    .collect(toList());

            MessageType requestedSchema = new MessageType(fileSchema.getName(), parquetFields.stream().filter(Objects::nonNull).collect(toImmutableList()));
            Map<List<String>, RichColumnDescriptor> descriptorsByPath = getDescriptors(fileSchema, requestedSchema);
            TupleDomain<ColumnDescriptor> parquetTupleDomain = getParquetTupleDomain(descriptorsByPath, effectivePredicate);
            Predicate parquetPredicate = buildPredicate(requestedSchema, parquetTupleDomain, descriptorsByPath);

            final ParquetDataSource finalDataSource = dataSource;
            List<BlockMetaData> blocks = new ArrayList<>();
            List<ColumnIndexStore> blockIndexStores = new ArrayList<>();
            for (BlockMetaData block : parquetMetadata.getBlocks()) {
                Optional<Integer> firstIndex = findFirstNonHiddenColumnId(block);
                if (firstIndex.isPresent()) {
                    long firstDataPage = block.getColumns().get(firstIndex.get()).getFirstDataPageOffset();
                    Optional<ColumnIndexStore> columnIndexStore = getColumnIndexStore(parquetPredicate, finalDataSource, block, descriptorsByPath, columnIndexFilterEnabled);
                    if ((firstDataPage >= start) && (firstDataPage < (start + length)) &&
                            predicateMatches(parquetPredicate, block, dataSource, descriptorsByPath, parquetTupleDomain, columnIndexStore, columnIndexFilterEnabled)) {
                        blocks.add(block);
                        blockIndexStores.add(columnIndexStore.orElse(null));
                    }
                }
            }

            MessageColumnIO messageColumnIO = getColumnIO(fileSchema, requestedSchema);
            ParquetReader parquetReader = new ParquetReader(
                    messageColumnIO,
                    blocks,
                    Optional.empty(),
                    dataSource,
                    systemMemoryContext,
                    maxReadBlockSize,
                    batchReaderEnabled,
                    verificationEnabled,
                    parquetPredicate,
                    blockIndexStores,
                    columnIndexFilterEnabled,
                    fileDecryptor);

            ImmutableList.Builder<String> namesBuilder = ImmutableList.builder();
            ImmutableList.Builder<com.facebook.presto.common.type.Type> prestoTypes = ImmutableList.builder();
            ImmutableList.Builder<Optional<Field>> internalFields = ImmutableList.builder();
            for (int columnIndex = 0; columnIndex < regularColumns.size(); columnIndex++) {
                HudiColumnHandle column = regularColumns.get(columnIndex);
                namesBuilder.add(column.getName());
                Type parquetField = parquetFields.get(columnIndex);

                com.facebook.presto.common.type.Type prestoType = column.getHiveType().getType(typeManager);

                prestoTypes.add(prestoType);

                if (parquetField == null) {
                    internalFields.add(Optional.empty());
                }
                else {
                    internalFields.add(constructField(prestoType, messageColumnIO.getChild(parquetField.getName())));
                }
            }

            return new ParquetPageSource(parquetReader, prestoTypes.build(), internalFields.build(), namesBuilder.build(), new RuntimeStats());
        }
        catch (Exception e) {
            try {
                if (dataSource != null) {
                    dataSource.close();
                }
            }
            catch (IOException ignored) {
            }
            if (e instanceof PrestoException) {
                throw (PrestoException) e;
            }
            String message = format("Error opening Hudi split %s (offset=%s, length=%s): %s", path, start, length, e.getMessage());
            throw new PrestoException(HUDI_CANNOT_OPEN_SPLIT, message, e);
        }
    }

    private static TupleDomain<ColumnDescriptor> getParquetTupleDomain(Map<List<String>, RichColumnDescriptor> descriptorsByPath, TupleDomain<HudiColumnHandle> effectivePredicate)
    {
        if (effectivePredicate.isNone()) {
            return TupleDomain.none();
        }

        ImmutableMap.Builder<ColumnDescriptor, Domain> predicate = ImmutableMap.builder();
        effectivePredicate.getDomains().get().forEach((columnHandle, domain) -> {
            String baseType = columnHandle.getHiveType().getTypeSignature().getBase();
            // skip looking up predicates for complex types as Parquet only stores stats for primitives
            if (!baseType.equals(StandardTypes.MAP) && !baseType.equals(StandardTypes.ARRAY) && !baseType.equals(StandardTypes.ROW)) {
                RichColumnDescriptor descriptor = descriptorsByPath.get(ImmutableList.of(columnHandle.getName()));
                if (descriptor != null) {
                    predicate.put(descriptor, domain);
                }
            }
        });
        return TupleDomain.withColumnDomains(predicate.build());
    }
}
