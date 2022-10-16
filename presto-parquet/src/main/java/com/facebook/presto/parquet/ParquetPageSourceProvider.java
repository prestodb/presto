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

package com.facebook.presto.parquet;

import com.facebook.presto.common.FileFormatDataSourceStats;
import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.filesystem.ExtendedFileSystem;
import com.facebook.presto.memory.context.AggregatedMemoryContext;
import com.facebook.presto.parquet.cache.MetadataReader;
import com.facebook.presto.parquet.predicate.Predicate;
import com.facebook.presto.parquet.reader.ParquetReader;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableFormatColumnHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.AccessControlException;
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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static com.facebook.presto.parquet.HdfsParquetDataSource.buildHdfsParquetDataSource;
import static com.facebook.presto.parquet.ParquetTypeUtils.getColumnIO;
import static com.facebook.presto.parquet.ParquetTypeUtils.getDescriptors;
import static com.facebook.presto.parquet.cache.MetadataReader.findFirstNonHiddenColumnId;
import static com.facebook.presto.parquet.predicate.PredicateUtils.buildPredicate;
import static com.facebook.presto.parquet.predicate.PredicateUtils.predicateMatches;
import static com.facebook.presto.parquet.reader.ColumnIndexFilterUtils.getColumnIndexStore;
import static com.facebook.presto.spi.StandardErrorCode.PERMISSION_DENIED;
import static com.google.common.base.Strings.nullToEmpty;
import static java.lang.String.format;
import static org.apache.parquet.crypto.DecryptionPropertiesFactory.loadFactory;

public class ParquetPageSourceProvider
{
    private ParquetPageSourceProvider()
    {
    }

    public static ConnectorPageSource createCommonParquetPageSource(
            HdfsEnvironment hdfsEnvironment,
            String user,
            Configuration configuration,
            Path path,
            long start,
            long length,
            List<? extends TableFormatColumnHandle> columns,
            SchemaTableName tableName,
            DataSize maxReadBlockSize,
            boolean batchReaderEnabled,
            boolean verificationEnabled,
            TypeManager typeManager,
            TupleDomain<? extends TableFormatColumnHandle> effectivePredicate,
            FileFormatDataSourceStats stats,
            boolean columnIndexFilterEnabled,
            PageSourceCommons pageSourceCommons,
            boolean useParquetColumnNames,
            RuntimeStats runtimeStats)
    {
        AggregatedMemoryContext systemMemoryContext = newSimpleAggregatedMemoryContext();

        ParquetDataSource dataSource = null;
        try {
            ExtendedFileSystem fileSystem = hdfsEnvironment.getFileSystem(user, path, configuration);
            FSDataInputStream inputStream = fileSystem.open(path);
            FileStatus fileStatus = fileSystem.getFileStatus(path);
            long fileSize = fileStatus.getLen();
            // Lambda expression below requires final variable, so we define a new variable parquetDataSource.
            final ParquetDataSource parquetDataSource = buildHdfsParquetDataSource(inputStream, path, stats);
            dataSource = parquetDataSource;
            Optional<InternalFileDecryptor> fileDecryptor = createDecryptor(configuration, path);
            ParquetMetadata parquetMetadata = hdfsEnvironment.doAs(user,
                    () -> MetadataReader.readFooter(parquetDataSource, fileSize, fileDecryptor).getParquetMetadata());

            FileMetaData fileMetaData = parquetMetadata.getFileMetaData();
            MessageType fileSchema = fileMetaData.getSchema();

            Optional<List<org.apache.parquet.schema.Type>> parquetFields = pageSourceCommons.getParquetFields(columns, typeManager, fileSchema, tableName, path);
            MessageType requestedSchema = pageSourceCommons.getRequestedSchema(columns, typeManager, fileSchema, tableName, path, parquetFields, useParquetColumnNames);

            ImmutableList.Builder<BlockMetaData> footerBlocks = ImmutableList.builder();
            for (BlockMetaData block : parquetMetadata.getBlocks()) {
                Optional<Integer> firstIndex = findFirstNonHiddenColumnId(block);
                if (firstIndex.isPresent()) {
                    long firstDataPage = block.getColumns().get(firstIndex.get()).getFirstDataPageOffset();
                    if (firstDataPage >= start && firstDataPage < start + length) {
                        footerBlocks.add(block);
                    }
                }
            }

            Map<List<String>, RichColumnDescriptor> descriptorsByPath = getDescriptors(fileSchema, requestedSchema);

            TupleDomain<ColumnDescriptor> parquetTupleDomain = pageSourceCommons.getParquetTupleDomain(descriptorsByPath, effectivePredicate);

            Predicate parquetPredicate = buildPredicate(requestedSchema, parquetTupleDomain, descriptorsByPath);
            final ParquetDataSource finalDataSource = dataSource;
            ImmutableList.Builder<BlockMetaData> blocks = ImmutableList.builder();
            List<ColumnIndexStore> blockIndexStores = new ArrayList<>();
            for (BlockMetaData block : footerBlocks.build()) {
                Optional<ColumnIndexStore> columnIndexStore = getColumnIndexStore(
                        parquetPredicate,
                        finalDataSource,
                        block,
                        descriptorsByPath,
                        columnIndexFilterEnabled);
                if (predicateMatches(
                        parquetPredicate,
                        block,
                        finalDataSource,
                        descriptorsByPath,
                        parquetTupleDomain,
                        columnIndexStore,
                        columnIndexFilterEnabled)) {
                    blocks.add(block);
                    blockIndexStores.add(columnIndexStore.orElse(null));
                }
            }
            MessageColumnIO messageColumnIO = getColumnIO(fileSchema, requestedSchema);
            ParquetReader parquetReader = new ParquetReader(
                    messageColumnIO,
                    blocks.build(),
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

            return pageSourceCommons.getPageSource(parquetReader, columns, messageColumnIO, fileSchema, tableName, path, typeManager, parquetFields, useParquetColumnNames, runtimeStats);
        }
        catch (Exception exception) {
            try {
                if (dataSource != null) {
                    dataSource.close();
                }
            }
            catch (IOException ignored) {
            }
            if (exception instanceof PrestoException) {
                throw (PrestoException) exception;
            }
            if (exception instanceof ParquetCorruptionException) {
                throw new PrestoException(ParquetErrorCode.PARQUET_BAD_DATA, exception);
            }
            if (exception instanceof AccessControlException) {
                throw new PrestoException(PERMISSION_DENIED, exception.getMessage(), exception);
            }
            if (nullToEmpty(exception.getMessage()).trim().equals("Filesystem closed") || exception instanceof FileNotFoundException) {
                throw new PrestoException(ParquetErrorCode.PARQUET_CANNOT_OPEN_SPLIT, exception);
            }
            String message = format("Error opening Hive split %s (offset=%s, length=%s): %s", path, start, length, exception.getMessage());
            if (exception.getClass().getSimpleName().equals("BlockMissingException")) {
                throw new PrestoException(ParquetErrorCode.PARQUET_MISSING_DATA, message, exception);
            }
            throw new PrestoException(ParquetErrorCode.PARQUET_CANNOT_OPEN_SPLIT, message, exception);
        }
    }

    public abstract static class PageSourceCommons
    {
        public abstract Optional<List<org.apache.parquet.schema.Type>> getParquetFields(
                List<? extends TableFormatColumnHandle> columns,
                TypeManager typeManager,
                MessageType fileSchema,
                SchemaTableName tableName,
                Path path);

        public abstract MessageType getRequestedSchema(
                List<? extends TableFormatColumnHandle> columns,
                TypeManager typeManager,
                MessageType fileSchema,
                SchemaTableName tableName,
                Path path,
                Optional<List<org.apache.parquet.schema.Type>> parquetFields,
                boolean useParquetColumnNames);

        public abstract ParquetPageSource getPageSource(
                ParquetReader parquetReader,
                List<? extends TableFormatColumnHandle> columns,
                MessageColumnIO messageColumnIO,
                MessageType fileSchema,
                SchemaTableName tableName,
                Path path,
                TypeManager typeManager,
                Optional<List<org.apache.parquet.schema.Type>> parquetFields,
                boolean useParquetColumnNames,
                RuntimeStats runtimeStats);

        public TupleDomain<ColumnDescriptor> getParquetTupleDomain(
                Map<List<String>, RichColumnDescriptor> descriptorsByPath,
                TupleDomain<? extends TableFormatColumnHandle> effectivePredicate)
        {
            if (effectivePredicate.isNone()) {
                return TupleDomain.none();
            }

            ImmutableMap.Builder<ColumnDescriptor, Domain> predicate = ImmutableMap.builder();
            effectivePredicate.getDomains().get().forEach((column, domain) -> {
                String baseType = column.getBaseType();
                // skip looking up predicates for complex types as Parquet only stores stats for primitives
                if (!baseType.equals(StandardTypes.MAP) && !baseType.equals(StandardTypes.ARRAY) && !baseType.equals(StandardTypes.ROW)) {
                    RichColumnDescriptor descriptor = descriptorsByPath.get(ImmutableList.of(column.getName()));
                    if (descriptor != null) {
                        predicate.put(descriptor, domain);
                    }
                }
            });
            return TupleDomain.withColumnDomains(predicate.build());
        }
    }

    public static Optional<InternalFileDecryptor> createDecryptor(Configuration configuration, Path path)
    {
        DecryptionPropertiesFactory cryptoFactory = loadFactory(configuration);
        FileDecryptionProperties fileDecryptionProperties = (cryptoFactory == null) ? null : cryptoFactory.getFileDecryptionProperties(configuration, path);
        return (fileDecryptionProperties == null) ? Optional.empty() : Optional.of(new InternalFileDecryptor(fileDecryptionProperties));
    }
}
