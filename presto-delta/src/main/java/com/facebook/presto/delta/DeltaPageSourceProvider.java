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

package com.facebook.presto.delta;

import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.common.Subfield;
import com.facebook.presto.common.Utils;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.hive.FileFormatDataSourceStats;
import com.facebook.presto.hive.HdfsContext;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.parquet.ParquetPageSource;
import com.facebook.presto.memory.context.AggregatedMemoryContext;
import com.facebook.presto.parquet.Field;
import com.facebook.presto.parquet.ParquetCorruptionException;
import com.facebook.presto.parquet.ParquetDataSource;
import com.facebook.presto.parquet.RichColumnDescriptor;
import com.facebook.presto.parquet.cache.MetadataReader;
import com.facebook.presto.parquet.predicate.Predicate;
import com.facebook.presto.parquet.reader.ColumnIndexFilterUtils;
import com.facebook.presto.parquet.reader.ParquetReader;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SplitContext;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
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
import org.apache.parquet.io.ColumnIO;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;

import javax.inject.Inject;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.delta.DeltaColumnHandle.ColumnType.PARTITION;
import static com.facebook.presto.delta.DeltaColumnHandle.ColumnType.REGULAR;
import static com.facebook.presto.delta.DeltaColumnHandle.ColumnType.SUBFIELD;
import static com.facebook.presto.delta.DeltaColumnHandle.getPushedDownSubfield;
import static com.facebook.presto.delta.DeltaColumnHandle.isPushedDownSubfield;
import static com.facebook.presto.delta.DeltaErrorCode.DELTA_BAD_DATA;
import static com.facebook.presto.delta.DeltaErrorCode.DELTA_CANNOT_OPEN_SPLIT;
import static com.facebook.presto.delta.DeltaErrorCode.DELTA_MISSING_DATA;
import static com.facebook.presto.delta.DeltaErrorCode.DELTA_PARQUET_SCHEMA_MISMATCH;
import static com.facebook.presto.delta.DeltaSessionProperties.getParquetMaxReadBlockSize;
import static com.facebook.presto.delta.DeltaSessionProperties.isParquetBatchReaderVerificationEnabled;
import static com.facebook.presto.delta.DeltaSessionProperties.isParquetBatchReadsEnabled;
import static com.facebook.presto.delta.DeltaTypeUtils.convertPartitionValue;
import static com.facebook.presto.hive.parquet.HdfsParquetDataSource.buildHdfsParquetDataSource;
import static com.facebook.presto.hive.parquet.ParquetPageSourceFactory.checkSchemaMatch;
import static com.facebook.presto.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static com.facebook.presto.parquet.ParquetTypeUtils.columnPathFromSubfield;
import static com.facebook.presto.parquet.ParquetTypeUtils.getColumnIO;
import static com.facebook.presto.parquet.ParquetTypeUtils.getDescriptors;
import static com.facebook.presto.parquet.ParquetTypeUtils.getParquetTypeByName;
import static com.facebook.presto.parquet.ParquetTypeUtils.getSubfieldType;
import static com.facebook.presto.parquet.ParquetTypeUtils.lookupColumnByName;
import static com.facebook.presto.parquet.ParquetTypeUtils.nestedColumnPath;
import static com.facebook.presto.parquet.predicate.PredicateUtils.buildPredicate;
import static com.facebook.presto.parquet.predicate.PredicateUtils.predicateMatches;
import static com.facebook.presto.spi.StandardErrorCode.PERMISSION_DENIED;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.nullToEmpty;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;
import static org.apache.parquet.io.ColumnIOConverter.constructField;
import static org.apache.parquet.io.ColumnIOConverter.findNestedColumnIO;

public class DeltaPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final HdfsEnvironment hdfsEnvironment;
    private final TypeManager typeManager;
    private final FileFormatDataSourceStats fileFormatDataSourceStats;

    @Inject
    public DeltaPageSourceProvider(
            HdfsEnvironment hdfsEnvironment,
            TypeManager typeManager,
            FileFormatDataSourceStats fileFormatDataSourceStats)
    {
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.fileFormatDataSourceStats = requireNonNull(fileFormatDataSourceStats, "fileFormatDataSourceStats is null");
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableLayoutHandle layout,
            List<ColumnHandle> columns,
            SplitContext splitContext)
    {
        DeltaSplit deltaSplit = (DeltaSplit) split;
        DeltaTableLayoutHandle deltaTableLayoutHandle = (DeltaTableLayoutHandle) layout;
        DeltaTableHandle deltaTableHandle = deltaTableLayoutHandle.getTable();

        HdfsContext hdfsContext = new HdfsContext(
                session,
                deltaSplit.getSchema(),
                deltaSplit.getTable(),
                deltaSplit.getFilePath(),
                false);
        Path filePath = new Path(deltaSplit.getFilePath());
        List<DeltaColumnHandle> deltaColumnHandles = columns.stream()
                .map(DeltaColumnHandle.class::cast)
                .collect(Collectors.toList());

        List<DeltaColumnHandle> regularColumnHandles = deltaColumnHandles.stream()
                .filter(columnHandle -> columnHandle.getColumnType() != PARTITION)
                .collect(Collectors.toList());

        ConnectorPageSource dataPageSource = createParquetPageSource(
                hdfsEnvironment,
                session.getUser(),
                hdfsEnvironment.getConfiguration(hdfsContext, filePath),
                filePath,
                deltaSplit.getStart(),
                deltaSplit.getLength(),
                deltaSplit.getFileSize(),
                regularColumnHandles,
                deltaTableHandle.toSchemaTableName(),
                getParquetMaxReadBlockSize(session),
                isParquetBatchReadsEnabled(session),
                isParquetBatchReaderVerificationEnabled(session),
                typeManager,
                deltaTableLayoutHandle.getPredicate(),
                fileFormatDataSourceStats,
                false);

        return new DeltaPageSource(
                deltaColumnHandles,
                convertPartitionValues(deltaColumnHandles, deltaSplit.getPartitionValues()),
                dataPageSource);
    }

    /**
     * Go through all the output columns, identify the partition columns and convert the partition values to Presto internal format.
     */
    private Map<String, Block> convertPartitionValues(List<DeltaColumnHandle> allColumns, Map<String, String> partitionValues)
    {
        return allColumns.stream()
                .filter(columnHandle -> columnHandle.getColumnType() == PARTITION)
                .collect(toMap(
                        DeltaColumnHandle::getName,
                        columnHandle -> {
                            Type columnType = typeManager.getType(columnHandle.getDataType());
                            return Utils.nativeValueToBlock(
                                    columnType,
                                    convertPartitionValue(
                                            columnHandle.getName(),
                                            partitionValues.get(columnHandle.getName()),
                                            columnType));
                        }));
    }

    private static ConnectorPageSource createParquetPageSource(
            HdfsEnvironment hdfsEnvironment,
            String user,
            Configuration configuration,
            Path path,
            long start,
            long length,
            long fileSize,
            List<DeltaColumnHandle> columns,
            SchemaTableName tableName,
            DataSize maxReadBlockSize,
            boolean batchReaderEnabled,
            boolean verificationEnabled,
            TypeManager typeManager,
            TupleDomain<DeltaColumnHandle> effectivePredicate,
            FileFormatDataSourceStats stats,
            boolean columnIndexFilterEnabled)
    {
        AggregatedMemoryContext systemMemoryContext = newSimpleAggregatedMemoryContext();

        ParquetDataSource dataSource = null;
        try {
            FSDataInputStream inputStream = hdfsEnvironment.getFileSystem(user, path, configuration).open(path);
            // Lambda expression below requires final variable
            final ParquetDataSource parquetDataSource = buildHdfsParquetDataSource(inputStream, path, stats);
            dataSource = parquetDataSource;
            DecryptionPropertiesFactory cryptoFactory = DecryptionPropertiesFactory.loadFactory(configuration);
            FileDecryptionProperties fileDecryptionProperties = (cryptoFactory == null) ?
                    null : cryptoFactory.getFileDecryptionProperties(configuration, path);
            InternalFileDecryptor fileDecryptor = (fileDecryptionProperties == null) ?
                    null : new InternalFileDecryptor(fileDecryptionProperties);
            ParquetMetadata parquetMetadata = hdfsEnvironment.doAs(user, () -> MetadataReader.readFooter(parquetDataSource, fileSize, fileDecryptor).getParquetMetadata());
            FileMetaData fileMetaData = parquetMetadata.getFileMetaData();
            MessageType fileSchema = fileMetaData.getSchema();

            Optional<MessageType> message = columns.stream()
                    .filter(column -> column.getColumnType() == REGULAR || isPushedDownSubfield(column))
                    .map(column -> getColumnType(typeManager.getType(column.getDataType()), fileSchema, column, tableName, path))
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .map(type -> new MessageType(fileSchema.getName(), type))
                    .reduce(MessageType::union);

            MessageType requestedSchema = message.orElse(new MessageType(fileSchema.getName(), ImmutableList.of()));

            ImmutableList.Builder<BlockMetaData> footerBlocks = ImmutableList.builder();
            for (BlockMetaData block : parquetMetadata.getBlocks()) {
                Integer firstIndex = MetadataReader.findFirstNonHiddenColumnId(block);
                if (firstIndex != null) {
                    long firstDataPage = block.getColumns().get(0).getFirstDataPageOffset();
                    if (firstDataPage >= start && firstDataPage < start + length) {
                        footerBlocks.add(block);
                    }
                }
            }

            Map<List<String>, RichColumnDescriptor> descriptorsByPath = getDescriptors(fileSchema, requestedSchema);
            TupleDomain<ColumnDescriptor> parquetTupleDomain = getParquetTupleDomain(descriptorsByPath, effectivePredicate);
            Predicate parquetPredicate = buildPredicate(requestedSchema, parquetTupleDomain, descriptorsByPath);
            final ParquetDataSource finalDataSource = dataSource;
            ImmutableList.Builder<BlockMetaData> blocks = ImmutableList.builder();
            List<ColumnIndexStore> blockIndexStores = new ArrayList<>();
            for (BlockMetaData block : footerBlocks.build()) {
                Optional<ColumnIndexStore> columnIndexStore = ColumnIndexFilterUtils.getColumnIndexStore(parquetPredicate, finalDataSource, block, descriptorsByPath, columnIndexFilterEnabled);
                if (predicateMatches(parquetPredicate, block, finalDataSource, descriptorsByPath, parquetTupleDomain, columnIndexStore, columnIndexFilterEnabled)) {
                    blocks.add(block);
                    blockIndexStores.add(columnIndexStore.orElse(null));
                }
            }
            MessageColumnIO messageColumnIO = getColumnIO(fileSchema, requestedSchema);
            ParquetReader parquetReader = new ParquetReader(
                    messageColumnIO,
                    blocks.build(),
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
            ImmutableList.Builder<Type> typesBuilder = ImmutableList.builder();
            ImmutableList.Builder<Optional<Field>> fieldsBuilder = ImmutableList.builder();
            for (DeltaColumnHandle column : columns) {
                checkArgument(column.getColumnType() == REGULAR || column.getColumnType() == SUBFIELD,
                        "column type must be regular or subfield column");

                String name = column.getName();
                Type type = typeManager.getType(column.getDataType());

                namesBuilder.add(name);
                typesBuilder.add(type);

                if (isPushedDownSubfield(column)) {
                    Subfield pushedDownSubfield = getPushedDownSubfield(column);
                    List<String> nestedColumnPath = nestedColumnPath(pushedDownSubfield);
                    Optional<ColumnIO> columnIO = findNestedColumnIO(lookupColumnByName(messageColumnIO, pushedDownSubfield.getRootName()), nestedColumnPath);
                    if (columnIO.isPresent()) {
                        fieldsBuilder.add(constructField(type, columnIO.get()));
                    }
                    else {
                        fieldsBuilder.add(Optional.empty());
                    }
                }
                else if (getParquetType(type, fileSchema, column, tableName, path).isPresent()) {
                    fieldsBuilder.add(constructField(type, lookupColumnByName(messageColumnIO, name)));
                }
                else {
                    fieldsBuilder.add(Optional.empty());
                }
            }
            return new ParquetPageSource(parquetReader, typesBuilder.build(), fieldsBuilder.build(), namesBuilder.build(), new RuntimeStats());
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
                throw new PrestoException(DELTA_BAD_DATA, exception);
            }
            if (exception instanceof AccessControlException) {
                throw new PrestoException(PERMISSION_DENIED, exception.getMessage(), exception);
            }
            if (nullToEmpty(exception.getMessage()).trim().equals("Filesystem closed") || exception instanceof FileNotFoundException) {
                throw new PrestoException(DELTA_CANNOT_OPEN_SPLIT, exception);
            }
            String message = format("Error opening Hive split %s (offset=%s, length=%s): %s", path, start, length, exception.getMessage());
            if (exception.getClass().getSimpleName().equals("BlockMissingException")) {
                throw new PrestoException(DELTA_MISSING_DATA, message, exception);
            }
            throw new PrestoException(DELTA_CANNOT_OPEN_SPLIT, message, exception);
        }
    }

    public static TupleDomain<ColumnDescriptor> getParquetTupleDomain(Map<List<String>, RichColumnDescriptor> descriptorsByPath, TupleDomain<DeltaColumnHandle> effectivePredicate)
    {
        if (effectivePredicate.isNone()) {
            return TupleDomain.none();
        }

        ImmutableMap.Builder<ColumnDescriptor, Domain> predicate = ImmutableMap.builder();
        for (Map.Entry<DeltaColumnHandle, Domain> entry : effectivePredicate.getDomains().get().entrySet()) {
            DeltaColumnHandle columnHandle = entry.getKey();

            RichColumnDescriptor descriptor;

            if (isPushedDownSubfield(columnHandle)) {
                Subfield pushedDownSubfield = getPushedDownSubfield(columnHandle);
                List<String> subfieldPath = columnPathFromSubfield(pushedDownSubfield);
                descriptor = descriptorsByPath.get(subfieldPath);
            }
            else {
                descriptor = descriptorsByPath.get(ImmutableList.of(columnHandle.getName()));
            }

            if (descriptor != null) {
                predicate.put(descriptor, entry.getValue());
            }
        }
        return TupleDomain.withColumnDomains(predicate.build());
    }

    public static Optional<org.apache.parquet.schema.Type> getParquetType(
            Type prestoType,
            MessageType messageType,
            DeltaColumnHandle column,
            SchemaTableName tableName,
            Path path)
    {
        org.apache.parquet.schema.Type type = getParquetTypeByName(column.getName(), messageType);
        if (type == null) {
            return Optional.empty();
        }

        if (!checkSchemaMatch(type, prestoType)) {
            String parquetTypeName;
            if (type.isPrimitive()) {
                parquetTypeName = type.asPrimitiveType().getPrimitiveTypeName().toString();
            }
            else {
                GroupType group = type.asGroupType();
                StringBuilder builder = new StringBuilder();
                group.writeToStringBuilder(builder, "");
                parquetTypeName = builder.toString();
            }
            throw new PrestoException(
                    DELTA_PARQUET_SCHEMA_MISMATCH,
                    format("The column %s of table %s is declared as type %s, but the Parquet file (%s) declares the column as type %s",
                            column.getName(),
                            tableName.toString(),
                            column.getDataType(),
                            path.toString(),
                            parquetTypeName));
        }
        return Optional.of(type);
    }

    public static Optional<org.apache.parquet.schema.Type> getColumnType(
            Type prestoType, MessageType messageType, DeltaColumnHandle column, SchemaTableName tableName, Path path)
    {
        if (isPushedDownSubfield(column)) {
            Subfield pushedDownSubfield = getPushedDownSubfield(column);
            return getSubfieldType(messageType, pushedDownSubfield.getRootName(), nestedColumnPath(pushedDownSubfield));
        }
        return getParquetType(prestoType, messageType, column, tableName, path);
    }
}
