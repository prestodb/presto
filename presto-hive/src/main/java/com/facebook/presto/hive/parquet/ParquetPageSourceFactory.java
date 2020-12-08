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

import com.facebook.presto.common.Subfield;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.hive.EncryptionInformation;
import com.facebook.presto.hive.FileFormatDataSourceStats;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveBatchPageSourceFactory;
import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.HiveFileContext;
import com.facebook.presto.hive.metastore.Storage;
import com.facebook.presto.memory.context.AggregatedMemoryContext;
import com.facebook.presto.parquet.Field;
import com.facebook.presto.parquet.ParquetCorruptionException;
import com.facebook.presto.parquet.ParquetDataSource;
import com.facebook.presto.parquet.RichColumnDescriptor;
import com.facebook.presto.parquet.cache.ParquetMetadataSource;
import com.facebook.presto.parquet.predicate.Predicate;
import com.facebook.presto.parquet.reader.ParquetReader;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.AccessControlException;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.ColumnIO;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.joda.time.DateTimeZone;

import javax.inject.Inject;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.common.type.StandardTypes.ARRAY;
import static com.facebook.presto.common.type.StandardTypes.BIGINT;
import static com.facebook.presto.common.type.StandardTypes.CHAR;
import static com.facebook.presto.common.type.StandardTypes.DATE;
import static com.facebook.presto.common.type.StandardTypes.DECIMAL;
import static com.facebook.presto.common.type.StandardTypes.INTEGER;
import static com.facebook.presto.common.type.StandardTypes.MAP;
import static com.facebook.presto.common.type.StandardTypes.REAL;
import static com.facebook.presto.common.type.StandardTypes.ROW;
import static com.facebook.presto.common.type.StandardTypes.SMALLINT;
import static com.facebook.presto.common.type.StandardTypes.TIMESTAMP;
import static com.facebook.presto.common.type.StandardTypes.TINYINT;
import static com.facebook.presto.common.type.StandardTypes.VARBINARY;
import static com.facebook.presto.common.type.StandardTypes.VARCHAR;
import static com.facebook.presto.hive.HiveColumnHandle.ColumnType.AGGREGATED;
import static com.facebook.presto.hive.HiveColumnHandle.ColumnType.REGULAR;
import static com.facebook.presto.hive.HiveColumnHandle.ColumnType.SYNTHESIZED;
import static com.facebook.presto.hive.HiveColumnHandle.getPushedDownSubfield;
import static com.facebook.presto.hive.HiveColumnHandle.isPushedDownSubfield;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_BAD_DATA;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_CANNOT_OPEN_SPLIT;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_MISSING_DATA;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_PARTITION_SCHEMA_MISMATCH;
import static com.facebook.presto.hive.HiveSessionProperties.getParquetMaxReadBlockSize;
import static com.facebook.presto.hive.HiveSessionProperties.isFailOnCorruptedParquetStatistics;
import static com.facebook.presto.hive.HiveSessionProperties.isParquetBatchReaderVerificationEnabled;
import static com.facebook.presto.hive.HiveSessionProperties.isParquetBatchReadsEnabled;
import static com.facebook.presto.hive.HiveSessionProperties.isUseParquetColumnNames;
import static com.facebook.presto.hive.parquet.HdfsParquetDataSource.buildHdfsParquetDataSource;
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
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category.PRIMITIVE;
import static org.apache.parquet.io.ColumnIOConverter.constructField;
import static org.apache.parquet.io.ColumnIOConverter.findNestedColumnIO;

public class ParquetPageSourceFactory
        implements HiveBatchPageSourceFactory
{
    private static final Set<String> PARQUET_SERDE_CLASS_NAMES = ImmutableSet.<String>builder()
            .add("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe")
            .add("parquet.hive.serde.ParquetHiveSerDe")
            .build();

    private final TypeManager typeManager;
    private final StandardFunctionResolution functionResolution;
    private final HdfsEnvironment hdfsEnvironment;
    private final FileFormatDataSourceStats stats;
    private final ParquetMetadataSource parquetMetadataSource;

    @Inject
    public ParquetPageSourceFactory(TypeManager typeManager,
            StandardFunctionResolution functionResolution,
            HdfsEnvironment hdfsEnvironment,
            FileFormatDataSourceStats stats,
            ParquetMetadataSource parquetMetadataSource)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.functionResolution = requireNonNull(functionResolution, "functionResolution is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.stats = requireNonNull(stats, "stats is null");
        this.parquetMetadataSource = requireNonNull(parquetMetadataSource, "parquetMetadataSource is null");
    }

    @Override
    public Optional<? extends ConnectorPageSource> createPageSource(
            Configuration configuration,
            ConnectorSession session,
            Path path,
            long start,
            long length,
            long fileSize,
            Storage storage,
            SchemaTableName tableName,
            Map<String, String> tableParameters,
            List<HiveColumnHandle> columns,
            TupleDomain<HiveColumnHandle> effectivePredicate,
            DateTimeZone hiveStorageTimeZone,
            HiveFileContext hiveFileContext,
            Optional<EncryptionInformation> encryptionInformation)
    {
        if (!PARQUET_SERDE_CLASS_NAMES.contains(storage.getStorageFormat().getSerDe())) {
            return Optional.empty();
        }

        return Optional.of(createParquetPageSource(
                hdfsEnvironment,
                session.getUser(),
                configuration,
                path,
                start,
                length,
                fileSize,
                columns,
                tableName,
                isUseParquetColumnNames(session),
                isFailOnCorruptedParquetStatistics(session),
                getParquetMaxReadBlockSize(session),
                isParquetBatchReadsEnabled(session),
                isParquetBatchReaderVerificationEnabled(session),
                typeManager,
                functionResolution,
                effectivePredicate,
                stats,
                hiveFileContext,
                parquetMetadataSource));
    }

    public static ConnectorPageSource createParquetPageSource(
            HdfsEnvironment hdfsEnvironment,
            String user,
            Configuration configuration,
            Path path,
            long start,
            long length,
            long fileSize,
            List<HiveColumnHandle> columns,
            SchemaTableName tableName,
            boolean useParquetColumnNames,
            boolean failOnCorruptedParquetStatistics,
            DataSize maxReadBlockSize,
            boolean batchReaderEnabled,
            boolean verificationEnabled,
            TypeManager typeManager,
            StandardFunctionResolution functionResolution,
            TupleDomain<HiveColumnHandle> effectivePredicate,
            FileFormatDataSourceStats stats,
            HiveFileContext hiveFileContext,
            ParquetMetadataSource parquetMetadataSource)
    {
        AggregatedMemoryContext systemMemoryContext = newSimpleAggregatedMemoryContext();

        ParquetDataSource dataSource = null;
        try {
            FSDataInputStream inputStream = hdfsEnvironment.getFileSystem(user, path, configuration).openFile(path, hiveFileContext);
            dataSource = buildHdfsParquetDataSource(inputStream, path, stats);
            ParquetMetadata parquetMetadata = parquetMetadataSource.getParquetMetadata(inputStream, dataSource.getId(), fileSize, hiveFileContext.isCacheable()).getParquetMetadata();

            if (!columns.isEmpty() && columns.stream().allMatch(hiveColumnHandle -> hiveColumnHandle.getColumnType() == AGGREGATED)) {
                return new AggregatedParquetPageSource(columns, parquetMetadata, typeManager, functionResolution);
            }

            FileMetaData fileMetaData = parquetMetadata.getFileMetaData();
            MessageType fileSchema = fileMetaData.getSchema();

            Optional<MessageType> message = columns.stream()
                    .filter(column -> column.getColumnType() == REGULAR || isPushedDownSubfield(column))
                    .map(column -> getColumnType(typeManager.getType(column.getTypeSignature()), fileSchema, useParquetColumnNames, column, tableName, path))
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .map(type -> new MessageType(fileSchema.getName(), type))
                    .reduce(MessageType::union);

            MessageType requestedSchema = message.orElse(new MessageType(fileSchema.getName(), ImmutableList.of()));

            ImmutableList.Builder<BlockMetaData> footerBlocks = ImmutableList.builder();
            for (BlockMetaData block : parquetMetadata.getBlocks()) {
                long firstDataPage = block.getColumns().get(0).getFirstDataPageOffset();
                if (firstDataPage >= start && firstDataPage < start + length) {
                    footerBlocks.add(block);
                }
            }

            Map<List<String>, RichColumnDescriptor> descriptorsByPath = getDescriptors(fileSchema, requestedSchema);
            TupleDomain<ColumnDescriptor> parquetTupleDomain = getParquetTupleDomain(descriptorsByPath, effectivePredicate);
            Predicate parquetPredicate = buildPredicate(requestedSchema, parquetTupleDomain, descriptorsByPath);
            final ParquetDataSource finalDataSource = dataSource;
            ImmutableList.Builder<BlockMetaData> blocks = ImmutableList.builder();
            for (BlockMetaData block : footerBlocks.build()) {
                if (predicateMatches(parquetPredicate, block, finalDataSource, descriptorsByPath, parquetTupleDomain, failOnCorruptedParquetStatistics)) {
                    blocks.add(block);
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
                    verificationEnabled);

            ImmutableList.Builder<String> namesBuilder = ImmutableList.builder();
            ImmutableList.Builder<Type> typesBuilder = ImmutableList.builder();
            ImmutableList.Builder<Optional<Field>> fieldsBuilder = ImmutableList.builder();
            for (HiveColumnHandle column : columns) {
                checkArgument(column.getColumnType() == REGULAR || column.getColumnType() == SYNTHESIZED, "column type must be regular or synthesized column");

                String name = column.getName();
                Type type = typeManager.getType(column.getTypeSignature());

                namesBuilder.add(name);
                typesBuilder.add(type);

                if (column.getColumnType() == SYNTHESIZED) {
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
                else if (getParquetType(type, fileSchema, useParquetColumnNames, column, tableName, path).isPresent()) {
                    String columnName = useParquetColumnNames ? name : fileSchema.getFields().get(column.getHiveColumnIndex()).getName();
                    fieldsBuilder.add(constructField(type, lookupColumnByName(messageColumnIO, columnName)));
                }
                else {
                    fieldsBuilder.add(Optional.empty());
                }
            }
            return new ParquetPageSource(parquetReader, typesBuilder.build(), fieldsBuilder.build(), namesBuilder.build());
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
            if (e instanceof ParquetCorruptionException) {
                throw new PrestoException(HIVE_BAD_DATA, e);
            }
            if (e instanceof AccessControlException) {
                throw new PrestoException(PERMISSION_DENIED, e.getMessage(), e);
            }
            if (nullToEmpty(e.getMessage()).trim().equals("Filesystem closed") ||
                    e instanceof FileNotFoundException) {
                throw new PrestoException(HIVE_CANNOT_OPEN_SPLIT, e);
            }
            String message = format("Error opening Hive split %s (offset=%s, length=%s): %s", path, start, length, e.getMessage());
            if (e.getClass().getSimpleName().equals("BlockMissingException")) {
                throw new PrestoException(HIVE_MISSING_DATA, message, e);
            }
            throw new PrestoException(HIVE_CANNOT_OPEN_SPLIT, message, e);
        }
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

    public static Optional<org.apache.parquet.schema.Type> getParquetType(Type prestoType, MessageType messageType, boolean useParquetColumnNames, HiveColumnHandle column, SchemaTableName tableName, Path path)
    {
        org.apache.parquet.schema.Type type = null;
        if (useParquetColumnNames) {
            type = getParquetTypeByName(column.getName(), messageType);
        }
        else if (column.getHiveColumnIndex() < messageType.getFieldCount()) {
            type = messageType.getType(column.getHiveColumnIndex());
        }

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
            throw new PrestoException(HIVE_PARTITION_SCHEMA_MISMATCH, format("The column %s of table %s is declared as type %s, but the Parquet file (%s) declares the column as type %s",
                    column.getName(),
                    tableName.toString(),
                    column.getHiveType(),
                    path.toString(),
                    parquetTypeName));
        }
        return Optional.of(type);
    }

    private static boolean checkSchemaMatch(org.apache.parquet.schema.Type parquetType, Type type)
    {
        String prestoType = type.getTypeSignature().getBase();
        if (parquetType instanceof GroupType) {
            GroupType groupType = parquetType.asGroupType();
            switch (prestoType) {
                case ROW:
                    RowType rowType = (RowType) type;
                    Map<String, Type> prestoFieldMap = rowType.getFields().stream().collect(
                            Collectors.toMap(
                                    field -> field.getName().get().toLowerCase(Locale.ENGLISH),
                                    field -> field.getType()));
                    for (int i = 0; i < groupType.getFields().size(); i++) {
                        org.apache.parquet.schema.Type parquetFieldType = groupType.getFields().get(i);
                        String fieldName = parquetFieldType.getName().toLowerCase(Locale.ENGLISH);
                        Type prestoFieldType = prestoFieldMap.get(fieldName);
                        if (prestoFieldType != null && !checkSchemaMatch(parquetFieldType, prestoFieldType)) {
                            return false;
                        }
                    }
                    return true;
                case MAP:
                    if (groupType.getFields().size() != 1) {
                        return false;
                    }
                    org.apache.parquet.schema.Type mapKeyType = groupType.getFields().get(0);
                    if (mapKeyType instanceof GroupType) {
                        GroupType mapGroupType = mapKeyType.asGroupType();
                        return mapGroupType.getFields().size() == 2 &&
                                checkSchemaMatch(mapGroupType.getFields().get(0), type.getTypeParameters().get(0)) &&
                                checkSchemaMatch(mapGroupType.getFields().get(1), type.getTypeParameters().get(1));
                    }
                    return false;
                case ARRAY:
                    /* array has a standard 3-level structure with middle level repeated group with a single field:
                     *  optional group my_list (LIST) {
                     *     repeated group element {
                     *        required type field;
                     *     };
                     *  }
                     *  Backward-compatibility support for 2-level arrays:
                     *   optional group my_list (LIST) {
                     *      repeated type field;
                     *   }
                     *  field itself could be primitive or group
                     */
                    if (groupType.getFields().size() != 1) {
                        return false;
                    }
                    org.apache.parquet.schema.Type bagType = groupType.getFields().get(0);
                    if (bagType.isPrimitive()) {
                        return checkSchemaMatch(bagType.asPrimitiveType(), type.getTypeParameters().get(0));
                    }
                    GroupType bagGroupType = bagType.asGroupType();
                    return checkSchemaMatch(bagGroupType, type.getTypeParameters().get(0)) ||
                            (bagGroupType.getFields().size() == 1 && checkSchemaMatch(bagGroupType.getFields().get(0), type.getTypeParameters().get(0)));
                default:
                    return false;
            }
        }

        checkArgument(parquetType.isPrimitive(), "Unexpected parquet type for column: %s " + parquetType.getName());
        PrimitiveTypeName parquetTypeName = parquetType.asPrimitiveType().getPrimitiveTypeName();
        switch (parquetTypeName) {
            case INT64:
                return prestoType.equals(BIGINT) || prestoType.equals(DECIMAL) || prestoType.equals(TIMESTAMP);
            case INT32:
                return prestoType.equals(INTEGER) || prestoType.equals(BIGINT) || prestoType.equals(SMALLINT) || prestoType.equals(DATE) || prestoType.equals(DECIMAL) || prestoType.equals(TINYINT);
            case BOOLEAN:
                return prestoType.equals(StandardTypes.BOOLEAN);
            case FLOAT:
                return prestoType.equals(REAL);
            case DOUBLE:
                return prestoType.equals(StandardTypes.DOUBLE);
            case BINARY:
                return prestoType.equals(VARBINARY) || prestoType.equals(VARCHAR) || prestoType.startsWith(CHAR) || prestoType.equals(DECIMAL);
            case INT96:
                return prestoType.equals(TIMESTAMP);
            case FIXED_LEN_BYTE_ARRAY:
                return prestoType.equals(DECIMAL);
            default:
                throw new IllegalArgumentException("Unexpected parquet type name: " + parquetTypeName);
        }
    }

    public static Optional<org.apache.parquet.schema.Type> getColumnType(Type prestoType, MessageType messageType, boolean useParquetColumnNames, HiveColumnHandle column, SchemaTableName tableName, Path path)
    {
        if (useParquetColumnNames && isPushedDownSubfield(column)) {
            Subfield pushedDownSubfield = getPushedDownSubfield(column);
            return getSubfieldType(messageType, pushedDownSubfield.getRootName(), nestedColumnPath(pushedDownSubfield));
        }
        return getParquetType(prestoType, messageType, useParquetColumnNames, column, tableName, path);
    }
}
