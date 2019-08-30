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

import com.facebook.presto.hive.FileFormatDataSourceStats;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveBatchPageSourceFactory;
import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.HiveType;
import com.facebook.presto.memory.context.AggregatedMemoryContext;
import com.facebook.presto.parquet.ParquetCorruptionException;
import com.facebook.presto.parquet.ParquetDataSource;
import com.facebook.presto.parquet.RichColumnDescriptor;
import com.facebook.presto.parquet.predicate.Predicate;
import com.facebook.presto.parquet.reader.MetadataReader;
import com.facebook.presto.parquet.reader.ParquetReader;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.joda.time.DateTimeZone;

import javax.inject.Inject;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import static com.facebook.presto.hive.HiveColumnHandle.ColumnType.REGULAR;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_BAD_DATA;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_CANNOT_OPEN_SPLIT;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_MISSING_DATA;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_PARTITION_SCHEMA_MISMATCH;
import static com.facebook.presto.hive.HiveSessionProperties.isFailOnCorruptedParquetStatistics;
import static com.facebook.presto.hive.HiveSessionProperties.isUseParquetColumnNames;
import static com.facebook.presto.hive.HiveUtil.getDeserializerClassName;
import static com.facebook.presto.hive.parquet.HdfsParquetDataSource.buildHdfsParquetDataSource;
import static com.facebook.presto.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static com.facebook.presto.parquet.ParquetTypeUtils.getColumnIO;
import static com.facebook.presto.parquet.ParquetTypeUtils.getDescriptors;
import static com.facebook.presto.parquet.ParquetTypeUtils.getParquetTypeByName;
import static com.facebook.presto.parquet.predicate.PredicateUtils.buildPredicate;
import static com.facebook.presto.parquet.predicate.PredicateUtils.predicateMatches;
import static com.facebook.presto.spi.type.StandardTypes.ARRAY;
import static com.facebook.presto.spi.type.StandardTypes.BIGINT;
import static com.facebook.presto.spi.type.StandardTypes.CHAR;
import static com.facebook.presto.spi.type.StandardTypes.DATE;
import static com.facebook.presto.spi.type.StandardTypes.DECIMAL;
import static com.facebook.presto.spi.type.StandardTypes.INTEGER;
import static com.facebook.presto.spi.type.StandardTypes.MAP;
import static com.facebook.presto.spi.type.StandardTypes.REAL;
import static com.facebook.presto.spi.type.StandardTypes.ROW;
import static com.facebook.presto.spi.type.StandardTypes.SMALLINT;
import static com.facebook.presto.spi.type.StandardTypes.TIMESTAMP;
import static com.facebook.presto.spi.type.StandardTypes.TINYINT;
import static com.facebook.presto.spi.type.StandardTypes.VARBINARY;
import static com.facebook.presto.spi.type.StandardTypes.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.nullToEmpty;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category.PRIMITIVE;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT96;

public class ParquetPageSourceFactory
        implements HiveBatchPageSourceFactory
{
    private static final Set<String> PARQUET_SERDE_CLASS_NAMES = ImmutableSet.<String>builder()
            .add("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe")
            .add("parquet.hive.serde.ParquetHiveSerDe")
            .build();

    private final TypeManager typeManager;
    private final HdfsEnvironment hdfsEnvironment;
    private final FileFormatDataSourceStats stats;

    @Inject
    public ParquetPageSourceFactory(TypeManager typeManager, HdfsEnvironment hdfsEnvironment, FileFormatDataSourceStats stats)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.stats = requireNonNull(stats, "stats is null");
    }

    @Override
    public Optional<? extends ConnectorPageSource> createPageSource(
            Configuration configuration,
            ConnectorSession session,
            Path path,
            long start,
            long length,
            long fileSize,
            Properties schema,
            List<HiveColumnHandle> columns,
            TupleDomain<HiveColumnHandle> effectivePredicate,
            DateTimeZone hiveStorageTimeZone)
    {
        if (!PARQUET_SERDE_CLASS_NAMES.contains(getDeserializerClassName(schema))) {
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
                schema,
                columns,
                isUseParquetColumnNames(session),
                isFailOnCorruptedParquetStatistics(session),
                typeManager,
                effectivePredicate,
                stats));
    }

    public static ParquetPageSource createParquetPageSource(
            HdfsEnvironment hdfsEnvironment,
            String user,
            Configuration configuration,
            Path path,
            long start,
            long length,
            long fileSize,
            Properties schema,
            List<HiveColumnHandle> columns,
            boolean useParquetColumnNames,
            boolean failOnCorruptedParquetStatistics,
            TypeManager typeManager,
            TupleDomain<HiveColumnHandle> effectivePredicate,
            FileFormatDataSourceStats stats)
    {
        AggregatedMemoryContext systemMemoryContext = newSimpleAggregatedMemoryContext();

        ParquetDataSource dataSource = null;
        try {
            FileSystem fileSystem = hdfsEnvironment.getFileSystem(user, path, configuration);
            FSDataInputStream inputStream = fileSystem.open(path);
            ParquetMetadata parquetMetadata = MetadataReader.readFooter(inputStream, path, fileSize);
            FileMetaData fileMetaData = parquetMetadata.getFileMetaData();
            MessageType fileSchema = fileMetaData.getSchema();
            dataSource = buildHdfsParquetDataSource(inputStream, path, fileSize, stats);

            List<org.apache.parquet.schema.Type> fields = columns.stream()
                    .filter(column -> column.getColumnType() == REGULAR)
                    .map(column -> getParquetType(typeManager.getType(column.getTypeSignature()), fileSchema, useParquetColumnNames, column.getName(), column.getHiveColumnIndex(), column.getHiveType()))
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .collect(toList());

            MessageType requestedSchema = new MessageType(fileSchema.getName(), fields);

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
                    systemMemoryContext);

            return new ParquetPageSource(
                    parquetReader,
                    fileSchema,
                    messageColumnIO,
                    typeManager,
                    schema,
                    columns,
                    effectivePredicate,
                    useParquetColumnNames);
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

            RichColumnDescriptor descriptor = descriptorsByPath.get(ImmutableList.of(columnHandle.getName()));
            if (descriptor != null) {
                predicate.put(descriptor, entry.getValue());
            }
        }
        return TupleDomain.withColumnDomains(predicate.build());
    }

    public static Optional<org.apache.parquet.schema.Type> getParquetType(Type prestoType, MessageType messageType, boolean useParquetColumnNames, String columnName, int columnHiveIndex, HiveType hiveType)
    {
        org.apache.parquet.schema.Type type = null;
        if (useParquetColumnNames) {
            type = getParquetTypeByName(columnName, messageType);
        }
        else if (columnHiveIndex < messageType.getFieldCount()) {
            type = messageType.getType(columnHiveIndex);
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
            throw new PrestoException(HIVE_PARTITION_SCHEMA_MISMATCH, format("The column %s is declared as type %s, but the Parquet file declares the column as type %s",
                                            columnName,
                                            hiveType,
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
                    if (groupType.getFields().size() == type.getTypeParameters().size()) {
                        for (int i = 0; i < groupType.getFields().size(); i++) {
                            if (!checkSchemaMatch(groupType.getFields().get(i), type.getTypeParameters().get(i))) {
                                return false;
                            }
                        }
                        return true;
                    }
                    return false;
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
                return prestoType.equals(BIGINT) || prestoType.equals(DECIMAL);
            case INT32:
                return prestoType.equals(INTEGER) || prestoType.equals(SMALLINT) || prestoType.equals(DATE) || prestoType.equals(DECIMAL) || prestoType.equals(TINYINT);
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
}
