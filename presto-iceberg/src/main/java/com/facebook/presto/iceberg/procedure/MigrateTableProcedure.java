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
package com.facebook.presto.iceberg.procedure;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.iceberg.CatalogType;
import com.facebook.presto.iceberg.CommitTaskData;
import com.facebook.presto.iceberg.IcebergAbstractMetadata;
import com.facebook.presto.iceberg.IcebergDistributedProcedureHandle;
import com.facebook.presto.iceberg.IcebergProcedureContext;
import com.facebook.presto.iceberg.IcebergTableHandle;
import com.facebook.presto.iceberg.IcebergTableLayoutHandle;
import com.facebook.presto.iceberg.IcebergTableName;
import com.facebook.presto.iceberg.PartitionData;
import com.facebook.presto.iceberg.SortField;
import com.facebook.presto.spi.ConnectorDistributedProcedureHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.facebook.presto.spi.procedure.DistributedProcedure;
import com.facebook.presto.spi.procedure.TableDataRewriteDistributedProcedure;
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Type;

import javax.inject.Inject;
import javax.inject.Provider;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;

import static com.facebook.presto.common.Utils.checkArgument;
import static com.facebook.presto.common.type.StandardTypes.VARCHAR;
import static com.facebook.presto.iceberg.IcebergSessionProperties.getCompressionCodec;
import static com.facebook.presto.iceberg.IcebergTableProperties.COMMIT_RETRIES;
import static com.facebook.presto.iceberg.IcebergTableProperties.LOCATION_PROPERTY;
import static com.facebook.presto.iceberg.IcebergUtil.getColumns;
import static com.facebook.presto.iceberg.IcebergUtil.getFileFormat;
import static com.facebook.presto.iceberg.IcebergUtil.getIcebergTable;
import static com.facebook.presto.iceberg.IcebergUtil.getSortFields;
import static com.facebook.presto.iceberg.IcebergUtil.tableExists;
import static com.facebook.presto.iceberg.IcebergUtil.tryGetLocation;
import static com.facebook.presto.iceberg.IcebergUtil.tryGetProperties;
import static com.facebook.presto.iceberg.IcebergUtil.tryGetSchema;
import static com.facebook.presto.iceberg.PartitionSpecConverter.toPrestoPartitionSpec;
import static com.facebook.presto.iceberg.SchemaConverter.toPrestoSchema;
import static com.facebook.presto.spi.StandardErrorCode.ALREADY_EXISTS;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.procedure.TableDataRewriteDistributedProcedure.SCHEMA;
import static com.facebook.presto.spi.procedure.TableDataRewriteDistributedProcedure.TABLE_NAME;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.TableProperties.COMMIT_NUM_RETRIES;
import static org.apache.iceberg.TableProperties.COMMIT_NUM_RETRIES_DEFAULT;
import static org.apache.iceberg.TableProperties.WRITE_DATA_LOCATION;

public class MigrateTableProcedure
        implements Provider<DistributedProcedure>
{
    TypeManager typeManager;
    JsonCodec<CommitTaskData> commitTaskCodec;

    @Inject
    public MigrateTableProcedure(
            TypeManager typeManager,
            JsonCodec<CommitTaskData> commitTaskCodec)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.commitTaskCodec = requireNonNull(commitTaskCodec, "commitTaskCodec is null");
    }

    @Override
    public DistributedProcedure get()
    {
        return new TableDataRewriteDistributedProcedure(
                "system",
                "migrate_table",
                ImmutableList.of(
                        new DistributedProcedure.Argument(SCHEMA, VARCHAR),
                        new DistributedProcedure.Argument(TABLE_NAME, VARCHAR),
                        new DistributedProcedure.Argument("target_table", VARCHAR),
                        new DistributedProcedure.Argument("target_location", VARCHAR, false, null)),
                (session, procedureContext, tableLayoutHandle, arguments, sortOrderIndex) -> beginCallDistributedProcedure(session, (IcebergProcedureContext) procedureContext, (IcebergTableLayoutHandle) tableLayoutHandle, arguments, sortOrderIndex),
                ((session, procedureContext, tableHandle, fragments) -> finishCallDistributedProcedure(session, (IcebergProcedureContext) procedureContext, tableHandle, fragments)),
                arguments -> {
                    checkArgument(arguments.length == 2, format("invalid number of arguments: %s (should have %s)", arguments.length, 2));
                    checkArgument(arguments[0] instanceof Table && arguments[1] instanceof IcebergAbstractMetadata, "Invalid arguments, required: [Table, IcebergAbstractMetadata]");
                    return new IcebergProcedureContext((Table) arguments[0], (IcebergAbstractMetadata) arguments[1]);
                });
    }

    private ConnectorDistributedProcedureHandle beginCallDistributedProcedure(ConnectorSession session, IcebergProcedureContext procedureContext, IcebergTableLayoutHandle layoutHandle, Object[] arguments, OptionalInt sortOrderIndex)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(getClass().getClassLoader())) {
            IcebergTableHandle tableHandle = layoutHandle.getTable();
            IcebergAbstractMetadata metadata = procedureContext.getMetadata();

            Optional<String> location = Optional.ofNullable((String) arguments[3]);
            if (metadata.getCatalogType() != CatalogType.HIVE && location.isPresent()) {
                throw new PrestoException(NOT_SUPPORTED, format("%s catalog do not support migrating table.", metadata.getCatalogType()));
            }

            ConnectorTableMetadata sourceTableMetadata = metadata.getTableMetadata(session, tableHandle);
            ImmutableMap.Builder<String, Object> propertiesBuilder = ImmutableMap.<String, Object>builder()
                    .putAll(sourceTableMetadata.getProperties().entrySet().stream()
                            .filter(entry -> !entry.getKey().equals(LOCATION_PROPERTY) &&
                                    !entry.getKey().equals(WRITE_DATA_LOCATION))
                            .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue)));
            String targetTable = (String) arguments[2];
            SchemaTableName targetSchemaTableName = SchemaTableName.valueOf(targetTable);
            location.ifPresent(loc -> propertiesBuilder.put(LOCATION_PROPERTY, loc));
            if (!sourceTableMetadata.getProperties().containsKey(COMMIT_NUM_RETRIES) && !sourceTableMetadata.getProperties().containsKey(COMMIT_RETRIES)) {
                propertiesBuilder.put(COMMIT_NUM_RETRIES, COMMIT_NUM_RETRIES_DEFAULT);
            }
            ConnectorTableMetadata targetTableMetadata = new ConnectorTableMetadata(targetSchemaTableName,
                    sourceTableMetadata.getColumns(),
                    propertiesBuilder.build());
            if (tableExists(metadata, session, targetSchemaTableName)) {
                throw new PrestoException(ALREADY_EXISTS, "Table already exists: " + targetSchemaTableName);
            }
            metadata.createTable(session, targetTableMetadata, false);

            Table icebergTable = getIcebergTable(metadata, session, targetSchemaTableName);
            List<SortField> sortFields = getSortFields(icebergTable);
            IcebergTableHandle newIcebergTableHandle = new IcebergTableHandle(
                    targetSchemaTableName.getSchemaName(),
                    IcebergTableName.from(targetSchemaTableName.getTableName()),
                    false,
                    tryGetLocation(icebergTable),
                    tryGetProperties(icebergTable),
                    tryGetSchema(icebergTable).map(SchemaParser::toJson),
                    Optional.empty(),
                    Optional.empty(),
                    sortFields,
                    ImmutableList.of(),
                    Optional.empty());
            return new IcebergDistributedProcedureHandle(
                    targetSchemaTableName.getSchemaName(),
                    IcebergTableName.from(targetSchemaTableName.getTableName()),
                    toPrestoSchema(icebergTable.schema(), typeManager),
                    toPrestoPartitionSpec(icebergTable.spec(), typeManager),
                    getColumns(icebergTable.schema(), icebergTable.spec(), typeManager),
                    icebergTable.location(),
                    getFileFormat(icebergTable),
                    getCompressionCodec(session),
                    icebergTable.properties(),
                    new IcebergTableLayoutHandle(
                            layoutHandle.getPartitionColumns(),
                            layoutHandle.getDataColumns(),
                            layoutHandle.getDomainPredicate(),
                            layoutHandle.getRemainingPredicate(),
                            layoutHandle.getPredicateColumns(),
                            layoutHandle.getRequestedColumns(),
                            layoutHandle.isPushdownFilterEnabled(),
                            layoutHandle.getPartitionColumnPredicate(),
                            layoutHandle.getPartitions(),
                            newIcebergTableHandle),
                    sortFields,
                    ImmutableMap.of());
        }
    }

    private void finishCallDistributedProcedure(ConnectorSession session, IcebergProcedureContext procedureContext, ConnectorDistributedProcedureHandle procedureHandle, Collection<Slice> fragments)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(getClass().getClassLoader())) {
            IcebergDistributedProcedureHandle handle = (IcebergDistributedProcedureHandle) procedureHandle;
            IcebergAbstractMetadata metadata = procedureContext.getMetadata();
            SchemaTableName targetTableName = new SchemaTableName(handle.getSchemaName(), handle.getTableName().getTableName());
            Table icebergTable = getIcebergTable(metadata, session, targetTableName);

            try {
                List<CommitTaskData> commitTasks = fragments.stream()
                        .map(slice -> commitTaskCodec.fromJson(slice.getBytes()))
                        .collect(toImmutableList());

                org.apache.iceberg.types.Type[] partitionColumnTypes = icebergTable.spec().fields().stream()
                        .map(field -> field.transform().getResultType(
                                icebergTable.schema().findType(field.sourceId())))
                        .toArray(Type[]::new);

                Set<DataFile> newFiles = new HashSet<>();
                for (CommitTaskData task : commitTasks) {
                    DataFiles.Builder builder = DataFiles.builder(icebergTable.spec())
                            .withPath(task.getPath())
                            .withFileSizeInBytes(task.getFileSizeInBytes())
                            .withFormat(handle.getFileFormat().name())
                            .withMetrics(task.getMetrics().metrics());

                    if (!icebergTable.spec().fields().isEmpty()) {
                        String partitionDataJson = task.getPartitionDataJson()
                                .orElseThrow(() -> new VerifyException("No partition data for partitioned table"));
                        builder.withPartition(PartitionData.fromJson(partitionDataJson, partitionColumnTypes));
                    }
                    newFiles.add(builder.build());
                }

                if (fragments.isEmpty()) {
                    return;
                }

                AppendFiles appendFiles = icebergTable.newAppend();
                newFiles.forEach(appendFiles::appendFile);
                appendFiles.commit();
            }
            catch (Exception e) {
                IcebergTableHandle icebergTableHandle = new IcebergTableHandle(handle.getSchemaName(), handle.getTableName(), false,
                        Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
                        ImmutableList.of(), ImmutableList.of(), Optional.empty());

                // If finish phase fails, try to delete the target table created in begin phase
                try {
                    metadata.dropTable(session, icebergTableHandle);
                }
                catch (Exception ex) {
                    // ignored
                }
                throw e;
            }
        }
    }
}
