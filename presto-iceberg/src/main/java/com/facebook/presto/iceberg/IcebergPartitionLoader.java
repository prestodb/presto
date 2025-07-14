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
package com.facebook.presto.iceberg;

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.NullableValue;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.hive.HivePartition;
import com.facebook.presto.hive.LazyLoadedPartitions.PartitionLoader;
import com.facebook.presto.hive.PartitionNameWithVersion;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.Constraint;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.common.Utils.checkArgument;
import static com.facebook.presto.iceberg.ExpressionConverter.toIcebergExpression;
import static com.facebook.presto.iceberg.IcebergUtil.getFileFormat;
import static com.facebook.presto.iceberg.IcebergUtil.getIdentityPartitions;
import static com.facebook.presto.iceberg.IcebergUtil.getNonMetadataColumnConstraints;
import static com.facebook.presto.iceberg.IcebergUtil.parsePartitionValue;
import static com.facebook.presto.iceberg.IcebergUtil.resolveSnapshotIdByName;
import static com.facebook.presto.iceberg.TypeConverter.toPrestoType;
import static org.apache.iceberg.types.Type.TypeID.BINARY;
import static org.apache.iceberg.types.Type.TypeID.FIXED;

public class IcebergPartitionLoader
        implements PartitionLoader
{
    private final TypeManager typeManager;
    private final FileFormat fileFormat;
    private final ConnectorTableHandle tableHandle;
    private final Constraint<ColumnHandle> constraint;
    private final List<IcebergColumnHandle> partitionColumns;
    private final boolean isEmptyTable;
    private final TableScan tableScan;
    int maxPartitionThreshold = -1;

    public IcebergPartitionLoader(
            TypeManager typeManager,
            ConnectorTableHandle tableHandle,
            Table icebergTable,
            Constraint<ColumnHandle> constraint,
            List<IcebergColumnHandle> partitionColumns)
    {
        this.typeManager = typeManager;
        this.partitionColumns = partitionColumns;
        this.fileFormat = getFileFormat(icebergTable);
        this.tableHandle = tableHandle;
        this.constraint = constraint;
        IcebergTableName name = ((IcebergTableHandle) tableHandle).getIcebergTableName();
        // Empty iceberg table would cause `snapshotId` not present
        Optional<Long> snapshotId = resolveSnapshotIdByName(icebergTable, name);
        if (!snapshotId.isPresent()) {
            this.isEmptyTable = true;
            this.tableScan = null;
        }
        else {
            this.isEmptyTable = false;
            this.tableScan = icebergTable.newScan()
                    .filter(toIcebergExpression(getNonMetadataColumnConstraints(constraint
                            .getSummary()
                            .simplify())))
                    .useSnapshot(snapshotId.get());
        }
    }

    @Override
    public synchronized List<HivePartition> loadPartitions()
    {
        if (isEmptyTable) {
            return ImmutableList.of();
        }

        // When `maxPartitionThreshold == 0`, skip the optimization directly
        if (maxPartitionThreshold == 0) {
            return ImmutableList.of(new HivePartition(((IcebergTableHandle) tableHandle).getSchemaTableName()));
        }

        Set<HivePartition> partitions = new HashSet<>();
        try (CloseableIterable<FileScanTask> fileScanTasks = tableScan.planFiles()) {
            for (FileScanTask fileScanTask : fileScanTasks) {
                // If exists delete files, skip the metadata optimization based on partition values as they might become incorrect
                if (!fileScanTask.deletes().isEmpty()) {
                    return ImmutableList.of(new HivePartition(((IcebergTableHandle) tableHandle).getSchemaTableName()));
                }

                // If threshold is set explicitly greater than 0, and partitions number exceeds the threshold, skip the optimization as well
                if (maxPartitionThreshold > 0 && partitions.size() >= maxPartitionThreshold) {
                    return ImmutableList.of(new HivePartition(((IcebergTableHandle) tableHandle).getSchemaTableName()));
                }
                StructLike partition = fileScanTask.file().partition();
                PartitionSpec spec = fileScanTask.spec();
                Map<PartitionField, Integer> fieldToIndex = getIdentityPartitions(spec);
                ImmutableMap.Builder<ColumnHandle, NullableValue> builder = ImmutableMap.builder();

                fieldToIndex.forEach((field, index) -> {
                    int id = field.sourceId();
                    org.apache.iceberg.types.Type type = spec.schema().findType(id);
                    Class<?> javaClass = type.typeId().javaClass();
                    Object value = partition.get(index, javaClass);
                    String partitionStringValue;

                    if (value == null) {
                        partitionStringValue = null;
                    }
                    else if (type.typeId() == FIXED || type.typeId() == BINARY) {
                        partitionStringValue = Base64.getEncoder().encodeToString(((ByteBuffer) value).array());
                    }
                    else {
                        partitionStringValue = value.toString();
                    }

                    NullableValue partitionValue = parsePartitionValue(fileFormat, partitionStringValue, toPrestoType(type, typeManager), partition.toString());
                    Optional<IcebergColumnHandle> column = partitionColumns.stream()
                            .filter(icebergColumnHandle -> Objects.equals(icebergColumnHandle.getId(), field.sourceId()))
                            .findAny();

                    if (column.isPresent()) {
                        builder.put(column.get(), partitionValue);
                    }
                });

                Map<ColumnHandle, NullableValue> values = builder.build();
                HivePartition newPartition = new HivePartition(
                        ((IcebergTableHandle) tableHandle).getSchemaTableName(),
                        new PartitionNameWithVersion(partition.toString(), Optional.empty()),
                        values);

                boolean isIncludePartition = true;
                Map<ColumnHandle, Domain> domains = constraint.getSummary().getDomains().get();
                for (IcebergColumnHandle column : partitionColumns) {
                    NullableValue value = newPartition.getKeys().get(column);
                    Domain allowedDomain = domains.get(column);
                    if (allowedDomain != null && !allowedDomain.includesNullableValue(value.getValue())) {
                        isIncludePartition = false;
                        break;
                    }
                }

                if (constraint.predicate().isPresent() && !constraint.predicate().get().test(newPartition.getKeys())) {
                    isIncludePartition = false;
                }

                if (isIncludePartition) {
                    partitions.add(newPartition);
                }
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return new ArrayList<>(partitions);
    }

    @Override
    public synchronized boolean isEmpty()
    {
        if (isEmptyTable) {
            return true;
        }

        try (CloseableIterable<FileScanTask> fileScanTasks = tableScan.planFiles();
                CloseableIterator<FileScanTask> iterator = fileScanTasks.iterator()) {
            return !iterator.hasNext();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void setMaxPartitionThreshold(int maxPartitionThreshold)
    {
        checkArgument(maxPartitionThreshold > -1, "maxPartitionThreshold must greater than -1");
        if (this.maxPartitionThreshold == -1) {
            this.maxPartitionThreshold = maxPartitionThreshold;
        }
    }
}
