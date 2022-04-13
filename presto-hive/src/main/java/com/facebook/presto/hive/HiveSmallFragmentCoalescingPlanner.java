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
package com.facebook.presto.hive;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.json.smile.SmileCodec;
import com.facebook.presto.common.Subfield;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.hive.PartitionUpdate.FileWriteInfo;
import com.facebook.presto.hive.metastore.Partition;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSmallFragmentCoalescingPlan;
import com.facebook.presto.spi.ConnectorSmallFragmentCoalescingPlan.SubPlan;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import org.joda.time.DateTimeZone;

import javax.inject.Inject;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static com.facebook.presto.common.predicate.Domain.multipleValues;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.expressions.LogicalRowExpressions.TRUE_CONSTANT;
import static com.facebook.presto.hive.HiveSessionProperties.isOptimizedPartitionUpdateSerializationEnabled;
import static com.facebook.presto.hive.HiveUtil.getPartitionKeyColumnHandles;
import static com.facebook.presto.hive.HiveUtil.serializeZstdCompressed;
import static com.facebook.presto.hive.LocationHandle.TableType.TEMPORARY;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Predicates.alwaysTrue;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Stream.concat;

/**
 * A helper to produce a plan for coalescing small fragments in a narrow set of cases.
 *
 * Works with partition and unpartitioned tables that aren't bucketed or using numbered file names.
 */
public class HiveSmallFragmentCoalescingPlanner
        implements SmallFragmentCoalescingPlanner
{
    private final JsonCodec<PartitionUpdate> partitionUpdateCodec;
    private final SmileCodec<PartitionUpdate> partitionUpdateSmileCodec;
    private final DateTimeZone timeZone;
    private final TypeManager typeManager;
    private final PartitionObjectBuilder partitionObjectBuilder;
    private final String prestoVersion;

    @Inject
    public HiveSmallFragmentCoalescingPlanner(
            JsonCodec<PartitionUpdate> partitionUpdateCodec,
            SmileCodec<PartitionUpdate> partitionUpdateSmileCodec,
            HiveClientConfig clientConfig,
            TypeManager typeManager,
            PartitionObjectBuilder partitionObjectBuilder,
            NodeVersion nodeVersion)
    {
        this.partitionUpdateCodec = requireNonNull(partitionUpdateCodec, "partitionUpdateCodec is null");
        this.partitionUpdateSmileCodec = requireNonNull(partitionUpdateSmileCodec, "partitionSmileUpdateCodec is null");
        this.timeZone = requireNonNull(clientConfig.getDateTimeZone(), "timeZone is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.partitionObjectBuilder = requireNonNull(partitionObjectBuilder, "partitionObjectBuilder is null");
        this.prestoVersion = requireNonNull(nodeVersion, "nodeVersion is null").toString();
    }

    @Override
    public ConnectorSmallFragmentCoalescingPlan createSmallFragmentCoalescingPlan(
            ConnectorSession session,
            HiveWritableTableHandle hiveWritableTableHandle,
            List<PartitionUpdate> partitionUpdates,
            Collection<Slice> fragments)
    {
        long sizeLimit = HiveSessionProperties.getSmallFileCoalescingThreshold(session).toBytes();
        // Checks to make sure this is supported are done here at the public entry point for the planner
        if (hiveWritableTableHandle.getBucketProperty().isPresent()
                || hiveWritableTableHandle.getLocationHandle().getTableType() == TEMPORARY
                || partitionUpdates.stream().anyMatch(PartitionUpdate::containsNumberedFileNames)
                // Some HiveFileWriters won't generate the statistics page
                || partitionUpdates.stream().anyMatch(HiveSmallFragmentCoalescingPlanner::partitionUpdateMissingStatistics)) {
            return ConnectorSmallFragmentCoalescingPlan.noOpPlan(fragments);
        }
        boolean optimizedPartitionUpdateSerializationEnabled = isOptimizedPartitionUpdateSerializationEnabled(session);

        List<Slice> retainedFragments = concat(
                // Retain files with no size or that are too big to coalesce
                partitionUpdates.stream()
                        // Need this filter to avoid including the PartitionUpdate row count twice
                        .filter(update -> update.getFileWriteInfos().size() > 1)
                        .map(update -> buildFragment(update, optimizedPartitionUpdateSerializationEnabled, info -> !info.getFileWriteStatistics().isPresent() || info.getFileWriteStatistics().get().getFileSize() > sizeLimit)),
                // Retain files from partition updates for partitions with only one file
                partitionUpdates.stream()
                        .filter(update -> update.getFileWriteInfos().size() <= 1)
                        .map(update -> buildFragment(update, optimizedPartitionUpdateSerializationEnabled, alwaysTrue()))
                ).collect(toList());
        List<Slice> deprecatedFragments = partitionUpdates.stream()
                .filter(update -> update.getFileWriteInfos().size() > 1)
                .map(update -> buildFragment(update, optimizedPartitionUpdateSerializationEnabled, info -> info.getFileWriteStatistics().isPresent() && info.getFileWriteStatistics().get().getFileSize() <= sizeLimit))
                .collect(toImmutableList());
        Stream<SubPlan> subPlans = partitionUpdates.stream()
                .filter(update -> hasCoalescableFiles(update, sizeLimit))
                .map(partitionUpdate -> coalescePartitionFiles(session, hiveWritableTableHandle, partitionUpdate, sizeLimit));
        return new ConnectorSmallFragmentCoalescingPlan(retainedFragments, deprecatedFragments, subPlans);
    }

    private Slice buildFragment(
            PartitionUpdate update,
            boolean optimizedPartitionUpdateSerializationEnabled,
            Predicate<FileWriteInfo> predicate)
    {
        List<FileWriteInfo> fileWriteInfos = update.getFileWriteInfos().stream().filter(predicate).collect(toImmutableList());
        PartitionUpdate replacementUpdate = update.withFileWriteInfos(fileWriteInfos);
        if (optimizedPartitionUpdateSerializationEnabled) {
            return wrappedBuffer(serializeZstdCompressed(partitionUpdateSmileCodec, replacementUpdate));
        }
        else {
            return wrappedBuffer(partitionUpdateCodec.toBytes(replacementUpdate));
        }
    }

    private SubPlan coalescePartitionFiles(
            ConnectorSession session,
            HiveWritableTableHandle writableTableHandle,
            PartitionUpdate partitionUpdate,
            long sizeLimit)
    {
        ConnectorTableLayoutHandle tableLayout = createConnectorTableLayoutHandle(session, writableTableHandle, partitionUpdate, sizeLimit);

        List<ColumnHandle> columnHandles = ImmutableList.copyOf(writableTableHandle.getInputColumns());
        HiveTableHandle tableHandle = new HiveTableHandle(writableTableHandle.getSchemaName(), writableTableHandle.getTableName());
        SubPlan subPlan = new SubPlan(tableLayout, columnHandles, tableHandle);
        return subPlan;
    }

    private ConnectorTableLayoutHandle createConnectorTableLayoutHandle(ConnectorSession session, HiveWritableTableHandle tableHandle, PartitionUpdate update, long sizeLimit)
    {
        Table table = tableHandle.getPageSinkMetadata().getTable().get();
        table = Table.builder(table).withStorage(storage -> storage.setLocation(update.getWritePath().toString())).build();
        HiveColumnHandle pathColumnHandle = HiveColumnHandle.pathColumnHandle();
        List<Slice> smallFilePaths = update.getFileWriteInfos().stream()
                .filter(info -> info.getFileWriteStatistics().isPresent() && info.getFileWriteStatistics().get().getFileSize() <= sizeLimit)
                .map(info -> utf8Slice(update.getWritePath().toString() + "/" + info.getTargetFileName())).collect(toList());
        Domain pathsDomain = multipleValues(VARCHAR, smallFilePaths);
        TupleDomain pathsTupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(new Subfield(pathColumnHandle.getName()), pathsDomain));
        List<HiveColumnHandle> partitionColumnHandles = getPartitionKeyColumnHandles(table);
        List<Type> partitionColumnTypes = partitionColumnHandles.stream().map(HiveColumnHandle::getTypeSignature).map(typeManager::getType).collect(toList());

        Map<String, String> partitionEncryptionParameters = ImmutableMap.of();
        if (tableHandle.getEncryptionInformation().isPresent()) {
            EncryptionInformation encryptionInformation = tableHandle.getEncryptionInformation().get();
            if (encryptionInformation.getDwrfEncryptionMetadata().isPresent()) {
                DwrfEncryptionMetadata dwrfEncryptionMetadata = encryptionInformation.getDwrfEncryptionMetadata().get();
                if (table.getPartitionColumns().isEmpty()) {
                    checkState(table.getParameters().entrySet().containsAll(dwrfEncryptionMetadata.getExtraMetadata().entrySet()), "Encryption parameters should be present");
                }
                else {
                    partitionEncryptionParameters = ImmutableMap.copyOf(dwrfEncryptionMetadata.getExtraMetadata());
                }
            }
        }

        HivePartition partition;
        if (!update.getName().isEmpty()) {
            HivePartition hivePartition = HivePartitionManager.parsePartition(tableHandle.getSchemaTableName(), update.getName(), partitionColumnHandles, partitionColumnTypes, timeZone);
            Partition metastorePartition = partitionObjectBuilder.buildUncommittedPartitionObject(
                    session,
                    table,
                    update,
                    prestoVersion,
                    partitionEncryptionParameters);
            partition = hivePartition.withMetastorePartition(metastorePartition);
        }
        else {
            partition = new HivePartition(tableHandle.getSchemaTableName());
        }

        HiveTableLayoutHandle handle = new HiveTableLayoutHandle(tableHandle.getSchemaTableName(),
                update.getWritePath().toString(),
                partitionColumnHandles,
                table.getDataColumns(),
                table.getParameters(),
                ImmutableList.of(partition),
                pathsTupleDomain,
                TRUE_CONSTANT,
                ImmutableMap.of(pathColumnHandle.getName(), pathColumnHandle),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                false,
                "Layout for small file coalescing",
                Optional.empty(),
                false,
                Optional.of(table));
        return handle;
    }

    private static boolean hasCoalescableFiles(PartitionUpdate update, long sizeLimit)
    {
        return update.getFileWriteInfos().stream().filter(info -> info.getFileWriteStatistics().isPresent() && info.getFileWriteStatistics().get().getFileSize() <= sizeLimit).count() > 1;
    }

    private static boolean partitionUpdateMissingStatistics(PartitionUpdate update)
    {
        return update.getFileWriteInfos().stream()
                .map(FileWriteInfo::getFileWriteStatistics)
                .anyMatch(Predicates.not(Optional::isPresent));
    }
}
