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

package com.facebook.presto.hudi.split;

import com.facebook.airlift.concurrent.BoundedExecutor;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.Partition;
import com.facebook.presto.hive.metastore.StorageFormat;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.hive.util.AsyncQueue;
import com.facebook.presto.hudi.HudiColumnHandle;
import com.facebook.presto.hudi.HudiPartition;
import com.facebook.presto.hudi.HudiTableHandle;
import com.facebook.presto.hudi.HudiTableLayoutHandle;
import com.facebook.presto.hudi.partition.HiveStylePartitionValueExtractor;
import com.facebook.presto.hudi.partition.MultiPartKeysValueExtractor;
import com.facebook.presto.hudi.partition.NonPartitionedExtractor;
import com.facebook.presto.hudi.partition.PartitionValueExtractor;
import com.facebook.presto.hudi.partition.SinglePartPartitionValueExtractor;
import com.facebook.presto.hudi.query.HudiDirectoryLister;
import com.facebook.presto.hudi.query.index.HudiPartitionStatsIndexSupport;
import com.facebook.presto.hudi.query.index.IndexSupportFactory;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Streams;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.util.Lazy;

import java.util.ArrayList;
import java.util.Deque;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.facebook.airlift.concurrent.MoreFutures.addExceptionCallback;
import static com.facebook.presto.hive.metastore.MetastoreUtil.extractPartitionValues;
import static com.facebook.presto.hudi.HudiErrorCode.HUDI_CANNOT_OPEN_SPLIT;
import static com.facebook.presto.hudi.HudiErrorCode.HUDI_INVALID_METADATA;
import static com.facebook.presto.hudi.HudiMetadata.fromDataColumns;
import static com.facebook.presto.hudi.HudiSessionProperties.getSplitGeneratorParallelism;
import static com.facebook.presto.hudi.HudiSessionProperties.isMetadataPartitionListingEnabled;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * A runnable to load Hudi splits asynchronously in the background
 */
public class HudiBackgroundSplitLoader
        implements Runnable
{
    private static final Logger log = Logger.get(HudiBackgroundSplitLoader.class);
    public static final String DELIMITER_STR = "/";
    public static final String EQUALS_STR = "=";

    private final ConnectorSession session;
    private final HudiTableLayoutHandle layout;
    private final HudiDirectoryLister hudiDirectoryLister;
    private final AsyncQueue<ConnectorSplit> asyncQueue;
    private final Lazy<Map<String, Partition>> lazyPartitionMap;
    private final int splitGeneratorNumThreads;
    private final ExecutorService splitGeneratorExecutorService;
    private final boolean enableMetadataTable;
    private final Lazy<HoodieTableMetadata> lazyTableMetadata;
    private final Consumer<Throwable> errorListener;
    private final Optional<HudiPartitionStatsIndexSupport> partitionIndexSupportOpt;
    private final boolean isMetadataPartitionListingEnabled;

    public HudiBackgroundSplitLoader(
            ConnectorSession session,
            ExecutorService splitGeneratorExecutorService,
            HudiTableLayoutHandle layout,
            HudiDirectoryLister hudiDirectoryLister,
            AsyncQueue<ConnectorSplit> asyncQueue,
            Lazy<Map<String, Partition>> lazyPartitionMap,
            boolean enableMetadataTable,
            Lazy<HoodieTableMetadata> lazyTableMetadata,
            Consumer<Throwable> errorListener)
    {
        this.session = requireNonNull(session, "session is null");
        this.layout = requireNonNull(layout, "layout is null");
        this.hudiDirectoryLister = requireNonNull(hudiDirectoryLister, "hudiDirectoryLister is null");
        this.asyncQueue = requireNonNull(asyncQueue, "asyncQueue is null");
        this.lazyPartitionMap = requireNonNull(lazyPartitionMap, "partitions is null");

        this.splitGeneratorNumThreads = getSplitGeneratorParallelism(session);
        this.splitGeneratorExecutorService = requireNonNull(splitGeneratorExecutorService, "splitGeneratorExecutorService is null");
        this.enableMetadataTable = enableMetadataTable;
        this.errorListener = errorListener;
        this.partitionIndexSupportOpt = enableMetadataTable ?
                IndexSupportFactory.createPartitionStatsIndexSupport(layout.getTableHandle(), Lazy.lazily(() -> layout.getTableHandle().getMetaClient()), lazyTableMetadata, layout.getRegularPredicates(), session) : Optional.empty();
        this.isMetadataPartitionListingEnabled = isMetadataPartitionListingEnabled(session);
        this.lazyTableMetadata = lazyTableMetadata;
    }

    @Override
    public void run()
    {
        try {
            if (enableMetadataTable) {
                generateSplits(true);
                return;
            }

            // Fallback to partition pruning generator
            generateSplits(false);
        }
        catch (Exception e) {
            errorListener.accept(e);
        }
    }

    private void generateSplits(boolean useIndex) {
        // Attempt to apply partition pruning using partition stats index
        Deque<HudiPartition> partitionQueue = getPartitions(useIndex);
        if (partitionQueue.isEmpty()) {
            asyncQueue.finish();
            return;
        }

        List<HudiPartitionSplitGenerator> splitGenerators = new ArrayList<>();
        List<ListenableFuture<Void>> futures = new ArrayList<>();

        int splitGeneratorParallelism = Math.max(1, Math.min(splitGeneratorNumThreads, partitionQueue.size()));
        Executor splitGeneratorExecutor = new BoundedExecutor(splitGeneratorExecutorService, splitGeneratorParallelism);

        for (int i = 0; i < splitGeneratorParallelism; i++) {
            HudiPartitionSplitGenerator generator = new HudiPartitionSplitGenerator(
                    session, layout, hudiDirectoryLister, lazyPartitionMap.get(), asyncQueue, partitionQueue, useIndex);
            splitGenerators.add(generator);
            ListenableFuture<Void> future = Futures.submit(generator, splitGeneratorExecutor);
            addExceptionCallback(future, errorListener);
            futures.add(future);
        }

        // Signal all generators to stop once partition queue is drained
        splitGenerators.forEach(HudiPartitionSplitGenerator::stopRunning);

        log.info("Wait for partition pruning split generation to finish on table %s.%s", layout.getTableHandle().getSchemaName(), layout.getTableHandle().getTableName());
        try {
            Futures.whenAllComplete(futures)
                    .run(asyncQueue::finish, directExecutor())
                    .get();
            log.info("Partition pruning split generation finished on table %s.%s", layout.getTableHandle().getSchemaName(), layout.getTableHandle().getTableName());
        }
        catch (InterruptedException | ExecutionException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new PrestoException(HUDI_CANNOT_OPEN_SPLIT, "Error generating Hudi split", e);
        }
    }

    private Deque<HudiPartition> getPartitions(boolean useIndex)
    {
        Map<String, Partition> metadataPartitions;
        HudiTableHandle tableHandle = layout.getTableHandle();
        if (enableMetadataTable && isMetadataPartitionListingEnabled) {
            try {
                PartitionValueExtractor partitionValueExtractor = getPartitionValueExtractor(tableHandle.getMetaClient().getTableConfig());
                List<HudiColumnHandle> hudiPartitionColumns = layout.getPartitionColumns();

                List<Column> partitionColumns = hudiPartitionColumns.stream()
                        .map(column -> new Column(
                                column.getName(),
                                column.getHiveType(),
                                column.getComment(),
                                Optional.empty()))
                        .toList();
                log.info("Listing partitions for %s.%s via metadata table using partition value extractor %s",
                        tableHandle.getSchemaName(), tableHandle.getTableName(), partitionValueExtractor.getClass().getSimpleName());
                metadataPartitions = lazyTableMetadata.get()
                        .getAllPartitionPaths().stream()
                        .map(partitionPath -> buildPartition(partitionPath, partitionColumns, tableHandle, partitionValueExtractor))
                        .collect(Collectors.toMap(
                                this::getHivePartitionName,
                                Function.identity()));

                if (metadataPartitions.isEmpty()) {
                    log.warn("No partitions found via metadata table for %s.%s, switching to metastore-based listing.",
                            tableHandle.getSchemaName(), tableHandle.getTableName());
                    metadataPartitions = lazyPartitionMap.get();
                }
            }
            catch (Exception e) {
                log.error(e, "Failed to get partitions from metadata table %s.%s, falling back to metastore based partition listing",
                        tableHandle.getSchemaName(), tableHandle.getTableName());
                metadataPartitions = lazyPartitionMap.get();
            }
        }
        else {
            metadataPartitions = lazyPartitionMap.get();
        }

        Set<String> allPartitions = metadataPartitions.keySet();
        Stream<String> effectivePartitions = Optional.ofNullable(useIndex && partitionIndexSupportOpt.isPresent()
                ? partitionIndexSupportOpt.get().prunePartitions(allPartitions).orElse(null)
                : null).orElse(allPartitions.stream());

        return effectivePartitions
                .map(partitionName -> getHudiPartition(layout, partitionName))
                .collect(Collectors.toCollection(ConcurrentLinkedDeque::new));
    }

    private HudiPartition getHudiPartition(HudiTableLayoutHandle tableLayout, String partitionName)
    {
        String databaseName = tableLayout.getTableHandle().getSchemaName();
        String tableName = tableLayout.getTableHandle().getTableName();
        List<HudiColumnHandle> partitionColumns = tableLayout.getPartitionColumns();
        Path tablePath = new Path(layout.getTableHandle().getPath());
        if (partitionColumns.isEmpty()) {
            // non-partitioned tableLayout
            Table metastoreTable = Optional.ofNullable(layout.getTableHandle().getTable())
                    .orElseThrow(() -> new PrestoException(HUDI_INVALID_METADATA, format("Table %s.%s expected but not found", databaseName, tableName)));
            Path partitionPath = new Path(metastoreTable.getStorage().getLocation());
            String relativePartitionPath = FSUtils.getRelativePartitionPath(new StoragePath(tablePath.toUri()), new StoragePath(partitionPath.toUri()));
            return new HudiPartition(partitionName, ImmutableList.of(), ImmutableMap.of(), metastoreTable.getStorage(), tableLayout.getDataColumns(), relativePartitionPath);
        }
        else {
            // partitioned tableLayout
            List<String> partitionValues = extractPartitionValues(partitionName);
            checkArgument(partitionColumns.size() == partitionValues.size(),
                    format("Invalid partition name %s for partition columns %s", partitionName, partitionColumns));
            Partition partition = Optional.ofNullable(lazyPartitionMap.get().get(partitionName))
                    .orElseThrow(() -> new PrestoException(HUDI_INVALID_METADATA, format("Partition %s expected but not found", partitionName)));
            Map<String, String> keyValues = zipPartitionKeyValues(partitionColumns, partitionValues);
            Path partitionPath = new Path(partition.getStorage().getLocation());
            String relativePartitionPath = FSUtils.getRelativePartitionPath(new StoragePath(tablePath.toUri()), new StoragePath(partitionPath.toUri()));
            return new HudiPartition(partitionName, partitionValues, keyValues, partition.getStorage(), fromDataColumns(partition.getColumns()), relativePartitionPath);
        }
    }

    private Map<String, String> zipPartitionKeyValues(List<HudiColumnHandle> partitionColumns, List<String> partitionValues)
    {
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        Streams.forEachPair(partitionColumns.stream(), partitionValues.stream(),
                (column, value) -> builder.put(column.getName(), value));
        return builder.build();
    }

    private Partition buildPartition(String partitionPath, List<Column> partitionColumns, HudiTableHandle tableHandle, PartitionValueExtractor partitionValueExtractor)
    {
        if (partitionPath == null || partitionPath.isEmpty()) {
            return Partition.builder()
                    .setDatabaseName(tableHandle.getSchemaName())
                    .setTableName(tableHandle.getTableName())
                    .withStorage(storageBuilder ->
                            storageBuilder.setLocation(layout.getTableHandle().getPath())
                                    .setStorageFormat(StorageFormat.VIEW_STORAGE_FORMAT))
                    .setColumns(ImmutableList.of())
                    .setValues(ImmutableList.of())
                    .build();
        }

        List<String> values = partitionValueExtractor.extractPartitionValuesInPath(partitionPath).stream().map(this::unescapePathName).toList();
        if (partitionColumns.size() != values.size()) {
            throw new HoodieException("Cannot extract partition values from partition path: " + partitionPath);
        }

        return Partition.builder()
                .setDatabaseName(tableHandle.getSchemaName())
                .setTableName(tableHandle.getTableName())
                .withStorage(storageBuilder ->
                        storageBuilder.setLocation(getFullPath(layout.getTableHandle().getPath(), partitionPath))
                                // Storage format is unused by the Hudi connector
                                .setStorageFormat(StorageFormat.VIEW_STORAGE_FORMAT))
                .setColumns(partitionColumns)
                .setValues(values)
                .build();
    }

    private String getFullPath(String basePath, String relativePartitionPath)
    {
        return basePath.endsWith(DELIMITER_STR)
                ? basePath + relativePartitionPath
                : basePath + DELIMITER_STR + relativePartitionPath;
    }

    String getHivePartitionName(Partition partition)
    {
        List<Column> columns = partition.getColumns();
        List<String> values = partition.getValues();

        return IntStream.range(0, columns.size())
                .mapToObj(i -> columns.get(i).getName() + EQUALS_STR + values.get(i))
                .collect(Collectors.joining(DELIMITER_STR));
    }

    private PartitionValueExtractor getPartitionValueExtractor(HoodieTableConfig tableConfig)
    {
        Option<String[]> partitionFieldsOpt = tableConfig.getPartitionFields();

        if (!partitionFieldsOpt.isPresent()) {
            return new NonPartitionedExtractor();
        }

        String[] partitionFields = partitionFieldsOpt.get();
        if (partitionFields.length == 1) {
            return Boolean.parseBoolean(tableConfig.getHiveStylePartitioningEnable()) ?
                    new HiveStylePartitionValueExtractor() : new SinglePartPartitionValueExtractor();
        }

        return new MultiPartKeysValueExtractor();
    }

    private String unescapePathName(String path)
    {
        // fast path, no escaped characters and therefore no copying necessary
        int escapedAtIndex = path.indexOf('%');
        if (escapedAtIndex < 0 || escapedAtIndex + 2 >= path.length()) {
            return path;
        }

        // slow path, unescape into a new string copy
        StringBuilder sb = new StringBuilder();
        int fromIndex = 0;
        while (escapedAtIndex >= 0 && escapedAtIndex + 2 < path.length()) {
            // preceding sequence without escaped characters
            if (escapedAtIndex > fromIndex) {
                sb.append(path, fromIndex, escapedAtIndex);
            }
            // try to parse the to digits after the percent sign as hex
            try {
                int code = HexFormat.fromHexDigits(path, escapedAtIndex + 1, escapedAtIndex + 3);
                sb.append((char) code);
                // advance past the percent sign and both hex digits
                fromIndex = escapedAtIndex + 3;
            }
            catch (NumberFormatException e) {
                // invalid escape sequence, only advance past the percent sign
                sb.append('%');
                fromIndex = escapedAtIndex + 1;
            }
            // find next escaped character
            escapedAtIndex = path.indexOf('%', fromIndex);
        }
        // trailing sequence without escaped characters
        if (fromIndex < path.length()) {
            sb.append(path, fromIndex, path.length());
        }
        return sb.toString();
    }
}
