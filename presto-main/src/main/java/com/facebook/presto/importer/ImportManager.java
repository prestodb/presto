package com.facebook.presto.importer;

import com.facebook.presto.ingest.SerializedPartitionChunk;
import com.facebook.presto.metadata.Node;
import com.facebook.presto.metadata.NodeManager;
import com.facebook.presto.metadata.ShardManager;
import com.facebook.presto.spi.ImportClient;
import com.facebook.presto.split.ImportClientManager;
import com.facebook.presto.util.ShardBoundedExecutor;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpUriBuilder;
import io.airlift.http.client.Request;
import io.airlift.http.client.StatusResponseHandler.StatusResponse;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import org.skife.jdbi.v2.exceptions.UnableToExecuteStatementException;

import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response.Status;

import java.net.URI;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.importer.ImportField.sourceColumnHandleGetter;
import static com.facebook.presto.metadata.ImportColumnHandle.columnNameGetter;
import static com.facebook.presto.util.RetryDriver.retry;
import static com.facebook.presto.util.Threads.threadsNamed;
import static com.google.common.base.Functions.compose;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Lists.transform;
import static io.airlift.http.client.JsonBodyGenerator.jsonBodyGenerator;
import static io.airlift.http.client.Request.Builder.prepareDelete;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.Request.Builder.preparePut;
import static io.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;

public class ImportManager
{
    private static final Logger log = Logger.get(ImportManager.class);

    private final ExecutorService partitionExecutor = newFixedThreadPool(50, threadsNamed("import-partition-%s"));
    private final ShardBoundedExecutor<PartitionMarker> partitionBoundedExecutor = new ShardBoundedExecutor<>(partitionExecutor);
    private final ScheduledExecutorService chunkExecutor = newScheduledThreadPool(50, threadsNamed("import-chunk-%s"));
    private final ShardBoundedExecutor<Long> chunkBoundedExecutor = new ShardBoundedExecutor<>(chunkExecutor);
    private final ScheduledExecutorService shardExecutor = newScheduledThreadPool(50, threadsNamed("import-shard-%s"));
    private final PartitionOperationTracker partitionOperationTracker = new PartitionOperationTracker();

    private final ImportClientManager importClientManager;
    private final ShardManager shardManager;
    private final NodeWorkerQueue nodeWorkerQueue;
    private final HttpClient httpClient;
    private final JsonCodec<ShardImport> shardImportCodec;
    private final NodeManager nodeManager;

    @Inject
    public ImportManager(
            ImportClientManager importClientManager,
            ShardManager shardManager,
            NodeWorkerQueue nodeWorkerQueue,
            @ForImportManager HttpClient httpClient,
            JsonCodec<ShardImport> shardImportCodec,
            NodeManager nodeManager)
    {
        this.importClientManager = checkNotNull(importClientManager, "importClientFactory is null");
        this.shardManager = checkNotNull(shardManager, "shardManager is null");
        this.nodeWorkerQueue = checkNotNull(nodeWorkerQueue, "nodeWorkerQueue is null");
        this.httpClient = checkNotNull(httpClient, "httpClient is null");
        this.shardImportCodec = checkNotNull(shardImportCodec, "shardImportCodec");
        this.nodeManager = checkNotNull(nodeManager, "nodeManager is null");
    }

    public synchronized ListenableFuture<?> importTable(long tableId, final String sourceName, final String databaseName, final String tableName, List<ImportField> fields)
    {
        checkNotNull(sourceName, "sourceName is null");
        checkNotNull(databaseName, "databaseName is null");
        checkNotNull(tableName, "tableName is null");
        checkNotNull(fields, "fields is null");
        checkArgument(!fields.isEmpty(), "fields is empty");

        shardManager.createImportTable(tableId, sourceName, databaseName, tableName);

        Set<String> activePartitions = retry().stopOnIllegalExceptions().runUnchecked(new Callable<Set<String>>()
        {
            @Override
            public Set<String> call()
                    throws Exception
            {
                ImportClient importClient = importClientManager.getClient(sourceName);
                return ImmutableSet.copyOf(importClient.getPartitionNames(databaseName, tableName));
            }
        });

        log.info("Found %d current remote partitions: table %d", activePartitions.size(), tableId);

        // TODO: handle minor race condition where getAllPartitions does not include pre-started partition import tasks
        Set<String> allPartitions = shardManager.getAllPartitions(tableId);
        Set<String> committedPartitions = shardManager.getCommittedPartitions(tableId);
        Set<String> uncommittedPartitions = Sets.difference(allPartitions, committedPartitions);
        Set<String> operatingPartitions = partitionOperationTracker.getOperatingPartitions(tableId);

        // Only repair failed partitions that are part of the active set
        Set<String> erroredPartitions = Sets.difference(uncommittedPartitions, operatingPartitions);
        Set<String> repairPartitions = Sets.intersection(erroredPartitions, activePartitions);
        log.info("Repairing %d failed partitions from before: table %d", repairPartitions.size(), tableId);

        Set<String> partitionsToAdd = Sets.difference(activePartitions, allPartitions);
        Set<String> partitionsToRemove = Sets.difference(allPartitions, activePartitions);

        ImmutableSet.Builder<ListenableFuture<?>> builder = ImmutableSet.builder();

        for (String partition : Iterables.concat(partitionsToRemove, repairPartitions)) {
            ListenableFutureTask<Void> dropFuture = ListenableFutureTask.create(new PartitionDropJob(tableId, partition), null);
            builder.add(dropFuture);
            partitionBoundedExecutor.execute(PartitionMarker.from(tableId, partition), dropFuture);
        }
        log.info("Dropping %d old partitions: table %d", partitionsToRemove.size(), tableId);

        List<String> columns = transform(fields, compose(columnNameGetter(), sourceColumnHandleGetter()));

        for (String partition : Iterables.concat(partitionsToAdd, repairPartitions)) {
            PartitionChunkSupplier supplier = new PartitionChunkSupplier(importClientManager, sourceName, databaseName, tableName, partition, columns);
            ListenableFutureTask<Void> importJob = ListenableFutureTask.create(new PartitionImportJob(tableId, sourceName, partition, supplier, fields), null);
            builder.add(importJob);

            partitionBoundedExecutor.execute(PartitionMarker.from(tableId, partition), importJob);
        }
        log.info("Loading %d new partitions: table %d", partitionsToAdd.size(), tableId);

        return Futures.allAsList(builder.build());
    }

    @PreDestroy
    public void stop()
    {
        partitionExecutor.shutdown();
        chunkExecutor.shutdown();
        shardExecutor.shutdown();
    }

    private class PartitionDropJob
            implements Runnable
    {
        private final long tableId;
        private final String partitionName;

        private PartitionDropJob(long tableId, String partitionName)
        {
            this.tableId = tableId;
            this.partitionName = partitionName;
        }

        @Override
        public void run()
        {
            Multimap<Long, String> shardNodes = shardManager.getShardNodes(tableId, partitionName);
            if (!shardNodes.isEmpty()) {
                PartitionMarker partitionMarker = PartitionMarker.from(tableId, partitionName);
                checkState(partitionOperationTracker.claimPartition(partitionMarker, shardNodes.size()));

                Map<String, Node> nodeMap = getNodeMap(nodeManager.getActiveNodes());
                for (Map.Entry<Long, String> shardNode : shardNodes.entries()) {
                    Node node = nodeMap.get(shardNode.getValue());
                    if (node != null) {
                        Long shardId = shardNode.getKey();
                        chunkBoundedExecutor.execute(shardId, new ShardDropJob(partitionMarker, shardId, node));
                    }
                }
            }
            shardManager.dropPartition(tableId, partitionName);
        }

        private Map<String, Node> getNodeMap(Set<Node> nodes)
        {
            return Maps.uniqueIndex(nodes, Node.getIdentifierFunction());
        }
    }

    private class ShardDropJob
            implements Runnable
    {
        private final PartitionMarker partitionMarker;
        private final long shardId;
        private final Node node;

        private ShardDropJob(PartitionMarker partitionMarker, long shardId, Node node)
        {
            this.partitionMarker = partitionMarker;
            this.shardId = shardId;
            this.node = node;
        }

        @Override
        public void run()
        {
            try {
                retry().stopOnIllegalExceptions().runUnchecked(new Callable<Void>()
                {
                    @Override
                    public Void call()
                            throws Exception
                    {
                        if (!dropShardRequest(node)) {
                            throw new Exception("Failed to drop shard for " + node);
                        }
                        return null;
                    }
                });
            }
            catch (Exception e) {
                log.error(e);
            }
            finally {
                partitionOperationTracker.decrementActiveTasks(partitionMarker);
            }
        }

        private boolean dropShardRequest(Node node)
        {
            URI shardUri = uriAppendPaths(node.getHttpUri(), "/v1/shard/" + shardId);
            Request request = prepareDelete().setUri(shardUri).build();

            StatusResponse response;
            try {
                response = httpClient.execute(request, createStatusResponseHandler());
            }
            catch (RuntimeException e) {
                log.warn("drop request failed: %s. Cause: %s", shardId, e.getMessage());
                return false;
            }

            if (response.getStatusCode() != Status.ACCEPTED.getStatusCode()) {
                log.warn("unexpected response status: %s: %s", shardId, response.getStatusCode());
                return false;
            }

            log.debug("initiated drop shard: %s", shardId);
            return true;
        }
    }

    private class PartitionImportJob
            implements Runnable
    {
        private final long tableId;
        private final List<ImportField> fields;
        private final String sourceName;
        private final String partitionName;
        private final PartitionChunkSupplier supplier;

        public PartitionImportJob(long tableId, String sourceName, String partitionName, PartitionChunkSupplier supplier, List<ImportField> fields)
        {
            checkArgument(tableId > 0, "tableId must be greater than zero");
            this.tableId = tableId;
            this.sourceName = checkNotNull(sourceName, "sourceName is null");
            this.partitionName = checkNotNull(partitionName, "partitionName is null");
            this.supplier = checkNotNull(supplier, "supplier is null");
            this.fields = ImmutableList.copyOf(checkNotNull(fields, "fields is null"));
        }

        @Override
        public void run()
        {
            // TODO: add throttling based on size of chunk job queue
            Iterable<SerializedPartitionChunk> chunks = supplier.get();
            List<Long> shardIds = shardManager.createImportPartition(tableId, partitionName, chunks);
            PartitionMarker partitionMarker = PartitionMarker.from(tableId, partitionName);
            checkState(partitionOperationTracker.claimPartition(partitionMarker, shardIds.size()));

            log.debug("retrieved partition chunks: %s: %s: %s", partitionName, shardIds.size(), shardIds);

            Iterator<Long> shardIdIter = shardIds.iterator();
            for (SerializedPartitionChunk chunk : chunks) {
                checkState(shardIdIter.hasNext());
                Long shardId = shardIdIter.next();
                chunkBoundedExecutor.execute(shardId, new ChunkImportJob(partitionMarker, sourceName, shardId, chunk, fields));
            }
        }
    }

    private class ChunkImportJob
            implements Runnable
    {
        private final PartitionMarker partitionMarker;
        private final long shardId;
        private final ShardImport shardImport;

        public ChunkImportJob(PartitionMarker partitionMarker, String sourceName, long shardId, SerializedPartitionChunk chunk, List<ImportField> fields)
        {
            this.partitionMarker = partitionMarker;
            this.shardId = shardId;
            this.shardImport = new ShardImport(sourceName, chunk, fields);
        }

        @Override
        public void run()
        {
            try {
                retry().stopOnIllegalExceptions().runUnchecked(new Callable<Void>()
                {
                    @Override
                    public Void call()
                            throws Exception
                    {
                        log.debug("acquiring worker for shard: %s", shardId);
                        final Node worker;
                        try {
                            worker = nodeWorkerQueue.acquireNodeWorker();
                        }
                        catch (InterruptedException e) {
                            log.warn("interrupted while acquiring node worker");
                            Thread.currentThread().interrupt();
                            return null;
                        }
                        log.debug("acquired worker for shard: %s: %s", shardId, worker);
                        if (!initiateShardCreation(worker)) {
                            nodeWorkerQueue.releaseNodeWorker(worker);
                            throw new RuntimeException("Failed to initiate shard creation on " + worker);
                        }
                        return null;
                    }
                });
            }
            catch (Exception e) {
                partitionOperationTracker.decrementActiveTasks(partitionMarker);
                log.error(e);
            }
        }

        private boolean initiateShardCreation(Node worker)
        {
            URI shardUri = uriAppendPaths(worker.getHttpUri(), "/v1/shard/" + shardId);
            Request request = preparePut()
                    .setUri(shardUri)
                    .setHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
                    .setBodyGenerator(jsonBodyGenerator(shardImportCodec, shardImport))
                    .build();

            StatusResponse response;
            try {
                response = httpClient.execute(request, createStatusResponseHandler());
            }
            catch (RuntimeException e) {
                log.warn("request failed: %s. Cause: %s", shardId, e.getMessage());
                return false;
            }

            if (response.getStatusCode() != Status.ACCEPTED.getStatusCode()) {
                log.warn("unexpected response status: %s: %s", shardId, response.getStatusCode());
                return false;
            }

            log.debug("initiated shard creation: %s", shardId);
            shardExecutor.schedule(new ShardMonitorJob(partitionMarker, shardId, worker), 1, TimeUnit.SECONDS);
            return true;
        }
    }

    private class ShardMonitorJob
            implements Runnable
    {
        private final PartitionMarker partitionMarker;
        private final long shardId;
        private final Node worker;

        private ShardMonitorJob(PartitionMarker partitionMarker, long shardId, Node worker)
        {
            this.partitionMarker = partitionMarker;
            this.shardId = shardId;
            this.worker = checkNotNull(worker, "worker is null");
        }

        @Override
        public void run()
        {
            if (shardProcessed()) {
                nodeWorkerQueue.releaseNodeWorker(worker);
                partitionOperationTracker.decrementActiveTasks(partitionMarker);
            }
            else {
                shardExecutor.schedule(this, 1, TimeUnit.SECONDS);
            }
        }

        private boolean shardProcessed()
        {
            Status status;
            try {
                status = shardStatus();
            }
            catch (RuntimeException e) {
                log.warn("Failed to get shard status: %s. Cause: %s", shardId, e.getMessage());
                return false;
            }

            switch (status) {
                case ACCEPTED:
                    // Still in progress
                    return false;

                case OK:
                    // Shard complete
                    try {
                        shardManager.commitShard(shardId, worker.getNodeIdentifier());
                    }
                    catch (UnableToExecuteStatementException e) {
                        log.warn("Shard commit error: %s. Cause: %s", shardId, e.getMessage());
                        return false;
                    }

                    log.info("shard imported: %s", shardId);
                    return true;

                case NOT_FOUND:
                    // Node import error
                    log.info("shard import error or was dropped before commit: %s", shardId);
                    return true;

                default:
                    throw new AssertionError("Unknown shard status: " + status);
            }
        }

        private Status shardStatus()
        {
            URI shardUri = uriAppendPaths(worker.getHttpUri(), "/v1/shard/" + shardId);
            Request request = prepareGet().setUri(shardUri).build();
            StatusResponse response = httpClient.execute(request, createStatusResponseHandler());
            Status status = Status.fromStatusCode(response.getStatusCode());
            log.debug("shard status: %s: %s", shardId, status);
            return status;
        }
    }

    private static URI uriAppendPaths(URI uri, String path, String... additionalPaths)
    {
        HttpUriBuilder builder = HttpUriBuilder.uriBuilderFrom(uri);
        builder.appendPath(path);
        for (String additionalPath : additionalPaths) {
            builder.appendPath(additionalPath);
        }
        return builder.build();
    }
}
