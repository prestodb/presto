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
package com.facebook.presto.metadata;

import com.facebook.presto.util.KeyBoundedExecutor;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import com.google.inject.Inject;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.HttpUriBuilder;
import io.airlift.http.client.Request;
import io.airlift.http.client.Request.Builder;
import io.airlift.http.client.StatusResponseHandler.StatusResponse;
import io.airlift.log.Logger;
import io.airlift.units.Duration;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.metadata.Node.getIdentifierFunction;
import static com.facebook.presto.util.Threads.daemonThreadsNamed;
import static com.google.common.base.Preconditions.checkNotNull;
import static io.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

public class ShardCleaner
{
    private static final Logger log = Logger.get(ShardCleaner.class);

    private final NodeManager nodeManager;
    private final ShardManager shardManager;
    private final HttpClient httpClient;

    private final Duration interval;

    private final boolean enabled;
    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicBoolean stopped = new AtomicBoolean();

    private final ScheduledExecutorService executorService = newSingleThreadScheduledExecutor(daemonThreadsNamed("shard-cleaner-%s"));
    private final AtomicReference<ScheduledFuture<?>> scheduledFuture = new AtomicReference<>();

    private final KeyBoundedExecutor<String> nodeBoundedExecutor;

    @Inject
    public ShardCleaner(NodeManager nodeManager,
            ShardManager shardManager,
            @ForShardCleaner HttpClient httpClient,
            ShardCleanerConfig config)
    {
        this.nodeManager = checkNotNull(nodeManager, "nodeManager is null");
        this.shardManager = checkNotNull(shardManager, "shardManager is null");
        this.httpClient = checkNotNull(httpClient, "httpClient is null");

        checkNotNull(config, "config is null");

        this.interval = config.getCleanerInterval();

        ExecutorService nodeExecutor = newScheduledThreadPool(config.getMaxThreads(), daemonThreadsNamed("shard-cleaner-worker-%s"));
        this.nodeBoundedExecutor = new KeyBoundedExecutor<>(nodeExecutor, config.getMaxThreads());
        this.enabled = config.isEnabled();
    }

    @PostConstruct
    public void start()
    {
        if (enabled) {
            if (started.compareAndSet(false, true)) {
                this.scheduledFuture.set(executorService.scheduleAtFixedRate(new ShardCleanerRunnable(),
                        interval.toMillis(), interval.toMillis(), TimeUnit.MILLISECONDS));
            }
        }
    }

    @PreDestroy
    public void stop()
    {
        if (!stopped.compareAndSet(false, true)) {
            executorService.shutdownNow();
        }
    }

    private class ShardCleanerRunnable
            implements Runnable
    {
        @Override
        public void run()
        {
            try {
                Map<String, Node> activeNodes = Maps.uniqueIndex(nodeManager.getAllNodes().getActiveNodes(), getIdentifierFunction());
                Iterable<String> shardNodes = shardManager.getAllNodesInUse();

                ImmutableList.Builder<ListenableFuture<Void>> builder = ImmutableList.builder();

                for (String nodeIdentifier : shardNodes) {
                    if (!activeNodes.keySet().contains(nodeIdentifier)) {
                        log.debug("Skipping Node %s, which is in the database but not active!", nodeIdentifier);
                        continue;
                    }

                    Iterable<Long> orphanedShards = shardManager.getOrphanedShardIds(Optional.of(nodeIdentifier));
                    Node node = activeNodes.get(nodeIdentifier);

                    for (Long shardId : orphanedShards) {
                        ListenableFutureTask<Void> task = ListenableFutureTask.create(new ShardDropJob(shardId, node), null);
                        nodeBoundedExecutor.execute(nodeIdentifier, task);
                        builder.add(task);
                    }
                }

                Future<List<Void>> allFutures = Futures.allAsList(builder.build());
                try {
                    allFutures.get();
                }
                catch (ExecutionException e) {
                    throw e.getCause();
                }

                // node run complete. Now vacuum out all the shards that are no longer on any local node.
                Iterable<Long> allOrphanedShards = shardManager.getOrphanedShardIds(Optional.<String>absent());
                for (Long shardId : allOrphanedShards) {
                    shardManager.dropShard(shardId);
                }

                shardManager.dropOrphanedPartitions();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            catch (Throwable e) {
                log.error(e, "Caught problem when dropping orphaned shards!");
            }
        }
    }

    private class ShardDropJob
            implements Runnable
    {
        private final long shardId;
        private final Node node;

        private ShardDropJob(long shardId, Node node)
        {
            this.shardId = shardId;
            this.node = node;
        }

        @Override
        public void run()
        {
            try {
                if (!dropShardRequest()) {
                    throw new RuntimeException("Failed to drop shard " + shardId + " for " + node);
                }
                shardManager.disassociateShard(shardId, node.getNodeIdentifier());
            }
            catch (RuntimeException e) {
                log.error(e);
            }
        }

        private boolean dropShardRequest()
        {
            URI shardUri = uriAppendPaths(node.getHttpUri(), "/v1/shard/" + shardId);
            Request request = Builder.prepareDelete().setUri(shardUri).build();

            StatusResponse response;
            try {
                response = httpClient.execute(request, createStatusResponseHandler());
            }
            catch (RuntimeException e) {
                log.warn("drop request failed: %s. Cause: %s", shardId, e.getMessage());
                return false;
            }

            if (response.getStatusCode() != HttpStatus.ACCEPTED.code()) {
                log.warn("unexpected response status: %s: %s", shardId, response.getStatusCode());
                return false;
            }

            log.debug("initiated drop shard: %s", shardId);
            return true;
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
