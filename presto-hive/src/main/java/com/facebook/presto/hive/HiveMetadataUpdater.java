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

import com.facebook.airlift.concurrent.MoreFutures;
import com.facebook.presto.spi.ConnectorMetadataUpdateHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.connector.ConnectorMetadataUpdater;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.SettableFuture;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;

import static java.util.Objects.requireNonNull;

// TODO: Revisit and make this class more robust
public class HiveMetadataUpdater
        implements ConnectorMetadataUpdater
{
    private final Executor boundedExecutor;

    // Stores writerIndex <-> requestId mapping
    private final Map<Integer, UUID> writerRequestMap = new ConcurrentHashMap<>();

    // Stores requestId <-> fileNameFuture mapping
    private final Map<UUID, SettableFuture<String>> requestFutureMap = new ConcurrentHashMap<>();

    // Queue of pending requests
    private final Queue<HiveMetadataUpdateHandle> hiveMetadataRequestQueue = new ConcurrentLinkedQueue<>();

    HiveMetadataUpdater(Executor boundedExecutor)
    {
        this.boundedExecutor = requireNonNull(boundedExecutor, "boundedExecutor is null");
    }

    @Override
    public List<ConnectorMetadataUpdateHandle> getPendingMetadataUpdateRequests()
    {
        ImmutableList.Builder<ConnectorMetadataUpdateHandle> result = ImmutableList.builder();
        for (HiveMetadataUpdateHandle request : hiveMetadataRequestQueue) {
            result.add(request);
        }
        return result.build();
    }

    @Override
    public void setMetadataUpdateResults(List<ConnectorMetadataUpdateHandle> results)
    {
        boundedExecutor.execute(() -> updateResultAsync(results));
    }

    private void updateResultAsync(List<ConnectorMetadataUpdateHandle> results)
    {
        for (ConnectorMetadataUpdateHandle connectorMetadataUpdateHandle : results) {
            HiveMetadataUpdateHandle updateResult = (HiveMetadataUpdateHandle) connectorMetadataUpdateHandle;
            UUID requestId = updateResult.getRequestId();

            if (!requestFutureMap.containsKey(requestId)) {
                continue;
            }

            Optional<String> fileName = updateResult.getMetadataUpdate();
            if (fileName.isPresent()) {
                // remove the request from queue
                hiveMetadataRequestQueue.removeIf(metadataUpdateRequest -> metadataUpdateRequest.getRequestId().equals(requestId));

                // Set the fileName future
                requestFutureMap.get(requestId).set(fileName.get());
            }
        }
    }

    public void addMetadataUpdateRequest(String schemaName, String tableName, Optional<String> partitionName, int writerIndex)
    {
        UUID requestId = UUID.randomUUID();
        requestFutureMap.put(requestId, SettableFuture.create());
        writerRequestMap.put(writerIndex, requestId);

        // create a request and add it to the queue
        hiveMetadataRequestQueue.add(new HiveMetadataUpdateHandle(requestId, new SchemaTableName(schemaName, tableName), partitionName, Optional.empty()));
    }

    public void removeResultFuture(int writerIndex)
    {
        UUID requestId = writerRequestMap.get(writerIndex);
        requestFutureMap.remove(requestId);
        writerRequestMap.remove(writerIndex);
    }

    public CompletableFuture<String> getMetadataResult(int writerIndex)
    {
        UUID requestId = writerRequestMap.get(writerIndex);
        return MoreFutures.toCompletableFuture(requestFutureMap.get(requestId));
    }
}
