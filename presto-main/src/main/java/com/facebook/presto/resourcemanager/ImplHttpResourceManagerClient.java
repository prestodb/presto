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
package com.facebook.presto.resourcemanager;

import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.http.client.HttpUriBuilder;
import com.facebook.airlift.http.client.Request;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.execution.resourceGroups.ResourceGroupRuntimeInfo;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.server.BasicQueryInfo;
import com.facebook.presto.server.NodeStatus;
import com.facebook.presto.spi.memory.ClusterMemoryPoolInfo;
import com.facebook.presto.spi.memory.MemoryPoolId;
import com.facebook.presto.util.RetryRunner;
import jakarta.inject.Inject;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;

import static com.facebook.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static com.facebook.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static com.facebook.airlift.http.client.Request.Builder.prepareGet;
import static com.facebook.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static com.facebook.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;
import static java.util.Objects.requireNonNull;

/**
 * HTTP-based implementation of ResourceManagerClient.
 * Communicates with Resource Manager via HTTP REST endpoints.
 */
public class ImplHttpResourceManagerClient
        implements HttpResourceManagerClient
{
    private static final Logger log = Logger.get(ImplHttpResourceManagerClient.class);

    private final HttpClient httpClient;
    private final JsonCodec<NodeStatus> nodeStatusCodec;
    private final JsonCodec<BasicQueryInfo> basicQueryInfoCodec;
    private final JsonCodec<List<ResourceGroupRuntimeInfo>> resourceGroupRuntimeInfoListCodec;
    private final JsonCodec<Map<MemoryPoolId, ClusterMemoryPoolInfo>> memoryPoolInfoCodec;
    private final JsonCodec<Integer> integerCodec;
    private final InternalNodeManager internalNodeManager;
    private final RetryRunner retryRunner;

    @Inject
    public ImplHttpResourceManagerClient(
            @ForResourceManager HttpClient httpClient,
            @ForResourceManager ScheduledExecutorService executor,
            JsonCodec<NodeStatus> nodeStatusCodec,
            JsonCodec<BasicQueryInfo> basicQueryInfoCodec,
            JsonCodec<List<ResourceGroupRuntimeInfo>> resourceGroupRuntimeInfoListCodec,
            JsonCodec<Map<MemoryPoolId, ClusterMemoryPoolInfo>> memoryPoolInfoCodec,
            InternalNodeManager internalNodeManager)
    {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.nodeStatusCodec = requireNonNull(nodeStatusCodec, "nodeStatusCodec is null");
        this.basicQueryInfoCodec = requireNonNull(basicQueryInfoCodec, "basicQueryInfoCodec is null");
        this.resourceGroupRuntimeInfoListCodec = requireNonNull(resourceGroupRuntimeInfoListCodec, "resourceGroupRuntimeInfoListCodec is null");
        this.memoryPoolInfoCodec = requireNonNull(memoryPoolInfoCodec, "memoryPoolInfoCodec is null");
        this.integerCodec = JsonCodec.jsonCodec(Integer.class);
        this.internalNodeManager = requireNonNull(internalNodeManager, "internalNodeManager is null");
        this.retryRunner = new RetryRunner(executor);
    }

    @Override
    public void queryHeartbeat(Optional<URI> target, String internalNode, BasicQueryInfo basicQueryInfo, long sequenceId)
    {
        URI uri = buildUri("/v1/resource-manager/nodes/" + internalNode + "/queries/" + basicQueryInfo.getQueryId().toString(), target);

        Request request = new Request.Builder()
                .setMethod("PATCH")
                .setUri(uri)
                .setHeader("Content-Type", APPLICATION_JSON)
                .setHeader("Sequence-Id", String.valueOf(sequenceId))
                .setBodyGenerator(createStaticBodyGenerator(basicQueryInfoCodec.toJsonBytes(basicQueryInfo)))
                .build();

        retryRunner.runWithRetry(() -> httpClient.execute(request, createStatusResponseHandler()), true);
    }

    @Override
    public List<ResourceGroupRuntimeInfo> getResourceGroupInfo(Optional<URI> target, String excludingNode)
            throws ResourceManagerInconsistentException
    {
        URI uri = buildUri(
                "/v1/resource-manager/resource-groups",
                target,
                "excludingNode",
                excludingNode);

        Request request = prepareGet()
                .setUri(uri)
                .setHeader("Accept", APPLICATION_JSON)
                .build();

        try {
            List<ResourceGroupRuntimeInfo> result =
                    httpClient.execute(
                            request,
                            createJsonResponseHandler(resourceGroupRuntimeInfoListCodec));
            return result;
        }
        catch (Exception e) {
            log.error(e);
            throw e;
        }
    }

    @Override
    public void nodeHeartbeat(Optional<URI> target, NodeStatus nodeStatus)
    {
        URI uri = buildUri("/v1/resource-manager/node/" + nodeStatus.getNodeId(), target);

        Request request = new Request.Builder()
                .setMethod("PATCH")
                .setUri(uri)
                .setHeader("Content-Type", APPLICATION_JSON)
                .setBodyGenerator(createStaticBodyGenerator(nodeStatusCodec.toJsonBytes(nodeStatus)))
                .build();

        retryRunner.runWithRetry(() -> httpClient.execute(request, createStatusResponseHandler()), true);
    }

    @Override
    public Map<MemoryPoolId, ClusterMemoryPoolInfo> getMemoryPoolInfo(Optional<URI> target)
    {
        URI uri = buildUri("/v1/resource-manager/memory-pools", target);

        Request request = prepareGet()
                .setUri(uri)
                .setHeader("Accept", APPLICATION_JSON)
                .build();

        return retryRunner.runWithRetry(() -> httpClient.execute(request, createJsonResponseHandler(memoryPoolInfoCodec)), true);
    }

    @Override
    public void resourceGroupRuntimeHeartbeat(Optional<URI> target, String nodeId, List<ResourceGroupRuntimeInfo> resourceGroupRuntimeInfo)
    {
        URI uri = buildUri("/v1/resource-manager/nodes/" + nodeId + "/resource-groups", target);

        Request request = new Request.Builder()
                .setMethod("PATCH")
                .setUri(uri)
                .setHeader("Content-Type", APPLICATION_JSON)
                .setBodyGenerator(createStaticBodyGenerator(resourceGroupRuntimeInfoListCodec.toJsonBytes(resourceGroupRuntimeInfo)))
                .build();

        retryRunner.runWithRetry(() -> httpClient.execute(request, createStatusResponseHandler()), true);
    }

    @Override
    public int getRunningTaskCount(Optional<URI> target)
    {
        URI uri = buildUri("/v1/resource-manager/tasks/count", target, "state", "running");

        Request request = prepareGet()
                .setUri(uri)
                .setHeader("Accept", APPLICATION_JSON)
                .build();

        return httpClient.execute(request, createJsonResponseHandler(integerCodec));
    }

    private URI buildUri(String path, Optional<URI> target, String... parameters)
    {
        URI uri = target.isPresent() ? target.get() : getResourceManagerURI();

        HttpUriBuilder uriBuilder = uriBuilderFrom(uri)
                .appendPath(path);
        if (parameters.length % 2 != 0) {
            throw new IllegalArgumentException("Parameters must be in key/value pairs");
        }

        for (int i = 0; i < parameters.length; i += 2) {
            uriBuilder.addParameter(parameters[i], parameters[i + 1]);
        }
        return uriBuilder.build();
    }

    private URI getResourceManagerURI()
    {
        List<URI> resourceManagers = internalNodeManager.getResourceManagers().stream()
                .map(InternalNode::getInternalUri)
                .collect(toImmutableList());

        if (resourceManagers.isEmpty()) {
            throw new ResourceManagerException("No resource manager found");
        }
        return resourceManagers.get(ThreadLocalRandom.current().nextInt(resourceManagers.size()));
    }
}
