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
import com.facebook.airlift.http.client.Response;
import com.facebook.airlift.http.client.ResponseHandler;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.log.Logger;
import com.facebook.drift.client.address.AddressSelector;
import com.facebook.presto.execution.resourceGroups.ResourceGroupRuntimeInfo;
import com.facebook.presto.server.BasicQueryInfo;
import com.facebook.presto.server.NodeStatus;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.memory.ClusterMemoryPoolInfo;
import com.facebook.presto.spi.memory.MemoryPoolId;
import com.google.inject.BindingAnnotation;
import jakarta.inject.Inject;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static com.facebook.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static com.facebook.airlift.http.client.Request.Builder.prepareGet;
import static com.facebook.airlift.http.client.Request.Builder.preparePut;
import static com.facebook.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static com.facebook.airlift.json.JsonCodec.listJsonCodec;
import static com.facebook.airlift.json.JsonCodec.mapJsonCodec;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;
import static java.lang.String.format;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.util.Objects.requireNonNull;

/**
 * HTTP-based implementation of ResourceManagerClient.
 * Communicates with Resource Manager via HTTP REST endpoints.
 */
public class HttpResourceManagerClient
{
    private static final Logger log = Logger.get(HttpResourceManagerClient.class);

    private final HttpClient httpClient;
    private final JsonCodec<NodeStatus> nodeStatusCodec;
    private final JsonCodec<BasicQueryInfo> basicQueryInfoCodec;
    private final JsonCodec<List<ResourceGroupRuntimeInfo>> resourceGroupRuntimeInfoListCodec;
    private final JsonCodec<Map<MemoryPoolId, ClusterMemoryPoolInfo>> memoryPoolInfoCodec;
    private final JsonCodec<Integer> integerCodec;
    private final AddressSelector resourceManagerAddressSelector;
    private Optional<String> addressSelectorValue = Optional.empty();

    @Inject
    public HttpResourceManagerClient(
            @ForResourceManager HttpClient httpClient,
            JsonCodec<NodeStatus> nodeStatusCodec,
            JsonCodec<BasicQueryInfo> basicQueryInfoCodec,
            JsonCodec<List<ResourceGroupRuntimeInfo>> resourceGroupRuntimeInfoListCodec,
            JsonCodec<Map<MemoryPoolId, ClusterMemoryPoolInfo>> memoryPoolInfoCodec,
            @ForResourceManager AddressSelector resourceManagerAddress)
    {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.nodeStatusCodec = requireNonNull(nodeStatusCodec, "nodeStatusCodec is null");
        this.basicQueryInfoCodec = requireNonNull(basicQueryInfoCodec, "basicQueryInfoCodec is null");
        this.resourceGroupRuntimeInfoListCodec = requireNonNull(resourceGroupRuntimeInfoListCodec, "resourceGroupRuntimeInfoListCodec is null");
        this.memoryPoolInfoCodec = requireNonNull(memoryPoolInfoCodec, "memoryPoolInfoCodec is null");
        this.integerCodec = JsonCodec.jsonCodec(Integer.class);
        this.resourceManagerAddressSelector = requireNonNull(resourceManagerAddress, "resourceManagerAddress is null");
    }

    public HttpResourceManagerClient(
            HttpClient httpClient,
            AddressSelector resourceManagerAddress)
    {
        this(
                httpClient,
                JsonCodec.jsonCodec(NodeStatus.class),
                JsonCodec.jsonCodec(BasicQueryInfo.class),
                listJsonCodec(ResourceGroupRuntimeInfo.class),
                mapJsonCodec(MemoryPoolId.class, ClusterMemoryPoolInfo.class),
                resourceManagerAddress);
    }

    public HttpResourceManagerClient setAddressSelectorContext(String value)
    {
        this.addressSelectorValue = Optional.of(value);
        return this;
    }

    public void queryHeartbeat(String internalNode, BasicQueryInfo basicQueryInfo, long sequenceId)
    {
        URI uri = buildUri("/v1/resourcemanager/queryHeartbeat",
                "nodeId", internalNode, "sequenceId", String.valueOf(sequenceId));

        Request request = preparePut()
                .setUri(uri)
                .setHeader("Content-Type", APPLICATION_JSON)
                .setBodyGenerator(createStaticBodyGenerator(basicQueryInfoCodec.toJsonBytes(basicQueryInfo)))
                .build();

        httpClient.execute(request, new VoidResponseHandler("queryHeartbeat"));
    }

    public List<ResourceGroupRuntimeInfo> getResourceGroupInfo(String excludingNode)
            throws ResourceManagerInconsistentException
    {
        URI uri = buildUri("/v1/resourcemanager/resourceGroupInfo",
                "excludingNode", excludingNode);

        Request request = prepareGet()
                .setUri(uri)
                .setHeader("Content-Type", APPLICATION_JSON)
                .build();

        return httpClient.execute(request, createJsonResponseHandler(resourceGroupRuntimeInfoListCodec));
    }

    public void nodeHeartbeat(NodeStatus nodeStatus)
    {
        URI uri = buildUri("/v1/resourcemanager/nodeHeartbeat");

        Request request = preparePut()
                .setUri(uri)
                .setHeader("Content-Type", APPLICATION_JSON)
                .setBodyGenerator(createStaticBodyGenerator(nodeStatusCodec.toJsonBytes(nodeStatus)))
                .build();

        httpClient.execute(request, new VoidResponseHandler("nodeHeartbeat"));
    }

    public Map<MemoryPoolId, ClusterMemoryPoolInfo> getMemoryPoolInfo()
    {
        URI uri = buildUri("/v1/resourcemanager/memoryPoolInfo");

        Request request = prepareGet()
                .setUri(uri)
                .setHeader("Content-Type", APPLICATION_JSON)
                .build();

        return httpClient.execute(request, createJsonResponseHandler(memoryPoolInfoCodec));
    }

    public void resourceGroupRuntimeHeartbeat(String node, List<ResourceGroupRuntimeInfo> resourceGroupRuntimeInfo)
    {
        URI uri = buildUri("/v1/resourcemanager/resourceGroupRuntimeHeartbeat", "node", node);

        Request request = preparePut()
                .setUri(uri)
                .setHeader("Content-Type", APPLICATION_JSON)
                .setBodyGenerator(createStaticBodyGenerator(resourceGroupRuntimeInfoListCodec.toJsonBytes(resourceGroupRuntimeInfo)))
                .build();

        httpClient.execute(request, new VoidResponseHandler("resourceGroupRuntimeHeartbeat"));
    }

    public int getRunningTaskCount()
    {
        URI uri = buildUri("/v1/resourcemanager/getRunningTaskCount");

        Request request = prepareGet()
                .setUri(uri)
                .setHeader("Content-Type", APPLICATION_JSON)
                .build();

        return httpClient.execute(request, createJsonResponseHandler(integerCodec));
    }

    private URI buildUri(String path, String... parameters)
    {
        HttpUriBuilder uriBuilder = uriBuilderFrom(URI.create("http://" + resourceManagerAddressSelector.selectAddress(addressSelectorValue).get()))
                .appendPath(path);

        if (parameters.length % 2 != 0) {
            throw new IllegalArgumentException("Parameters must be in key/value pairs");
        }

        for (int i = 0; i < parameters.length; i += 2) {
            uriBuilder.addParameter(parameters[i], parameters[i + 1]);
        }
        return uriBuilder.build();
    }

    private static class VoidResponseHandler
            implements ResponseHandler<Void, RuntimeException>
    {
        private final String operationName;

        public VoidResponseHandler(String operationName)
        {
            this.operationName = requireNonNull(operationName, "operationName is null");
        }

        @Override
        public Void handleException(Request request, Exception exception)
        {
            log.error(exception, "Resource manager %s request to %s failed", operationName, request.getUri());
            throw new PrestoException(
                    GENERIC_INTERNAL_ERROR,
                    format("Resource manager %s request to %s failed: %s", operationName, request.getUri(), exception.getMessage()),
                    exception);
        }

        @Override
        public Void handle(Request request, Response response)
        {
            if (response.getStatusCode() >= 400) {
                String errorMessage = format(
                        "Resource manager %s request to %s failed with status %d",
                        operationName,
                        request.getUri(),
                        response.getStatusCode());
                log.error("%s", errorMessage);
                throw new PrestoException(GENERIC_INTERNAL_ERROR, errorMessage);
            }
            return null;
        }
    }
}
