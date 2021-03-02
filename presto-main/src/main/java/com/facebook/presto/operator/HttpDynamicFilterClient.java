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
package com.facebook.presto.operator;

import com.facebook.airlift.http.client.FullJsonResponseHandler.JsonResponse;
import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.http.client.HttpUriBuilder;
import com.facebook.airlift.http.client.StatusResponseHandler.StatusResponse;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.json.JsonCodecFactory;
import com.facebook.airlift.json.ObjectMapperProvider;
import com.facebook.presto.block.BlockJsonSerde;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockEncodingManager;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.type.TypeDeserializer;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;

import java.net.URI;
import java.util.NoSuchElementException;
import java.util.Optional;

import static com.facebook.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static com.facebook.airlift.http.client.JsonBodyGenerator.jsonBodyGenerator;
import static com.facebook.airlift.http.client.Request.Builder.prepareGet;
import static com.facebook.airlift.http.client.Request.Builder.preparePut;
import static com.facebook.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;
import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static java.util.Objects.requireNonNull;

public class HttpDynamicFilterClient
        implements DynamicFilterClient
{
    private final JsonCodec<DynamicFilterSummary> summaryJsonCodec;
    private final URI coordinatorUri;
    private final HttpClient httpClient;
    private final Optional<TaskId> taskId;
    private final Optional<String> source;
    private final int driverId;
    private final int expectedDriversCount;
    private final TypeManager typeManager;

    public HttpDynamicFilterClient(JsonCodec<DynamicFilterSummary> summaryJsonCodec, URI coordinatorUri, HttpClient httpClient, Optional<TaskId> taskId, Optional<String> source, int driverId, int expectedDriversCount, TypeManager typeManager)
    {
        this.summaryJsonCodec = requireNonNull(summaryJsonCodec, "summaryJsonCodec is null");
        this.coordinatorUri = requireNonNull(coordinatorUri, "coordinatorURI obtained is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.taskId = taskId;
        this.source = source;
        this.driverId = driverId;
        this.expectedDriversCount = expectedDriversCount;
        this.typeManager = typeManager;
    }

    @Override
    public ListenableFuture<JsonResponse<DynamicFilterSummary>> getSummary()
    {
        TaskId task = taskId.orElseThrow(() -> new NoSuchElementException("taskId is empty"));
        String src = source.orElseThrow(() -> new NoSuchElementException("source is empty"));
        return httpClient.executeAsync(
                prepareGet()
                        .setUri(HttpUriBuilder.uriBuilderFrom(coordinatorUri)
                                .appendPath("/v1/dynamic-filter")
                                .appendPath("/" + task.getQueryId())
                                .appendPath("/" + src)
                                .build())
                        .build(),
                createFullJsonResponseHandler(jsonCodec(DynamicFilterSummary.class)));
    }

    @Override
    public ListenableFuture<JsonResponse<DynamicFilterSummary>> getSummary(String queryId, String source)
    {
        return httpClient.executeAsync(
                prepareGet()
                        .setUri(HttpUriBuilder.uriBuilderFrom(coordinatorUri)
                                .appendPath("/v1/dynamic-filter")
                                .appendPath("/" + queryId)
                                .appendPath("/" + source)
                                .build())
                        .build(),
                createFullJsonResponseHandler(new JsonCodecFactory(getObjectMapperProvider(typeManager))
                        .jsonCodec(DynamicFilterSummary.class)));
    }

    @Override
    public ListenableFuture<StatusResponse> storeSummary(DynamicFilterSummary summary)
    {
        TaskId task = taskId.orElseThrow(() -> new NoSuchElementException("taskId is empty"));
        String src = source.orElseThrow(() -> new NoSuchElementException("source is empty"));
        return httpClient.executeAsync(
                preparePut()
                        .setUri(HttpUriBuilder.uriBuilderFrom(coordinatorUri)
                                .appendPath("/v1/dynamic-filter")
                                .appendPath("/" + task.getQueryId())
                                .appendPath("/" + src)
                                .appendPath("/" + task.getStageExecutionId().getId())
                                .appendPath("/" + task.getId())
                                .appendPath("/" + driverId)
                                .appendPath("/" + expectedDriversCount)
                                .build())
                        .addHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                        .setBodyGenerator(jsonBodyGenerator(summaryJsonCodec, summary))
                        .build(),
                createStatusResponseHandler());
    }

    private ObjectMapperProvider getObjectMapperProvider(TypeManager typeManager)
    {
        ObjectMapperProvider provider = new ObjectMapperProvider();
        if (typeManager != null) {
            ImmutableMap.Builder deserializers = ImmutableMap.builder();
            BlockEncodingManager blockEncodingSerde = new BlockEncodingManager();
            deserializers.put(Block.class, new BlockJsonSerde.Deserializer(blockEncodingSerde));
            deserializers.put(Type.class, new TypeDeserializer(typeManager));
            provider.setJsonDeserializers(deserializers.build());
            provider.setJsonSerializers(ImmutableMap.of(Block.class, new BlockJsonSerde.Serializer(blockEncodingSerde)));
        }
        return provider;
    }
}
