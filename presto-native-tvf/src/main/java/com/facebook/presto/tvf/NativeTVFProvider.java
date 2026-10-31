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
package com.facebook.presto.tvf;

import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.http.client.HttpUriBuilder;
import com.facebook.airlift.http.client.Request;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.json.JsonCodecFactory;
import com.facebook.airlift.json.JsonObjectMapperProvider;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.table.ConnectorTableFunction;
import com.facebook.presto.spi.tvf.TVFProvider;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.FromStringDeserializer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

import javax.inject.Inject;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static com.facebook.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static com.facebook.airlift.http.client.Request.Builder.prepareGet;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static java.util.Objects.requireNonNull;

/**
 * Native implementation of TVF provider.
 */
public final class NativeTVFProvider
        implements TVFProvider
{
    private static final int MAX_RETRIES = 10;
    private static final long RETRY_DELAY_MS =
            Duration.ofMinutes(1).toMillis();
    private static final long CACHE_EXPIRATION_MS = 100000;
    private static final Logger LOG =
            Logger.get(NativeTVFProvider.class);
    private static final String TABLE_FUNCTIONS_ENDPOINT =
            "/v1/functions/tvf";

    private final NodeManager nodeManager;
    private final TypeManager typeManager;
    private final HttpClient httpClient;
    private final JsonCodec<Map<String, JsonBasedTableFunctionMetadata>>
            connectorTableFunctionListJsonCodec;
    private final Supplier<List<ConnectorTableFunction>>
            memoizedTableFunctionsSupplier;

    /**
     * Constructs a native TVF provider.
     *
     * @param nodeManager the node manager
     * @param httpClient the HTTP client
     * @param typeManager the type manager
     */
    @Inject
    public NativeTVFProvider(
            final NodeManager nodeManager,
            @ForWorkerInfo final HttpClient httpClient,
            final TypeManager typeManager)
    {
        this.nodeManager = requireNonNull(nodeManager,
                "nodeManager is null");
        this.typeManager = requireNonNull(typeManager,
                "typeManager is null");
        this.httpClient = requireNonNull(httpClient,
                "httpClient is null");
        this.memoizedTableFunctionsSupplier =
                Suppliers.memoizeWithExpiration(
                        this::loadConnectorTableFunctions,
                        CACHE_EXPIRATION_MS,
                        TimeUnit.MILLISECONDS);

        JsonObjectMapperProvider provider =
                new JsonObjectMapperProvider();
        provider.setJsonDeserializers(ImmutableMap.of(
                Type.class, new TypeDeserializer(typeManager)));
        JsonCodecFactory codecFactory = new JsonCodecFactory(provider);
        this.connectorTableFunctionListJsonCodec =
                codecFactory.mapJsonCodec(String.class,
                        JsonBasedTableFunctionMetadata.class);
    }

    /**
     * Gets the table functions.
     *
     * @return the list of connector table functions
     */
    @Override
    public List<ConnectorTableFunction> getTableFunctions()
    {
        return memoizedTableFunctionsSupplier.get();
    }

    /**
     * Gets the worker location for the given endpoint.
     *
     * @param nodeManager the node manager
     * @param endpoint the endpoint path
     * @return the worker URI
     */
    public static URI getWorkerLocation(
            final NodeManager nodeManager,
            final String endpoint)
    {
        Node workerNode = null;
        for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
            Set<Node> workerNodes = nodeManager.getWorkerNodes();
            if (!workerNodes.isEmpty()) {
                workerNode = Iterables.get(workerNodes,
                        new Random().nextInt(workerNodes.size()));
                break;
            }
            LOG.warn("No worker nodes available yet (attempt "
                    + attempt + ")");
            if (attempt < MAX_RETRIES) {
                try {
                    Thread.sleep(RETRY_DELAY_MS);
                }
                catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(
                            "Retry fetching table function registry "
                                    + "interrupted",
                            ie);
                }
            }
        }

        if (workerNode == null) {
            throw new RuntimeException("Failed to get worker node");
        }
        URI workerHttpUri = workerNode.getHttpUri();
        return HttpUriBuilder.uriBuilder()
                .scheme(workerHttpUri.getScheme())
                .host(workerHttpUri.getHost())
                .port(workerHttpUri.getPort())
                .appendPath(endpoint)
                .build();
    }

    /**
     * Extracts the reason from a Velox error message.
     *
     * @param errorMessage the error message
     * @return the extracted reason or full message if not found
     */
    public static String extractReasonFromVeloxError(
            final String errorMessage)
    {
        String[] lines = errorMessage.split("\n");
        for (String line : lines) {
            String trimmed = line.trim();
            if (trimmed.startsWith("Reason:")) {
                return trimmed.substring("Reason:".length()).trim();
            }
        }
        return errorMessage;
    }

    /**
     * Gets the HTTP client.
     *
     * @return the HTTP client
     */
    @VisibleForTesting
    public HttpClient getHttpClient()
    {
        return httpClient;
    }

    /**
     * Loads connector table functions from the endpoint.
     *
     * @return the list of connector table functions
     */
    private synchronized List<ConnectorTableFunction>
            loadConnectorTableFunctions()
    {
        Map<String, JsonBasedTableFunctionMetadata>
                connectorTableFunctions;
        try {
            Request request = prepareGet()
                    .setUri(getWorkerLocation(nodeManager,
                            TABLE_FUNCTIONS_ENDPOINT))
                    .build();
            connectorTableFunctions = httpClient.execute(
                    request,
                    createJsonResponseHandler(
                            connectorTableFunctionListJsonCodec));
        }
        catch (Exception e) {
            throw new PrestoException(INVALID_ARGUMENTS,
                    "Failed to get table functions from endpoint.", e);
        }

        return connectorTableFunctions.values().stream()
                .map(this::createNativeConnectorTableFunction)
                .collect(ImmutableList.toImmutableList());
    }

    /**
     * Creates a native connector table function.
     *
     * @param connectorTableFunction the table function metadata
     * @return the native connector table function
     */
    private synchronized NativeConnectorTableFunction
            createNativeConnectorTableFunction(
                    final JsonBasedTableFunctionMetadata
                            connectorTableFunction)
    {
        return new NativeConnectorTableFunction(
                httpClient,
                nodeManager,
                typeManager,
                connectorTableFunction.getQualifiedObjectName(),
                connectorTableFunction.getArguments(),
                connectorTableFunction.getReturnTypeSpecification());
    }

    /**
     * Deserializer for Type objects.
     */
    private static final class TypeDeserializer
            extends FromStringDeserializer<Type>
    {
        private final TypeManager typeManager;

        /**
         * Constructs a type deserializer.
         *
         * @param typeManager the type manager
         */
        @Inject
        public TypeDeserializer(final TypeManager typeManager)
        {
            super(Type.class);
            this.typeManager = requireNonNull(typeManager,
                    "typeManager is null");
        }

        /**
         * Deserializes a type from string.
         *
         * @param value the string value
         * @param context the deserialization context
         * @return the deserialized type
         */
        @Override
        protected Type _deserialize(
                final String value,
                final DeserializationContext context)
        {
            return typeManager.getType(parseTypeSignature(value));
        }
    }
}
