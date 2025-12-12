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
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.table.ConnectorTableFunction;
import com.facebook.presto.spi.tvf.TVFProvider;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.deser.std.FromStringDeserializer;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

import javax.inject.Inject;

import java.net.URI;
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

public class NativeTVFProvider
        implements TVFProvider
{
    private final NodeManager nodeManager;
    private final TypeManager typeManager;
    private final HttpClient httpClient;
    private static final String TABLE_FUNCTIONS_ENDPOINT = "/v1/functions/tvf";
    private final JsonCodec<Map<String, JsonBasedTableFunctionMetadata>> connectorTableFunctionListJsonCodec;
    private final Supplier<List<ConnectorTableFunction>> memoizedTableFunctionsSupplier;

    @Inject
    public NativeTVFProvider(
            NodeManager nodeManager,
            @ForWorkerInfo HttpClient httpClient,
            TypeManager typeManager)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.memoizedTableFunctionsSupplier = Suppliers.memoizeWithExpiration(this::loadConnectorTableFunctions,
                100000, TimeUnit.MILLISECONDS);

        JsonObjectMapperProvider provider = new JsonObjectMapperProvider();

        provider.setJsonDeserializers(ImmutableMap.of(
                Type.class, new TypeDeserializer(typeManager)));

        ObjectMapper mapper = provider.get();
        mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        JsonCodecFactory codecFactory = new JsonCodecFactory(provider);
        this.connectorTableFunctionListJsonCodec = codecFactory.mapJsonCodec(String.class, JsonBasedTableFunctionMetadata.class);
    }

    @Override
    public List<ConnectorTableFunction> getTableFunctions()
    {
        return memoizedTableFunctionsSupplier.get();
    }

    public static URI getWorkerLocation(NodeManager nodeManager, String endpoint)
    {
        Set<Node> workerNodes = nodeManager.getWorkerNodes();
        Node workerNode = Iterables.get(workerNodes, new Random().nextInt(workerNodes.size()));
        return HttpUriBuilder.uriBuilder()
                .scheme("http")
                .host(workerNode.getHost())
                .port(workerNode.getHostAndPort().getPort())
                .appendPath(endpoint)
                .build();
    }

    private synchronized List<ConnectorTableFunction> loadConnectorTableFunctions()
    {
        Map<String, JsonBasedTableFunctionMetadata> connectorTableFunctions;
        try {
            Request request = prepareGet().setUri(getWorkerLocation(nodeManager, TABLE_FUNCTIONS_ENDPOINT)).build();
            connectorTableFunctions = httpClient.execute(request, createJsonResponseHandler(connectorTableFunctionListJsonCodec));
        }
        catch (Exception e) {
            throw new PrestoException(INVALID_ARGUMENTS, "Failed to get table functions from endpoint.", e);
        }

        return connectorTableFunctions.values().stream().map(this::createNativeConnectorTableFunction).collect(ImmutableList.toImmutableList());
    }

    private synchronized NativeConnectorTableFunction createNativeConnectorTableFunction(JsonBasedTableFunctionMetadata connectorTableFunction)
    {
        return new NativeConnectorTableFunction(
                httpClient,
                nodeManager,
                typeManager,
                connectorTableFunction.getQualifiedObjectName(),
                connectorTableFunction.getArguments(),
                connectorTableFunction.getReturnTypeSpecification());
    }

    public static final class TypeDeserializer
            extends FromStringDeserializer<Type>
    {
        private final TypeManager typeManager;

        @Inject
        public TypeDeserializer(TypeManager typeManager)
        {
            super(Type.class);
            this.typeManager = requireNonNull(typeManager, "typeManager is null");
        }

        @Override
        protected Type _deserialize(String value, DeserializationContext context)
        {
            return typeManager.getType(parseTypeSignature(value));
        }
    }
}
