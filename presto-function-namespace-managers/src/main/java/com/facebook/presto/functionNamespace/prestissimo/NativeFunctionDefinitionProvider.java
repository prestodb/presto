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
package com.facebook.presto.functionNamespace.prestissimo;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.functionNamespace.JsonBasedUdfFunctionMetadata;
import com.facebook.presto.functionNamespace.UdfFunctionSignatureMap;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import okhttp3.OkHttpClient;
import okhttp3.Request;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;

public class NativeFunctionDefinitionProvider
        implements FunctionDefinitionProvider
{
    private static final Logger log = Logger.get(NativeFunctionDefinitionProvider.class);

    public static final String FUNCTION_SIGNATURES_ENDPOINT = "/v1/info/workerFunctionSignatures";

    private final OkHttpClient httpClient = new OkHttpClient().newBuilder().build();

    private final JsonCodec<Map<String, List<JsonBasedUdfFunctionMetadata>>> nativeFunctionSignatureMapJsonCodec;

    @Inject
    public NativeFunctionDefinitionProvider(JsonCodec<Map<String, List<JsonBasedUdfFunctionMetadata>>> nativeFunctionSignatureMapJsonCodec)
    {
        this.nativeFunctionSignatureMapJsonCodec = nativeFunctionSignatureMapJsonCodec;
    }

    private URL getNativeWorkerUrl(NodeManager nodeManager)
    {
        Set<Node> nodes = nodeManager.getAllNodes();

        // TODO: Need to identity native worker.
        // Temporary picking first worker for now.
        Node nativeNode = nodes.stream().filter(Node::isCoordinatorSidecar).findFirst()
                .orElseThrow(() -> new PrestoException(NOT_FOUND, "failed to find native node !"));
        try {
            // endpoint to retrieve function signatures from native worker
            return new URL(nativeNode.getHttpUri().toString() + FUNCTION_SIGNATURES_ENDPOINT);
        }
        catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public UdfFunctionSignatureMap getUdfDefinition(NodeManager nodeManager)
            throws IllegalStateException
    {
        // Todo: Clean this method up
        try {
            Request request = new Request.Builder()
                    .url(getNativeWorkerUrl(nodeManager))
                    .build();
            String responseBody = httpClient.newCall(request).execute().body().string();
            Map<String, List<JsonBasedUdfFunctionMetadata>> nativeFunctionSignatureMap = nativeFunctionSignatureMapJsonCodec.fromJson(responseBody);
            return new UdfFunctionSignatureMap(ImmutableMap.copyOf(nativeFunctionSignatureMap));
        }
        catch (Exception e) {
            throw new IllegalStateException("Failed to get function definition for NativeFunctionNamespaceManager, " + e.getMessage());
        }
    }
}
