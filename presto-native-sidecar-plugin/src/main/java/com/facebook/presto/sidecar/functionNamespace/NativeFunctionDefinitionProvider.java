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
package com.facebook.presto.sidecar.functionNamespace;

import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.http.client.Request;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.functionNamespace.JsonBasedUdfFunctionMetadata;
import com.facebook.presto.functionNamespace.UdfFunctionSignatureMap;
import com.facebook.presto.sidecar.ForSidecarInfo;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.PrestoException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;

import java.util.List;
import java.util.Map;

import static com.facebook.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static com.facebook.airlift.http.client.Request.Builder.prepareGet;
import static com.facebook.presto.builtin.tools.NativeSidecarFunctionRegistryTool.getSidecarLocationOnStartup;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static java.util.Objects.requireNonNull;

public class NativeFunctionDefinitionProvider
        implements FunctionDefinitionProvider
{
    private static final Logger log = Logger.get(NativeFunctionDefinitionProvider.class);
    private final JsonCodec<Map<String, List<JsonBasedUdfFunctionMetadata>>> nativeFunctionSignatureMapJsonCodec;
    private final HttpClient httpClient;
    private final NativeFunctionNamespaceManagerConfig config;

    @Inject
    public NativeFunctionDefinitionProvider(
            @ForSidecarInfo HttpClient httpClient,
            JsonCodec<Map<String, List<JsonBasedUdfFunctionMetadata>>> nativeFunctionSignatureMapJsonCodec,
            NativeFunctionNamespaceManagerConfig config)
    {
        this.nativeFunctionSignatureMapJsonCodec =
                requireNonNull(nativeFunctionSignatureMapJsonCodec, "nativeFunctionSignatureMapJsonCodec is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.config = requireNonNull(config, "config is null");
    }

    @Override
    public UdfFunctionSignatureMap getUdfDefinition(NodeManager nodeManager)
    {
        try {
            Request request =
                    prepareGet().setUri(
                            getSidecarLocationOnStartup(
                                    nodeManager, config.getSidecarNumRetries(), config.getSidecarRetryDelay().toMillis())).build();
            Map<String, List<JsonBasedUdfFunctionMetadata>> nativeFunctionSignatureMap = httpClient.execute(request, createJsonResponseHandler(nativeFunctionSignatureMapJsonCodec));
            return new UdfFunctionSignatureMap(ImmutableMap.copyOf(nativeFunctionSignatureMap));
        }
        catch (Exception e) {
            throw new PrestoException(INVALID_ARGUMENTS, "Failed to get functions from sidecar.", e);
        }
    }

    @VisibleForTesting
    public HttpClient getHttpClient()
    {
        return httpClient;
    }
}
