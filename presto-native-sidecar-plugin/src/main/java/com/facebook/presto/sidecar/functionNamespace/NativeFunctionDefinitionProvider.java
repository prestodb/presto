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
import com.facebook.airlift.http.client.HttpUriBuilder;
import com.facebook.airlift.http.client.Request;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.util.Backoff;
import com.facebook.presto.functionNamespace.JsonBasedUdfFunctionMetadata;
import com.facebook.presto.functionNamespace.ServingCatalog;
import com.facebook.presto.functionNamespace.UdfFunctionSignatureMap;
import com.facebook.presto.sidecar.ForSidecarInfo;
import com.facebook.presto.sidecar.SidecarRetryConfig;
import com.facebook.presto.sidecar.SidecarRetryDriver;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.PrestoException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;

import java.net.URI;
import java.util.List;
import java.util.Map;

import static com.facebook.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static com.facebook.airlift.http.client.Request.Builder.prepareGet;
import static com.facebook.presto.builtin.tools.NativeSidecarFunctionRegistryTool.getSidecarLocationOnStartup;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class NativeFunctionDefinitionProvider
        implements FunctionDefinitionProvider
{
    private static final Logger log = Logger.get(NativeFunctionDefinitionProvider.class);
    private final JsonCodec<Map<String, List<JsonBasedUdfFunctionMetadata>>> nativeFunctionSignatureMapJsonCodec;
    private final HttpClient httpClient;
    private final NativeFunctionNamespaceManagerConfig config;
    private final String catalogName;
    private final SidecarRetryConfig retryConfig;

    @Inject
    public NativeFunctionDefinitionProvider(
            @ForSidecarInfo HttpClient httpClient,
            JsonCodec<Map<String, List<JsonBasedUdfFunctionMetadata>>> nativeFunctionSignatureMapJsonCodec,
            NativeFunctionNamespaceManagerConfig config,
            @ServingCatalog String catalogName,
            SidecarRetryConfig retryConfig)
    {
        this.nativeFunctionSignatureMapJsonCodec =
                requireNonNull(nativeFunctionSignatureMapJsonCodec, "nativeFunctionSignatureMapJsonCodec is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.config = requireNonNull(config, "config is null");
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.retryConfig = requireNonNull(retryConfig, "retryConfig is null");
    }

    @Override
    public UdfFunctionSignatureMap getUdfDefinition(NodeManager nodeManager)
    {
        // getSidecarLocationOnStartup uses its own retry config (sidecarNumRetries / sidecarRetryDelay)
        // scoped to node discovery at startup. The SidecarRetryDriver below retries the HTTP call itself
        // and is governed by SidecarRetryConfig (maxFailureInterval).
        // Base endpoint: /v1/functions
        URI baseUri;
        try {
            baseUri = getSidecarLocationOnStartup(
                    nodeManager, config.getSidecarNumRetries(), config.getSidecarRetryDelay().toMillis());
        }
        catch (RuntimeException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR,
                    format("Failed to discover sidecar node for catalog '%s'", catalogName), e);
        }
        // Catalog-filtered endpoint: /v1/functions/{catalog}
        URI catalogUri = HttpUriBuilder.uriBuilderFrom(baseUri).appendPath(catalogName).build();

        Backoff backoff = new Backoff(retryConfig.getMaxFailureInterval());
        Map<String, List<JsonBasedUdfFunctionMetadata>> nativeFunctionSignatureMap =
                SidecarRetryDriver.executeWithRetry(
                        () -> {
                            Request catalogRequest = prepareGet().setUri(catalogUri).build();
                            return httpClient.execute(catalogRequest, createJsonResponseHandler(nativeFunctionSignatureMapJsonCodec));
                        },
                        backoff,
                        "function definitions for catalog " + catalogName,
                        () -> new PrestoException(GENERIC_INTERNAL_ERROR, format("Failed to get catalog-scoped functions from sidecar for catalog '%s'", catalogName)));

        if (nativeFunctionSignatureMap == null) {
            return new UdfFunctionSignatureMap(ImmutableMap.of());
        }
        return new UdfFunctionSignatureMap(ImmutableMap.copyOf(nativeFunctionSignatureMap));
    }

    @VisibleForTesting
    public HttpClient getHttpClient()
    {
        return httpClient;
    }

    @VisibleForTesting
    public SidecarRetryConfig getRetryConfig()
    {
        return retryConfig;
    }
}
