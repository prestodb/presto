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
package com.facebook.presto.builtin.tools;

import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.http.client.HttpUriBuilder;
import com.facebook.airlift.http.client.JsonResponseHandler;
import com.facebook.airlift.http.client.Request;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.functionNamespace.JsonBasedUdfFunctionMetadata;
import com.facebook.presto.functionNamespace.UdfFunctionSignatureMap;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.function.SqlFunction;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class NativeSidecarFunctionRegistryTool
        implements WorkerFunctionRegistryTool
{
    private static final int MAX_RETRIES = 8;
    private static final long RETRY_DELAY_MS = Duration.ofMinutes(1).toMillis();
    private static final Logger log = Logger.get(NativeSidecarFunctionRegistryTool.class);
    private final JsonCodec<Map<String, List<JsonBasedUdfFunctionMetadata>>> nativeFunctionSignatureMapJsonCodec;
    private final NodeManager nodeManager;
    private final HttpClient httpClient;
    private static final String FUNCTION_SIGNATURES_ENDPOINT = "/v1/functions";

    @Inject
    public NativeSidecarFunctionRegistryTool(
            @ForNativeFunctionRegistryInfo HttpClient httpClient,
            JsonCodec<Map<String, List<JsonBasedUdfFunctionMetadata>>> nativeFunctionSignatureMapJsonCodec,
            NodeManager nodeManager)
    {
        this.nativeFunctionSignatureMapJsonCodec =
                requireNonNull(nativeFunctionSignatureMapJsonCodec, "nativeFunctionSignatureMapJsonCodec is null");
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.httpClient = requireNonNull(httpClient, "typeManager is null");
    }

    @Override
    public List<? extends SqlFunction> getWorkerFunctions()
    {
        return getNativeFunctionSignatureMap()
                .getUDFSignatureMap()
                .entrySet()
                .stream()
                .flatMap(entry -> entry.getValue().stream()
                        .map(metaInfo -> WorkerFunctionUtil.createSqlInvokedFunction(entry.getKey(), metaInfo, "presto")))
                .collect(ImmutableList.toImmutableList());
    }

    private UdfFunctionSignatureMap getNativeFunctionSignatureMap()
    {
        try {
            Request request = Request.Builder.prepareGet().setUri(getSidecarLocationOnStartup()).build();
            Map<String, List<JsonBasedUdfFunctionMetadata>> nativeFunctionSignatureMap = httpClient.execute(request, JsonResponseHandler.createJsonResponseHandler(nativeFunctionSignatureMapJsonCodec));
            return new UdfFunctionSignatureMap(ImmutableMap.copyOf(nativeFunctionSignatureMap));
        }
        catch (Exception e) {
            throw new PrestoException(StandardErrorCode.INVALID_ARGUMENTS, "Failed to get functions from sidecar.", e);
        }
    }

    private URI getSidecarLocationOnStartup()
    {
        Node sidecarNode = null;
        int retryCount = 0;
        while (sidecarNode == null && retryCount < MAX_RETRIES) {
            try {
                sidecarNode = nodeManager.getSidecarNode();
                break;
            }
            catch (Exception e) {
                retryCount++;
                log.error("Error getting sidecar node (attempt " + retryCount + "): " + e.getMessage());
                if (retryCount < MAX_RETRIES) {
                    try {
                        Thread.sleep(RETRY_DELAY_MS);
                    }
                    catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Retry fetching sidecar function registry interrupted", ie);
                    }
                }
                else {
                    throw new RuntimeException("Failed to get sidecar node", e);
                }
            }
        }

        return HttpUriBuilder
                .uriBuilderFrom(sidecarNode.getHttpUri())
                .appendPath(FUNCTION_SIGNATURES_ENDPOINT)
                .build();
    }
}
