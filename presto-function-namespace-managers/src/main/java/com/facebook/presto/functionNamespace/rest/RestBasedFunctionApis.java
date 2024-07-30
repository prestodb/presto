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
package com.facebook.presto.functionNamespace.rest;

import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.http.client.Request;
import com.facebook.airlift.http.client.StatusResponseHandler;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.functionNamespace.ForRestServer;
import com.facebook.presto.functionNamespace.JsonBasedUdfFunctionMetadata;
import com.facebook.presto.functionNamespace.UdfFunctionSignatureMap;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;

import java.net.URI;
import java.util.List;
import java.util.Map;

import static com.facebook.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static com.facebook.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;

public class RestBasedFunctionApis
{
    public static final String ALL_FUNCTIONS_ENDPOINT = "/v1/functions";

    private final HttpClient httpClient;

    private final JsonCodec<Map<String, List<JsonBasedUdfFunctionMetadata>>> functionSignatureMapJsonCodec;

    private final RestBasedFunctionNamespaceManagerConfig managerConfig;

    @Inject
    public RestBasedFunctionApis(
            JsonCodec<Map<String, List<JsonBasedUdfFunctionMetadata>>> nativeFunctionSignatureMapJsonCodec,
            @ForRestServer HttpClient httpClient,
            RestBasedFunctionNamespaceManagerConfig managerConfig)
    {
        this.functionSignatureMapJsonCodec = nativeFunctionSignatureMapJsonCodec;
        this.httpClient = httpClient;
        this.managerConfig = managerConfig;
    }

    public String getFunctionsETag()
    {
        try {
            URI uri = uriBuilderFrom(URI.create(managerConfig.getRestUrl()))
                    .appendPath(ALL_FUNCTIONS_ENDPOINT)
                    .build();
            Request request = Request.builder()
                    .prepareHead()
                    .setUri(uri)
                    .build();

            StatusResponseHandler.StatusResponse response = httpClient.execute(request, StatusResponseHandler.createStatusResponseHandler());
            String version = response.getHeader("ETag");
            if (version == null) {
                throw new IllegalStateException("Failed to retrieve API version: 'ETag' header is missing");
            }
            return version;
        }
        catch (Exception e) {
            throw new IllegalStateException("Failed to get functions ETag from REST server, " + e.getMessage());
        }
    }

    public UdfFunctionSignatureMap getAllFunctions()
    {
        return getFunctionsAt(ALL_FUNCTIONS_ENDPOINT);
    }

    public UdfFunctionSignatureMap getFunctions(String schema)
    {
        return getFunctionsAt(ALL_FUNCTIONS_ENDPOINT + "/" + schema);
    }

    public UdfFunctionSignatureMap getFunctions(String schema, String functionName)
    {
        return getFunctionsAt(ALL_FUNCTIONS_ENDPOINT + "/" + schema + "/" + functionName);
    }

    public String addFunction(String schema, String functionName, JsonBasedUdfFunctionMetadata metadata)
    {
        throw new PrestoException(NOT_SUPPORTED, "Add Function is yet to be added");
    }

    public String updateFunction(String schema, String functionName, String functionId, JsonBasedUdfFunctionMetadata metadata)
    {
        throw new PrestoException(NOT_SUPPORTED, "Update Function is yet to be added");
    }

    public String deleteFunction(String schema, String functionName, String functionId)
    {
        throw new PrestoException(NOT_SUPPORTED, "Delete Function is yet to be added");
    }

    private UdfFunctionSignatureMap getFunctionsAt(String endpoint)
            throws IllegalStateException
    {
        try {
            URI uri = uriBuilderFrom(URI.create(managerConfig.getRestUrl()))
                    .appendPath(endpoint)
                    .build();
            Request request = Request.builder()
                    .prepareGet()
                    .setUri(uri)
                    .build();

            Map<String, List<JsonBasedUdfFunctionMetadata>> nativeFunctionSignatureMap = httpClient.execute(request, createJsonResponseHandler(functionSignatureMapJsonCodec));
            return new UdfFunctionSignatureMap(ImmutableMap.copyOf(nativeFunctionSignatureMap));
        }
        catch (Exception e) {
            throw new IllegalStateException("Failed to get function definitions from REST server, " + e.getMessage());
        }
    }
}
