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
import com.facebook.airlift.http.client.StatusResponseHandler.StatusResponse;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.functionNamespace.ForRestServer;
import com.facebook.presto.functionNamespace.JsonBasedUdfFunctionMetadata;
import com.facebook.presto.functionNamespace.UdfFunctionSignatureMap;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.inject.name.Named;

import java.net.URI;
import java.util.List;
import java.util.Map;

import static com.facebook.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static com.facebook.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static com.facebook.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;
import static com.facebook.presto.functionNamespace.rest.RestErrorCode.REST_SERVER_FUNCTION_FETCH_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;

public class RestBasedFunctionApis
{
    public static final String ALL_FUNCTIONS_ENDPOINT = "/v1/functions";
    private final HttpClient httpClient;
    private final JsonCodec<Map<String, List<JsonBasedUdfFunctionMetadata>>> functionSignatureMapJsonCodec;
    private final String restUrl;

    @Inject
    public RestBasedFunctionApis(
            JsonCodec<Map<String, List<JsonBasedUdfFunctionMetadata>>> nativeFunctionSignatureMapJsonCodec,
            @ForRestServer HttpClient httpClient,
            @Named("restUrl") String restUrl)
    {
        this.functionSignatureMapJsonCodec = requireNonNull(nativeFunctionSignatureMapJsonCodec, "nativeFunctionSignatureMapJsonCodec is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.restUrl = requireNonNull(restUrl, "restUrl is null");
    }

    public String getFunctionsETag()
    {
        URI uri = uriBuilderFrom(URI.create(restUrl))
                .appendPath(ALL_FUNCTIONS_ENDPOINT)
                .build();
        Request request = Request.builder()
                .prepareHead()
                .setUri(uri)
                .build();

        StatusResponse response = httpClient.execute(request, createStatusResponseHandler());
        return response.getHeader("ETag");
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

    private UdfFunctionSignatureMap getFunctionsAt(String endpoint)
            throws IllegalStateException
    {
        try {
            URI uri = uriBuilderFrom(URI.create(restUrl))
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
            throw new PrestoException(REST_SERVER_FUNCTION_FETCH_ERROR, "Failed to fetch function definitions from REST server: " + e.getMessage(), e);
        }
    }
}
