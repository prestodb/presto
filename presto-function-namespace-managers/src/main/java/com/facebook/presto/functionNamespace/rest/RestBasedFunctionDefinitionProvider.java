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
import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.functionNamespace.ForRestServer;
import com.facebook.presto.functionNamespace.FunctionDefinitionProvider;
import com.facebook.presto.functionNamespace.JsonBasedUdfFunctionMetadata;
import com.facebook.presto.functionNamespace.UdfFunctionSignatureMap;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;

import java.net.URI;
import java.util.List;
import java.util.Map;

import static com.facebook.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static com.facebook.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;

public class RestBasedFunctionDefinitionProvider
        implements FunctionDefinitionProvider
{
    private static final Logger log = Logger.get(RestBasedFunctionDefinitionProvider.class);

    public static final String FUNCTION_SIGNATURES_ENDPOINT = "/v1/info/functionSignatures";

    private final HttpClient httpClient;

    private final JsonCodec<Map<String, List<JsonBasedUdfFunctionMetadata>>> functionSignatureMapJsonCodec;

    @Inject
    public RestBasedFunctionDefinitionProvider(JsonCodec<Map<String, List<JsonBasedUdfFunctionMetadata>>> nativeFunctionSignatureMapJsonCodec, @ForRestServer HttpClient httpClient)
    {
        this.functionSignatureMapJsonCodec = nativeFunctionSignatureMapJsonCodec;
        this.httpClient = httpClient;
    }

    @Override
    public UdfFunctionSignatureMap getUdfDefinition(String restURL)
            throws IllegalStateException
    {
        try {
            URI uri = uriBuilderFrom(URI.create(restURL))
                    .appendPath(FUNCTION_SIGNATURES_ENDPOINT)
                    .build();
            Request request = Request.builder()
                    .prepareGet()
                    .setUri(uri)
                    .build();

            Map<String, List<JsonBasedUdfFunctionMetadata>> nativeFunctionSignatureMap = httpClient.execute(request, createJsonResponseHandler(functionSignatureMapJsonCodec));
            return new UdfFunctionSignatureMap(ImmutableMap.copyOf(nativeFunctionSignatureMap));
        }
        catch (Exception e) {
            throw new IllegalStateException("Failed to get function definitions for REST server/ Native worker, " + e.getMessage());
        }
    }
}
