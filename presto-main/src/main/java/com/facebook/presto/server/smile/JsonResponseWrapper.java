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
package com.facebook.presto.server.smile;

import com.facebook.airlift.http.client.FullJsonResponseHandler.JsonResponse;
import com.facebook.airlift.http.client.HeaderName;
import com.google.common.collect.ListMultimap;

import java.util.List;

import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

public class JsonResponseWrapper<T>
        implements BaseResponse<T>
{
    private final JsonResponse<T> jsonResponse;

    private JsonResponseWrapper(JsonResponse<T> jsonResponse)
    {
        this.jsonResponse = requireNonNull(jsonResponse, "jsonResponse is null");
    }

    public static <T> JsonResponseWrapper<T> wrapJsonResponse(JsonResponse<T> response)
    {
        return new JsonResponseWrapper<>(response);
    }

    public static <T> JsonResponse<T> unwrapJsonResponse(BaseResponse<T> response)
    {
        verify(response instanceof JsonResponseWrapper);
        return ((JsonResponseWrapper<T>) response).jsonResponse;
    }

    @Override
    public int getStatusCode()
    {
        return jsonResponse.getStatusCode();
    }

    @Override
    public String getStatusMessage()
    {
        return jsonResponse.getStatusMessage();
    }

    @Override
    public String getHeader(String name)
    {
        return jsonResponse.getHeader(name);
    }

    @Override
    public List<String> getHeaders(String name)
    {
        return jsonResponse.getHeaders(name);
    }

    @Override
    public ListMultimap<HeaderName, String> getHeaders()
    {
        return jsonResponse.getHeaders();
    }

    @Override
    public boolean hasValue()
    {
        return jsonResponse.hasValue();
    }

    @Override
    public T getValue()
    {
        return jsonResponse.getValue();
    }

    @Override
    public int getResponseSize()
    {
        return jsonResponse.getResponseSize();
    }

    @Override
    public byte[] getResponseBytes()
    {
        return jsonResponse.getResponseBytes();
    }

    @Override
    public Exception getException()
    {
        return jsonResponse.getException();
    }
}
