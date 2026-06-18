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
package com.facebook.presto.spark.execution.http;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.spark.execution.http.server.smile.BaseResponse;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ListMultimap;
import okhttp3.Response;

import java.io.IOException;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Adapter class that bridges OkHttp Response to BaseResponse interface
 */
public class OkHttpBaseResponse<T>
        implements BaseResponse<T>
{
    private final int statusCode;
    private final ListMultimap<OkHttpHeaderName, String> headers;
    private final T value;
    private final byte[] responseBytes;
    private final Exception exception;
    private final boolean hasValue;

    public OkHttpBaseResponse(Response response, JsonCodec<T> codec)
            throws IOException
    {
        requireNonNull(response, "response is null");
        requireNonNull(codec, "codec is null");

        this.statusCode = response.code();
        this.headers = buildHeaders(response);

        // Calculate values first
        T tempValue = null;
        boolean tempHasValue = false;
        Exception tempException = null;
        byte[] tempResponseBytes;

        if (response.body() != null) {
            tempResponseBytes = response.body().bytes();
            if (statusCode == 200 && tempResponseBytes.length > 0) {
                try {
                    tempValue = codec.fromBytes(tempResponseBytes);
                    tempHasValue = true;
                    tempException = null;
                }
                catch (Exception e) {
                    tempValue = null;
                    tempHasValue = false;
                    tempException = e;
                }
            }
            else {
                tempValue = null;
                tempHasValue = false;
                tempException = null;
            }
        }
        else {
            tempResponseBytes = new byte[0];
            tempValue = null;
            tempHasValue = false;
            tempException = null;
        }

        // Assign to final fields only once
        this.responseBytes = tempResponseBytes;
        this.value = tempValue;
        this.hasValue = tempHasValue;
        this.exception = tempException;
    }

    private static ListMultimap<OkHttpHeaderName, String> buildHeaders(Response response)
    {
        ImmutableListMultimap.Builder<OkHttpHeaderName, String> builder = ImmutableListMultimap.builder();

        for (String name : response.headers().names()) {
            for (String value : response.headers().values(name)) {
                builder.put(OkHttpHeaderName.of(name), value);
            }
        }

        return builder.build();
    }

    @Override
    public int getStatusCode()
    {
        return statusCode;
    }

    @Override
    public String getHeader(String name)
    {
        List<String> values = getHeaders(name);
        return values.isEmpty() ? null : values.get(0);
    }

    @Override
    public List<String> getHeaders(String name)
    {
        return headers.get(OkHttpHeaderName.of(name));
    }

    @Override
    public ListMultimap<OkHttpHeaderName, String> getHeaders()
    {
        return headers;
    }

    @Override
    public boolean hasValue()
    {
        return hasValue;
    }

    @Override
    public T getValue()
    {
        return value;
    }

    @Override
    public int getResponseSize()
    {
        return responseBytes.length;
    }

    @Override
    public byte[] getResponseBytes()
    {
        return responseBytes;
    }

    @Override
    public Exception getException()
    {
        return exception;
    }
}
