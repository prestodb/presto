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
package com.facebook.presto.spark.execution.http.server.smile;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.spark.execution.http.OkHttpBaseResponse;
import com.facebook.presto.spark.execution.http.OkHttpResponseHandler;
import okhttp3.Request;
import okhttp3.Response;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

/**
 * This response handler helps clients convert OkHttp responses to BaseResponse,
 * which simplifies the client code for handling both JSON and SMILE responses.
 */
public class AdaptingJsonResponseHandler<T>
        implements OkHttpResponseHandler<T>
{
    private final JsonCodec<T> jsonCodec;

    private AdaptingJsonResponseHandler(JsonCodec<T> jsonCodec)
    {
        this.jsonCodec = requireNonNull(jsonCodec, "jsonCodec is null");
    }

    public static <T> AdaptingJsonResponseHandler<T> createAdaptingJsonResponseHandler(JsonCodec<T> jsonCodec)
    {
        return new AdaptingJsonResponseHandler<>(jsonCodec);
    }

    public BaseResponse<T> handleException(Request request, Exception exception)
            throws RuntimeException
    {
        throw new RuntimeException("Failed to execute request: " + request.url(), exception);
    }

    public BaseResponse<T> handle(Request request, Response response)
            throws IOException
    {
        return new OkHttpBaseResponse<>(response, jsonCodec);
    }
}
