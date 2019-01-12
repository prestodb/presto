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
package io.prestosql.client;

import io.airlift.json.JsonCodec;
import okhttp3.Headers;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.UncheckedIOException;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.net.HttpHeaders.LOCATION;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class JsonResponse<T>
{
    private final int statusCode;
    private final String statusMessage;
    private final Headers headers;
    private final String responseBody;
    private final boolean hasValue;
    private final T value;
    private final IllegalArgumentException exception;

    private JsonResponse(int statusCode, String statusMessage, Headers headers, String responseBody)
    {
        this.statusCode = statusCode;
        this.statusMessage = statusMessage;
        this.headers = requireNonNull(headers, "headers is null");
        this.responseBody = requireNonNull(responseBody, "responseBody is null");

        this.hasValue = false;
        this.value = null;
        this.exception = null;
    }

    private JsonResponse(int statusCode, String statusMessage, Headers headers, String responseBody, JsonCodec<T> jsonCodec)
    {
        this.statusCode = statusCode;
        this.statusMessage = statusMessage;
        this.headers = requireNonNull(headers, "headers is null");
        this.responseBody = requireNonNull(responseBody, "responseBody is null");

        T value = null;
        IllegalArgumentException exception = null;
        try {
            value = jsonCodec.fromJson(responseBody);
        }
        catch (IllegalArgumentException e) {
            exception = new IllegalArgumentException(format("Unable to create %s from JSON response:\n[%s]", jsonCodec.getType(), responseBody), e);
        }
        this.hasValue = (exception == null);
        this.value = value;
        this.exception = exception;
    }

    public int getStatusCode()
    {
        return statusCode;
    }

    public String getStatusMessage()
    {
        return statusMessage;
    }

    public Headers getHeaders()
    {
        return headers;
    }

    public boolean hasValue()
    {
        return hasValue;
    }

    public T getValue()
    {
        if (!hasValue) {
            throw new IllegalStateException("Response does not contain a JSON value", exception);
        }
        return value;
    }

    public String getResponseBody()
    {
        return responseBody;
    }

    @Nullable
    public IllegalArgumentException getException()
    {
        return exception;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("statusCode", statusCode)
                .add("statusMessage", statusMessage)
                .add("headers", headers.toMultimap())
                .add("hasValue", hasValue)
                .add("value", value)
                .omitNullValues()
                .toString();
    }

    public static <T> JsonResponse<T> execute(JsonCodec<T> codec, OkHttpClient client, Request request)
    {
        try (Response response = client.newCall(request).execute()) {
            // TODO: fix in OkHttp: https://github.com/square/okhttp/issues/3111
            if ((response.code() == 307) || (response.code() == 308)) {
                String location = response.header(LOCATION);
                if (location != null) {
                    request = request.newBuilder().url(location).build();
                    return execute(codec, client, request);
                }
            }

            ResponseBody responseBody = requireNonNull(response.body());
            String body = responseBody.string();
            if (isJson(responseBody.contentType())) {
                return new JsonResponse<>(response.code(), response.message(), response.headers(), body, codec);
            }
            return new JsonResponse<>(response.code(), response.message(), response.headers(), body);
        }
        catch (IOException e) {
            // OkHttp throws this after clearing the interrupt status
            // TODO: remove after updating to Okio 1.15.0+
            if ((e instanceof InterruptedIOException) && "thread interrupted".equals(e.getMessage())) {
                Thread.currentThread().interrupt();
            }
            throw new UncheckedIOException(e);
        }
    }

    private static boolean isJson(MediaType type)
    {
        return (type != null) && "application".equals(type.type()) && "json".equals(type.subtype());
    }
}
