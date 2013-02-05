/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.server.FullJsonResponseHandler.JsonResponse;
import com.google.common.base.Charsets;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.io.CharStreams;
import io.airlift.http.client.Request;
import io.airlift.http.client.Response;
import io.airlift.http.client.ResponseHandler;
import io.airlift.json.JsonCodec;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ConnectException;
import java.util.List;

// TODO: remove when this class change is applied to airlift
public class FullJsonResponseHandler<T> implements ResponseHandler<JsonResponse<T>, RuntimeException>
{
    private static final com.google.common.net.MediaType MEDIA_TYPE_JSON = com.google.common.net.MediaType.create("application", "json");

    public static <T> FullJsonResponseHandler<T> createFullJsonResponseHandler(JsonCodec<T> jsonCodec)
    {
        return new FullJsonResponseHandler<>(jsonCodec);
    }

    private final JsonCodec<T> jsonCodec;

    FullJsonResponseHandler(JsonCodec<T> jsonCodec)
    {
        this.jsonCodec = jsonCodec;
    }

    @Override
    public RuntimeException handleException(Request request, Exception exception)
    {
        if (exception instanceof ConnectException) {
            return new RuntimeException("Server refused connection: " + request.getUri().toASCIIString());
        }
        return Throwables.propagate(exception);
    }

    @Override
    public JsonResponse<T> handle(Request request, Response response)
    {
        String contentType = response.getHeader("Content-Type");
        if (!com.google.common.net.MediaType.parse(contentType).is(MEDIA_TYPE_JSON)) {
            return new JsonResponse<>(response.getStatusCode(), response.getStatusMessage(), response.getHeaders(), jsonCodec);
        }
        try {
            String json = CharStreams.toString(new InputStreamReader(response.getInputStream(), Charsets.UTF_8));
            return new JsonResponse<>(response.getStatusCode(), response.getStatusMessage(), response.getHeaders(), jsonCodec, json);
        }
        catch (IOException e) {
            throw new RuntimeException("Error reading JSON response from server", e);
        }
    }

    public static class JsonResponse<T>
    {
        private final int statusCode;
        private final String statusMessage;
        private final ListMultimap<String, String> headers;
        private final boolean hasValue;
        private final JsonCodec<T> jsonCodec;
        private final String json;
        private T value;

        public JsonResponse(int statusCode, String statusMessage, ListMultimap<String, String> headers, JsonCodec<T> jsonCodec)
        {
            this.statusCode = statusCode;
            this.statusMessage = statusMessage;
            this.jsonCodec = jsonCodec;
            this.headers = ImmutableListMultimap.copyOf(headers);

            this.hasValue = false;
            this.json = null;
        }

        public JsonResponse(int statusCode, String statusMessage, ListMultimap<String, String> headers, JsonCodec<T> jsonCodec, String json)
        {
            this.statusCode = statusCode;
            this.statusMessage = statusMessage;
            this.jsonCodec = jsonCodec;
            this.headers = ImmutableListMultimap.copyOf(headers);

            this.hasValue = true;
            this.json = json;
        }

        public int getStatusCode()
        {
            return statusCode;
        }

        public String getStatusMessage()
        {
            return statusMessage;
        }

        public String getHeader(String name)
        {
            List<String> values = getHeaders().get(name);
            if (values.isEmpty()) {
                return null;
            }
            return values.get(0);
        }

        public ListMultimap<String, String> getHeaders()
        {
            return headers;
        }

        public boolean hasValue()
        {
            return json != null;
        }

        public T getValue()
        {
            Preconditions.checkState(hasValue, "Response does not contain a JSON value");
            if (value == null) {
                value = jsonCodec.fromJson(json);
            }
            return value;
        }

        @Override
        public String toString()
        {
            return Objects.toStringHelper(this)
                    .add("statusCode", statusCode)
                    .add("statusMessage", statusMessage)
                    .add("headers", headers)
                    .add("hasValue", hasValue)
                    .add("value", value)
                    .toString();
        }
    }
}
