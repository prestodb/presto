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

import com.facebook.airlift.http.client.HeaderName;
import com.facebook.airlift.http.client.Request;
import com.facebook.airlift.http.client.Response;
import com.facebook.airlift.http.client.ResponseHandler;
import com.facebook.airlift.json.smile.SmileCodec;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.net.MediaType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

import static com.facebook.airlift.http.client.ResponseHandlerUtils.propagate;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.io.ByteStreams.toByteArray;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static java.util.Objects.requireNonNull;

public class FullSmileResponseHandler<T>
        implements ResponseHandler<FullSmileResponseHandler.SmileResponse<T>, RuntimeException>
{
    private static final MediaType MEDIA_TYPE_SMILE = MediaType.create("application", "x-jackson-smile");

    public static <T> FullSmileResponseHandler<T> createFullSmileResponseHandler(SmileCodec<T> smileCodec)
    {
        return new FullSmileResponseHandler<>(smileCodec);
    }

    private final SmileCodec<T> smileCodec;

    private FullSmileResponseHandler(SmileCodec<T> smileCodec)
    {
        this.smileCodec = smileCodec;
    }

    @Override
    public SmileResponse<T> handleException(Request request, Exception exception)
    {
        throw propagate(request, exception);
    }

    @Override
    public SmileResponse<T> handle(Request request, Response response)
    {
        byte[] bytes = readResponseBytes(response);
        String contentType = response.getHeader(CONTENT_TYPE);
        if ((contentType == null) || !MediaType.parse(contentType).is(MEDIA_TYPE_SMILE)) {
            return new SmileResponse<>(response.getStatusCode(), response.getStatusMessage(), response.getHeaders(), bytes);
        }
        return new SmileResponse<>(response.getStatusCode(), response.getStatusMessage(), response.getHeaders(), smileCodec, bytes);
    }

    private static byte[] readResponseBytes(Response response)
    {
        try {
            return toByteArray(response.getInputStream());
        }
        catch (IOException e) {
            throw new UncheckedIOException("Error reading response from server", e);
        }
    }

    public static class SmileResponse<T>
            implements BaseResponse<T>
    {
        private final int statusCode;
        private final String statusMessage;
        private final ListMultimap<HeaderName, String> headers;
        private final boolean hasValue;
        private final byte[] smileBytes;
        private final byte[] responseBytes;
        private final T value;
        private final IllegalArgumentException exception;

        public SmileResponse(int statusCode, String statusMessage, ListMultimap<HeaderName, String> headers, byte[] responseBytes)
        {
            this.statusCode = statusCode;
            this.statusMessage = statusMessage;
            this.headers = ImmutableListMultimap.copyOf(headers);

            this.hasValue = false;
            this.smileBytes = null;
            this.responseBytes = requireNonNull(responseBytes, "responseBytes is null");
            this.value = null;
            this.exception = null;
        }

        @SuppressWarnings("ThrowableInstanceNeverThrown")
        public SmileResponse(int statusCode, String statusMessage, ListMultimap<HeaderName, String> headers, SmileCodec<T> smileCodec, byte[] smileBytes)
        {
            this.statusCode = statusCode;
            this.statusMessage = statusMessage;
            this.headers = ImmutableListMultimap.copyOf(headers);

            this.smileBytes = requireNonNull(smileBytes, "smileBytes is null");
            this.responseBytes = smileBytes;

            T value = null;
            IllegalArgumentException exception = null;
            try {
                value = smileCodec.fromSmile(smileBytes);
            }
            catch (IllegalArgumentException e) {
                exception = new IllegalArgumentException("Unable to create " + smileCodec.getType() + " from SMILE response", e);
            }

            this.hasValue = (exception == null);
            this.value = value;
            this.exception = exception;
        }

        @Override
        public int getStatusCode()
        {
            return statusCode;
        }

        @Override
        public String getStatusMessage()
        {
            return statusMessage;
        }

        @Override
        public String getHeader(String name)
        {
            List<String> values = getHeaders().get(HeaderName.of(name));
            if (values.isEmpty()) {
                return null;
            }
            return values.get(0);
        }

        @Override
        public List<String> getHeaders(String name)
        {
            return headers.get(HeaderName.of(name));
        }

        @Override
        public ListMultimap<HeaderName, String> getHeaders()
        {
            return headers;
        }

        @Override
        public boolean hasValue()
        {
            return hasValue;
        }

        public T getValue()
        {
            if (!hasValue) {
                throw new IllegalStateException("Response does not contain a SMILE value", exception);
            }
            return value;
        }

        public byte[] getSmileBytes()
        {
            return (smileBytes == null) ? null : smileBytes.clone();
        }

        @Override
        public int getResponseSize()
        {
            return responseBytes.length;
        }

        @Override
        public byte[] getResponseBytes()
        {
            return responseBytes.clone();
        }

        @Override
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
                    .add("headers", headers)
                    .add("hasValue", hasValue)
                    .add("value", value)
                    .toString();
        }
    }
}
