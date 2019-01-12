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
package io.prestosql.proxy;

import com.google.common.collect.ListMultimap;
import com.google.common.net.MediaType;
import io.airlift.http.client.HeaderName;
import io.airlift.http.client.Request;
import io.airlift.http.client.Response;
import io.airlift.http.client.ResponseHandler;
import io.prestosql.proxy.ProxyResponseHandler.ProxyResponse;

import java.io.IOException;

import static com.google.common.io.ByteStreams.toByteArray;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static io.airlift.http.client.HttpStatus.NO_CONTENT;
import static io.airlift.http.client.HttpStatus.OK;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.util.Objects.requireNonNull;

public class ProxyResponseHandler
        implements ResponseHandler<ProxyResponse, RuntimeException>
{
    private static final MediaType MEDIA_TYPE_JSON = MediaType.create("application", "json");

    @Override
    public ProxyResponse handleException(Request request, Exception exception)
    {
        throw new ProxyException("Request to remote Presto server failed", exception);
    }

    @Override
    public ProxyResponse handle(Request request, Response response)
    {
        if (response.getStatusCode() == NO_CONTENT.code()) {
            return new ProxyResponse(response.getHeaders(), new byte[0]);
        }

        if (response.getStatusCode() != OK.code()) {
            throw new ProxyException(format("Bad status code from remote Presto server: %s: %s", response.getStatusCode(), readBody(response)));
        }

        String contentType = response.getHeader(CONTENT_TYPE);
        if (contentType == null) {
            throw new ProxyException("No Content-Type set in response from remote Presto server");
        }
        if (!MediaType.parse(contentType).is(MEDIA_TYPE_JSON)) {
            throw new ProxyException("Bad Content-Type from remote Presto server:" + contentType);
        }

        try {
            return new ProxyResponse(response.getHeaders(), toByteArray(response.getInputStream()));
        }
        catch (IOException e) {
            throw new ProxyException("Failed reading response from remote Presto server", e);
        }
    }

    private static String readBody(Response response)
    {
        try {
            return new String(toByteArray(response.getInputStream()), US_ASCII);
        }
        catch (IOException e) {
            return "";
        }
    }

    public static class ProxyResponse
    {
        private final ListMultimap<HeaderName, String> headers;
        private final byte[] body;

        private ProxyResponse(ListMultimap<HeaderName, String> headers, byte[] body)
        {
            this.headers = requireNonNull(headers, "headers is null");
            this.body = requireNonNull(body, "body is null");
        }

        public ListMultimap<HeaderName, String> getHeaders()
        {
            return headers;
        }

        public byte[] getBody()
        {
            return body;
        }
    }
}
