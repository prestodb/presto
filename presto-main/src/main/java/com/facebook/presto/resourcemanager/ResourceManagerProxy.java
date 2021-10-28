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
package com.facebook.presto.resourcemanager;

import com.facebook.airlift.http.client.BodyGenerator;
import com.facebook.airlift.http.client.HeaderName;
import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.http.client.Request;
import com.facebook.presto.metadata.InternalNodeManager;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.io.ByteStreams;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import io.airlift.units.Duration;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.Response;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URI;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.airlift.http.client.HttpStatus.INTERNAL_SERVER_ERROR;
import static com.facebook.airlift.http.server.AsyncResponseHandler.bindAsyncResponse;
import static com.google.common.base.Verify.verify;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.HttpHeaders.COOKIE;
import static com.google.common.net.HttpHeaders.USER_AGENT;
import static com.google.common.net.HttpHeaders.X_FORWARDED_FOR;
import static com.google.common.util.concurrent.Futures.transform;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.list;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static javax.ws.rs.core.MediaType.TEXT_PLAIN;
import static javax.ws.rs.core.Response.Status.GATEWAY_TIMEOUT;
import static javax.ws.rs.core.Response.status;

@SuppressWarnings("UnstableApiUsage")
public class ResourceManagerProxy
{
    private final InternalNodeManager internalNodeManager;
    private final HttpClient httpClient;
    private final Duration asyncTimeout;
    private final Executor executor;

    @Inject
    private ResourceManagerProxy(
            InternalNodeManager internalNodeManager,
            @ForResourceManager HttpClient httpClient,
            ResourceManagerConfig resourceManagerConfig,
            @ForResourceManager ListeningExecutorService executor)
    {
        this.internalNodeManager = requireNonNull(internalNodeManager, "internalNodeManager is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.asyncTimeout = requireNonNull(resourceManagerConfig, "resourceManagerConfig is null").getProxyAsyncTimeout();
        this.executor = requireNonNull(executor, "executor is null");
    }

    public void performRequest(
            HttpServletRequest servletRequest,
            AsyncResponse asyncResponse,
            URI remoteUri)
    {
        try {
            BodyGenerator bodyGenerator = new InputStreamBodyGenerator(servletRequest.getInputStream());
            Request request = createRequest(servletRequest, servletRequest.getMethod(), remoteUri, bodyGenerator);
            ListenableFuture<ProxyResponse> proxyResponse = httpClient.executeAsync(request, new ResponseHandler());
            ListenableFuture<Response> future = transform(proxyResponse, this::toResponse, executor);
            setupAsyncResponse(servletRequest, asyncResponse, future);
        }
        catch (IOException e) {
            asyncResponse.resume(e);
        }
    }

    private Request createRequest(HttpServletRequest servletRequest, String httpMethod, URI remoteUri, BodyGenerator bodyGenerator)
    {
        Request.Builder requestBuilder = new Request.Builder()
                .setMethod(httpMethod)
                .setUri(remoteUri)
                .setPreserveAuthorizationOnRedirect(true)
                .setBodyGenerator(bodyGenerator);

        for (String name : list(servletRequest.getHeaderNames())) {
            if (isPrestoHeader(name) || name.equalsIgnoreCase(COOKIE)) {
                for (String value : list(servletRequest.getHeaders(name))) {
                    requestBuilder.addHeader(name, value);
                }
            }
            else if (name.equalsIgnoreCase(USER_AGENT)) {
                for (String value : list(servletRequest.getHeaders(name))) {
                    requestBuilder.addHeader(name, "[Resource Manager] " + value);
                }
            }
        }

        StringBuilder xForwardedFor = new StringBuilder();
        if (servletRequest.getHeader(X_FORWARDED_FOR) != null) {
            xForwardedFor.append(servletRequest.getHeader(X_FORWARDED_FOR) + ",");
        }
        xForwardedFor.append(servletRequest.getRemoteAddr());
        requestBuilder.addHeader(X_FORWARDED_FOR, xForwardedFor.toString());

        return requestBuilder.build();
    }

    private static boolean isPrestoHeader(String name)
    {
        return name.toLowerCase(ENGLISH).startsWith("x-presto-");
    }

    private Response toResponse(ProxyResponse input)
    {
        Response.ResponseBuilder entity = status(input.getStatusCode()).entity(input.getBody());
        input.getHeaders().forEach(((headerName, value) -> entity.header(headerName.toString(), value)));
        return entity.build();
    }

    private void setupAsyncResponse(HttpServletRequest servletRequest, AsyncResponse asyncResponse, ListenableFuture<Response> future)
    {
        bindAsyncResponse(asyncResponse, future, executor)
                .withTimeout(asyncTimeout, () -> status(GATEWAY_TIMEOUT)
                        .type(TEXT_PLAIN)
                        .entity(format("Request to remote Presto server (%s), current node (%s), timed out after %s",
                                servletRequest.getRemoteAddr(),
                                internalNodeManager.getCurrentNode().getNodeIdentifier(),
                                asyncTimeout.toString()))
                        .build());
    }

    private static class InputStreamBodyGenerator
            implements BodyGenerator
    {
        private final InputStream inputStream;
        private final AtomicBoolean called = new AtomicBoolean();

        public InputStreamBodyGenerator(InputStream inputStream)
        {
            this.inputStream = requireNonNull(inputStream, "inputStream is null");
        }

        @Override
        public void write(OutputStream outputStream)
                throws Exception
        {
            verify(called.compareAndSet(false, true), "Already read servlet request body");
            try {
                ByteStreams.copy(inputStream, outputStream);
            }
            finally {
                inputStream.close();
            }
        }
    }

    private static class ResponseHandler
            implements com.facebook.airlift.http.client.ResponseHandler
    {
        @Override
        public ProxyResponse handleException(Request request, Exception exception)
        {
            StringWriter sw = new StringWriter();
            exception.printStackTrace(new PrintWriter(sw));
            String message = format("Exception receiving response from %s: %s", request.getUri(), sw.toString());
            InputStream inputStream = new ByteArrayInputStream(message.getBytes(UTF_8));
            return new ProxyResponse(INTERNAL_SERVER_ERROR.code(), ImmutableListMultimap.of(HeaderName.of(CONTENT_TYPE), TEXT_PLAIN), inputStream);
        }

        @Override
        public ProxyResponse handle(Request request, com.facebook.airlift.http.client.Response response)
        {
            try {
                return new ProxyResponse(response.getStatusCode(), response.getHeaders(), response.getInputStream());
            }
            catch (IOException e) {
                return handleException(request, e);
            }
        }
    }

    private static class ProxyResponse
    {
        private final int statusCode;
        private final ListMultimap<HeaderName, String> headers;
        private final InputStream body;

        ProxyResponse(int statusCode, ListMultimap<HeaderName, String> headers, InputStream body)
        {
            this.statusCode = statusCode;
            this.headers = requireNonNull(headers, "headers is null");
            this.body = requireNonNull(body, "body is null");
        }

        public int getStatusCode()
        {
            return statusCode;
        }

        public ListMultimap<HeaderName, String> getHeaders()
        {
            return headers;
        }

        public InputStream getBody()
        {
            return body;
        }
    }
}
