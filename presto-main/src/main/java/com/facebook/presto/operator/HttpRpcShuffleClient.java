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
package com.facebook.presto.operator;

import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.http.client.HttpStatus;
import com.facebook.airlift.http.client.Request;
import com.facebook.airlift.http.client.Response;
import com.facebook.airlift.http.client.ResponseHandler;
import com.facebook.airlift.http.client.ResponseTooLargeException;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.operator.PageBufferClient.PagesResponse;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.page.SerializedPage;
import com.google.common.collect.ImmutableList;
import com.google.common.net.MediaType;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.InputStreamSliceInput;
import io.airlift.slice.SliceInput;
import io.airlift.units.DataSize;

import javax.annotation.concurrent.ThreadSafe;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.List;
import java.util.Optional;

import static com.facebook.airlift.http.client.HttpStatus.familyForStatusCode;
import static com.facebook.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static com.facebook.airlift.http.client.Request.Builder.prepareDelete;
import static com.facebook.airlift.http.client.Request.Builder.prepareGet;
import static com.facebook.airlift.http.client.ResponseHandlerUtils.propagate;
import static com.facebook.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;
import static com.facebook.presto.PrestoMediaTypes.PRESTO_PAGES_TYPE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_BUFFER_COMPLETE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_MAX_SIZE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_PAGE_NEXT_TOKEN;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_PAGE_TOKEN;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_TASK_INSTANCE_ID;
import static com.facebook.presto.operator.PageBufferClient.PagesResponse.createEmptyPagesResponse;
import static com.facebook.presto.operator.PageBufferClient.PagesResponse.createPagesResponse;
import static com.facebook.presto.spi.page.PagesSerdeUtil.readSerializedPages;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public final class HttpRpcShuffleClient
        implements RpcShuffleClient
{
    private static final Logger log = Logger.get(HttpRpcShuffleClient.class);

    private final HttpClient httpClient;
    private final URI location;
    private final Optional<URI> asyncPageTransportLocation;

    public HttpRpcShuffleClient(HttpClient httpClient, URI location)
    {
        this(httpClient, location, Optional.empty());
    }

    public HttpRpcShuffleClient(HttpClient httpClient, URI location, Optional<URI> asyncPageTransportLocation)
    {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.location = requireNonNull(location, "location is null");
        this.asyncPageTransportLocation = requireNonNull(asyncPageTransportLocation, "asyncPageTransportLocation is null");
    }

    @Override
    public ListenableFuture<PagesResponse> getResults(long token, DataSize maxResponseSize)
    {
        URI uriBase = asyncPageTransportLocation.orElse(location);
        URI uri = uriBuilderFrom(uriBase).appendPath(String.valueOf(token)).build();
        return httpClient.executeAsync(
                prepareGet()
                        .setHeader(PRESTO_MAX_SIZE, maxResponseSize.toString())
                        .setUri(uri).build(),
                new PageResponseHandler());
    }

    @Override
    public void acknowledgeResultsAsync(long nextToken)
    {
        URI uri = uriBuilderFrom(location).appendPath(String.valueOf(nextToken)).appendPath("acknowledge").build();
        httpClient.executeAsync(prepareGet().setUri(uri).build(), new ResponseHandler<Void, RuntimeException>()
        {
            @Override
            public Void handleException(Request request, Exception exception)
            {
                log.debug(exception, "Acknowledge request failed: %s", uri);
                return null;
            }

            @Override
            public Void handle(Request request, Response response)
            {
                if (familyForStatusCode(response.getStatusCode()) != HttpStatus.Family.SUCCESSFUL) {
                    log.debug("Unexpected acknowledge response code: %s", response.getStatusCode());
                }
                return null;
            }
        });
    }

    @Override
    public ListenableFuture<?> abortResults()
    {
        return httpClient.executeAsync(prepareDelete().setUri(location).build(), createStatusResponseHandler());
    }

    @Override
    public Throwable rewriteException(Throwable throwable)
    {
        if (throwable instanceof ResponseTooLargeException) {
            return new PageTooLargeException(throwable);
        }
        return throwable;
    }

    public static class PageResponseHandler
            implements ResponseHandler<PagesResponse, RuntimeException>
    {
        @Override
        public PagesResponse handleException(Request request, Exception exception)
        {
            throw propagate(request, exception);
        }

        @Override
        public PagesResponse handle(Request request, Response response)
        {
            try {
                // no content means no content was created within the wait period, but query is still ok
                // if job is finished, complete is set in the response
                if (response.getStatusCode() == HttpStatus.NO_CONTENT.code()) {
                    return createEmptyPagesResponse(
                            getTaskInstanceId(request, response),
                            getToken(request, response),
                            getNextToken(request, response),
                            getComplete(request, response));
                }

                // otherwise we must have gotten an OK response, everything else is considered fatal
                if (response.getStatusCode() != HttpStatus.OK.code()) {
                    StringBuilder body = new StringBuilder();
                    try (BufferedReader reader = new BufferedReader(new InputStreamReader(response.getInputStream(), UTF_8))) {
                        // Get up to 1000 lines for debugging
                        for (int i = 0; i < 1000; i++) {
                            String line = reader.readLine();
                            // Don't output more than 100KB
                            if (line == null || body.length() + line.length() > 100 * 1024) {
                                break;
                            }
                            body.append(line + "\n");
                        }
                    }
                    catch (RuntimeException | IOException e) {
                        // Ignored. Just return whatever message we were able to decode
                    }
                    throw new PageTransportErrorException(
                            HostAddress.fromUri(request.getUri()),
                            format("Expected response code to be 200, but was %s %s:%n%s",
                                    response.getStatusCode(),
                                    response.getStatusMessage(),
                                    body.toString()));
                }

                // invalid content type can happen when an error page is returned, but is unlikely given the above 200
                String contentType = response.getHeader(CONTENT_TYPE);
                if (contentType == null) {
                    throw new PageTransportErrorException(
                            HostAddress.fromUri(request.getUri()),
                            format("%s header is not set: %s", CONTENT_TYPE, response));
                }
                if (!mediaTypeMatches(contentType, PRESTO_PAGES_TYPE)) {
                    throw new PageTransportErrorException(
                            HostAddress.fromUri(request.getUri()),
                            format("Expected %s response from server but got %s", PRESTO_PAGES_TYPE, contentType));
                }

                String taskInstanceId = getTaskInstanceId(request, response);
                long token = getToken(request, response);
                long nextToken = getNextToken(request, response);
                boolean complete = getComplete(request, response);

                try (SliceInput input = new InputStreamSliceInput(response.getInputStream())) {
                    List<SerializedPage> pages = ImmutableList.copyOf(readSerializedPages(input));
                    return createPagesResponse(taskInstanceId, token, nextToken, pages, complete);
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            catch (PageTransportErrorException e) {
                throw new PageTransportErrorException(
                        e.getRemoteHost(),
                        "Error fetching " + request.getUri().toASCIIString(),
                        e);
            }
        }

        private static String getTaskInstanceId(Request request, Response response)
        {
            String taskInstanceId = response.getHeader(PRESTO_TASK_INSTANCE_ID);
            if (taskInstanceId == null) {
                throw new PageTransportErrorException(HostAddress.fromUri(request.getUri()), format("Expected %s header", PRESTO_TASK_INSTANCE_ID));
            }
            return taskInstanceId;
        }

        private static long getToken(Request request, Response response)
        {
            String tokenHeader = response.getHeader(PRESTO_PAGE_TOKEN);
            if (tokenHeader == null) {
                throw new PageTransportErrorException(HostAddress.fromUri(request.getUri()), format("Expected %s header", PRESTO_PAGE_TOKEN));
            }
            return Long.parseLong(tokenHeader);
        }

        private static long getNextToken(Request request, Response response)
        {
            String nextTokenHeader = response.getHeader(PRESTO_PAGE_NEXT_TOKEN);
            if (nextTokenHeader == null) {
                throw new PageTransportErrorException(HostAddress.fromUri(request.getUri()), format("Expected %s header", PRESTO_PAGE_NEXT_TOKEN));
            }
            return Long.parseLong(nextTokenHeader);
        }

        private static boolean getComplete(Request request, Response response)
        {
            String bufferComplete = response.getHeader(PRESTO_BUFFER_COMPLETE);
            if (bufferComplete == null) {
                throw new PageTransportErrorException(HostAddress.fromUri(request.getUri()), format("Expected %s header", PRESTO_BUFFER_COMPLETE));
            }
            return Boolean.parseBoolean(bufferComplete);
        }

        private static boolean mediaTypeMatches(String value, MediaType range)
        {
            try {
                return MediaType.parse(value).is(range);
            }
            catch (IllegalArgumentException | IllegalStateException e) {
                return false;
            }
        }
    }
}
