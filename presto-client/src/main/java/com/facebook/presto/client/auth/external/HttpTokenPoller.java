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
package com.facebook.presto.client.auth.external;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.client.JsonResponse;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.FailsafeException;
import net.jodah.failsafe.RetryPolicy;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.time.Duration;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.presto.client.JsonResponse.execute;
import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.net.HttpHeaders.USER_AGENT;
import static java.lang.String.format;
import static java.net.HttpURLConnection.HTTP_INTERNAL_ERROR;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.net.HttpURLConnection.HTTP_UNAVAILABLE;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.Objects.requireNonNull;

public class HttpTokenPoller
        implements TokenPoller
{
    private static final JsonCodec<TokenPollRepresentation> TOKEN_POLL_CODEC = jsonCodec(TokenPollRepresentation.class);
    private static final String USER_AGENT_VALUE = "PrestoTokenPoller/" +
            firstNonNull(HttpTokenPoller.class.getPackage().getImplementationVersion(), "unknown");

    private final Supplier<OkHttpClient> client;

    public HttpTokenPoller(OkHttpClient client)
    {
        requireNonNull(client, "client is null");
        this.client = () -> client;
    }

    public HttpTokenPoller(OkHttpClient client, Consumer<OkHttpClient.Builder> refreshableClientConfig)
    {
        requireNonNull(client, "client is null");
        requireNonNull(refreshableClientConfig, "refreshableClientConfig is null");

        this.client = () -> {
            OkHttpClient.Builder builder = client.newBuilder();
            refreshableClientConfig.accept(builder);
            return builder.build();
        };
    }

    @Override
    public TokenPollResult pollForToken(URI tokenUri, Duration timeout)
    {
        try {
            return Failsafe.with(new RetryPolicy<TokenPollResult>()
                    .withMaxAttempts(-1)
                    .withMaxDuration(timeout)
                    .withBackoff(100, 500, MILLIS)
                    .handle(IOException.class))
                    .get(() -> executePoll(prepareRequestBuilder(tokenUri).build()));
        }
        catch (FailsafeException e) {
            if (e.getCause() instanceof IOException) {
                throw new UncheckedIOException((IOException) e.getCause());
            }
            throw e;
        }
    }

    @Override
    public void tokenReceived(URI tokenUri)
    {
        try {
            Failsafe.with(new RetryPolicy<Integer>()
                    .withMaxAttempts(-1)
                    .withMaxDuration(Duration.ofSeconds(4))
                    .withBackoff(100, 500, MILLIS)
                    .handleResultIf(code -> code >= HTTP_INTERNAL_ERROR))
                    .get(() -> {
                        Request request = prepareRequestBuilder(tokenUri)
                                .delete()
                                .build();
                        try (Response response = client.get().newCall(request)
                                .execute()) {
                            return response.code();
                        }
                    });
        }
        catch (FailsafeException e) {
            if (e.getCause() instanceof IOException) {
                throw new UncheckedIOException((IOException) e.getCause());
            }
            throw e;
        }
    }

    private static Request.Builder prepareRequestBuilder(URI tokenUri)
    {
        HttpUrl url = HttpUrl.get(tokenUri);
        if (url == null) {
            throw new IllegalArgumentException("Invalid token URI: " + tokenUri);
        }

        return new Request.Builder()
                .url(url)
                .addHeader(USER_AGENT, USER_AGENT_VALUE);
    }

    private TokenPollResult executePoll(Request request)
            throws IOException
    {
        JsonResponse<TokenPollRepresentation> response = executeRequest(request);

        if ((response.getStatusCode() == HTTP_OK) && response.hasValue()) {
            return response.getValue().toResult();
        }

        Optional<String> responseBody = Optional.ofNullable(response.getResponseBody());
        String message = format("Request to %s failed: %s [Error: %s]", request.url(), response, responseBody.orElse("<Response Too Large>"));

        if (response.getStatusCode() == HTTP_UNAVAILABLE) {
            throw new IOException(message);
        }

        return TokenPollResult.failed(message);
    }

    private JsonResponse<TokenPollRepresentation> executeRequest(Request request)
            throws IOException
    {
        try {
            return execute(TOKEN_POLL_CODEC, client.get(), request);
        }
        catch (UncheckedIOException e) {
            throw e.getCause();
        }
    }

    public static class TokenPollRepresentation
    {
        private final String token;
        private final URI nextUri;
        private final String error;

        @JsonCreator
        public TokenPollRepresentation(
                @JsonProperty("token") String token,
                @JsonProperty("nextUri") URI nextUri,
                @JsonProperty("error") String error)
        {
            this.token = token;
            this.nextUri = nextUri;
            this.error = error;
        }

        TokenPollResult toResult()
        {
            if (token != null) {
                return TokenPollResult.successful(new Token(token));
            }
            if (error != null) {
                return TokenPollResult.failed(error);
            }
            if (nextUri != null) {
                return TokenPollResult.pending(nextUri);
            }
            return TokenPollResult.failed("Failed to poll for token. No fields set in response.");
        }
    }
}
