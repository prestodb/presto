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

import com.facebook.presto.client.ClientException;
import com.google.common.annotations.VisibleForTesting;
import okhttp3.Authenticator;
import okhttp3.Challenge;
import okhttp3.Interceptor;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.Route;

import javax.annotation.Nullable;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;

import static com.google.common.net.HttpHeaders.AUTHORIZATION;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ExternalAuthenticator
        implements Authenticator, Interceptor
{
    public static final String TOKEN_URI_FIELD = "x_token_server";
    public static final String REDIRECT_URI_FIELD = "x_redirect_server";

    private final TokenPoller tokenPoller;
    private final RedirectHandler redirectHandler;
    private final Duration timeout;
    private final KnownToken knownToken;

    public ExternalAuthenticator(RedirectHandler redirect, TokenPoller tokenPoller, KnownToken knownToken, Duration timeout)
    {
        this.tokenPoller = requireNonNull(tokenPoller, "tokenPoller is null");
        this.redirectHandler = requireNonNull(redirect, "redirect is null");
        this.knownToken = requireNonNull(knownToken, "knownToken is null");
        this.timeout = requireNonNull(timeout, "timeout is null");
    }

    @Nullable
    @Override
    public Request authenticate(Route route, Response response)
    {
        knownToken.setupToken(() -> {
            Optional<ExternalAuthentication> authentication = toAuthentication(response);
            if (!authentication.isPresent()) {
                return Optional.empty();
            }

            return authentication.get().obtainToken(timeout, redirectHandler, tokenPoller);
        });

        return knownToken.getToken()
                .map(token -> withBearerToken(response.request(), token))
                .orElse(null);
    }

    @Override
    public Response intercept(Chain chain)
            throws IOException
    {
        Optional<Token> token = knownToken.getToken();
        if (token.isPresent()) {
            return chain.proceed(withBearerToken(chain.request(), token.get()));
        }

        return chain.proceed(chain.request());
    }

    private static Request withBearerToken(Request request, Token token)
    {
        return request.newBuilder()
                .header(AUTHORIZATION, "Bearer " + token.token())
                .build();
    }

    @VisibleForTesting
    static Optional<ExternalAuthentication> toAuthentication(Response response)
    {
        for (Challenge challenge : response.challenges()) {
            if (challenge.scheme().equalsIgnoreCase("Bearer")) {
                Optional<URI> tokenUri = parseField(challenge.authParams(), TOKEN_URI_FIELD);
                Optional<URI> redirectUri = parseField(challenge.authParams(), REDIRECT_URI_FIELD);
                if (tokenUri.isPresent()) {
                    return Optional.of(new ExternalAuthentication(tokenUri.get(), redirectUri));
                }
            }
        }

        return Optional.empty();
    }

    private static Optional<URI> parseField(Map<String, String> fields, String key)
    {
        return Optional.ofNullable(fields.get(key)).map(value -> {
            try {
                return new URI(value);
            }
            catch (URISyntaxException e) {
                throw new ClientException(format("Failed to parse URI for field '%s'", key), e);
            }
        });
    }
}
