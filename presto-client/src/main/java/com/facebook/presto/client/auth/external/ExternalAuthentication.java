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

import java.net.URI;
import java.time.Duration;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

class ExternalAuthentication
{
    private final URI tokenUri;
    private final Optional<URI> redirectUri;

    public ExternalAuthentication(URI tokenUri, Optional<URI> redirectUri)
    {
        this.tokenUri = requireNonNull(tokenUri, "tokenUri is null");
        this.redirectUri = requireNonNull(redirectUri, "redirectUri is null");
    }

    public Optional<Token> obtainToken(Duration timeout, RedirectHandler handler, TokenPoller poller)
    {
        redirectUri.ifPresent(handler::redirectTo);
        //handler.redirectTo(URI.create("https://dev-59837273.okta.com/oauth2/default/v1/authorize"));

        URI currentUri = tokenUri;

        long start = System.nanoTime();
        long timeoutNanos = timeout.toNanos();

        while (true) {
            long remaining = timeoutNanos - (System.nanoTime() - start);
            if (remaining < 0) {
                return Optional.empty();
            }

            TokenPollResult result = poller.pollForToken(currentUri, Duration.ofNanos(remaining));

            if (result.isFailed()) {
                throw new ClientException(result.getError());
            }

            if (result.isPending()) {
                currentUri = result.getNextTokenUri();
                continue;
            }
            poller.tokenReceived(currentUri);
            return Optional.of(result.getToken());
        }
    }

//    public Optional<Token> obtainToken1(Duration timeout, RedirectHandler handler, TokenPoller poller) {
//        try {
//            // 1. Construct the proper authorization URL
//            String clientId = "YOUR_CLIENT_ID";
//            String redirectUriStr = "http://localhost:8400/callback";
//            URI redirectUri = new URI(redirectUriStr);
//
//            String authUrl = String.format(
//                    "https://dev-59837273.okta.com/oauth2/default/v1/authorize" +
//                            "?client_id=%s&response_type=code&scope=openid%%20email" +
//                            "&redirect_uri=%s&state=123",
//                    URLEncoder.encode(clientId, StandardCharsets.UTF_8),
//                    URLEncoder.encode(redirectUriStr, StandardCharsets.UTF_8));
//
//            // 2. Start a local HTTP server to listen for the callback
//            AuthorizationCodeReceiver codeReceiver = new AuthorizationCodeReceiver(8400);
//            codeReceiver.start();
//
//            // 3. Redirect the user to the auth URL (opens browser)
//            handler.redirectTo(URI.create(authUrl));
//
//            // 4. Wait for the code (within timeout)
//            String code = codeReceiver.waitForCode(timeout);
//            if (code == null) {
//                return Optional.empty(); // timeout
//            }
//
//            // 5. Poll/token exchange via POST to tokenUri
//            TokenPollResult result = poller.pollForToken(tokenUri, code, redirectUriStr);
//
//            if (result.isFailed()) {
//                throw new ClientException(result.getError());
//            }
//
//            if (result.isPending()) {
//                throw new ClientException("Unexpected pending status");
//            }
//
//            return Optional.of(result.getToken());
//
//        } catch (Exception e) {
//            throw new RuntimeException("Failed to obtain token", e);
//        }
//    }
    @VisibleForTesting
    Optional<URI> getRedirectUri()
    {
        return redirectUri;
    }

    @VisibleForTesting
    URI getTokenUri()
    {
        return tokenUri;
    }
}
