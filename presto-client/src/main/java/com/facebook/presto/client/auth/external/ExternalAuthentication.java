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
