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
package com.facebook.presto.server.security.oauth2;

import java.net.URI;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public interface OAuth2Client
{
    void load();

    Request createAuthorizationRequest(String state, URI callbackUri);

    Response getOAuth2Response(String code, URI callbackUri, Optional<String> nonce)
            throws ChallengeFailedException;

    Optional<Map<String, Object>> getClaims(String accessToken);

    Response refreshTokens(String refreshToken)
            throws ChallengeFailedException;

    class Request
    {
        private final URI authorizationUri;
        private final Optional<String> nonce;

        public Request(URI authorizationUri, Optional<String> nonce)
        {
            this.authorizationUri = requireNonNull(authorizationUri, "authorizationUri is null");
            this.nonce = requireNonNull(nonce, "nonce is null");
        }

        public URI getAuthorizationUri()
        {
            return authorizationUri;
        }

        public Optional<String> getNonce()
        {
            return nonce;
        }
    }

    class Response
    {
        private final String accessToken;
        private final Instant expiration;

        private final Optional<String> refreshToken;

        public Response(String accessToken, Instant expiration, Optional<String> refreshToken)
        {
            this.accessToken = requireNonNull(accessToken, "accessToken is null");
            this.expiration = requireNonNull(expiration, "expiration is null");
            this.refreshToken = requireNonNull(refreshToken, "refreshToken is null");
        }

        public String getAccessToken()
        {
            return accessToken;
        }

        public Instant getExpiration()
        {
            return expiration;
        }

        public Optional<String> getRefreshToken()
        {
            return refreshToken;
        }
    }
}
