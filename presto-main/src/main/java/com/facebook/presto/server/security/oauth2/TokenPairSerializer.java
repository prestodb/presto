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

import com.facebook.presto.server.security.oauth2.OAuth2Client.Response;

import javax.annotation.Nullable;

import java.util.Date;
import java.util.Optional;

import static java.lang.Long.MAX_VALUE;
import static java.util.Objects.requireNonNull;

public interface TokenPairSerializer
{
    TokenPairSerializer ACCESS_TOKEN_ONLY_SERIALIZER = new TokenPairSerializer()
    {
        @Override
        public TokenPair deserialize(String token)
        {
            return TokenPair.accessToken(token);
        }

        @Override
        public String serialize(TokenPair tokenPair)
        {
            return tokenPair.getAccessToken();
        }
    };

    TokenPair deserialize(String token);

    String serialize(TokenPair tokenPair);

    class TokenPair
    {
        private final String accessToken;
        private final Date expiration;
        private final Optional<String> refreshToken;

        private TokenPair(String accessToken, Date expiration, Optional<String> refreshToken)
        {
            this.accessToken = requireNonNull(accessToken, "accessToken is nul");
            this.expiration = requireNonNull(expiration, "expiration is null");
            this.refreshToken = requireNonNull(refreshToken, "refreshToken is null");
        }

        public static TokenPair accessToken(String accessToken)
        {
            return new TokenPair(accessToken, new Date(MAX_VALUE), Optional.empty());
        }

        public static TokenPair fromOAuth2Response(Response tokens)
        {
            requireNonNull(tokens, "tokens is null");
            return new TokenPair(tokens.getAccessToken(), Date.from(tokens.getExpiration()), tokens.getRefreshToken());
        }

        public static TokenPair accessAndRefreshTokens(String accessToken, Date expiration, @Nullable String refreshToken)
        {
            return new TokenPair(accessToken, expiration, Optional.ofNullable(refreshToken));
        }

        public String getAccessToken()
        {
            return accessToken;
        }

        public Date getExpiration()
        {
            return expiration;
        }

        public Optional<String> getRefreshToken()
        {
            return refreshToken;
        }
    }
}
