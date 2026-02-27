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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.server.security.oauth2.OAuth2Client.Response;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.Nullable;

import java.io.IOException;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static java.lang.Long.MAX_VALUE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public interface TokenPairSerializer
{
    /**
     * Serializer that stores access token and claims in a simple JSON format.
     * Used when refresh tokens are disabled but we still need to preserve claims for authentication.
     */
    TokenPairSerializer ACCESS_TOKEN_CLAIMS_ONLY_SERIALIZER = new TokenPairSerializer()
    {
        private static final Logger LOG = Logger.get(TokenPairSerializer.class);
        private final ObjectMapper objectMapper = new ObjectMapper();

        @Override
        public TokenPair deserialize(String token)
        {
            // Try to decode from Base64 and parse as JSON (new format with claims)
            try {
                // First try to decode from Base64
                byte[] decodedBytes = Base64.getDecoder().decode(token);
                String decodedJson = new String(decodedBytes, UTF_8);

                @SuppressWarnings("unchecked")
                Map<String, Object> data = objectMapper.readValue(decodedJson, Map.class);
                if (data.containsKey("accessToken") && data.containsKey("claims")) {
                    String accessToken = (String) data.get("accessToken");
                    @SuppressWarnings("unchecked")
                    Map<String, Object> claims = (Map<String, Object>) data.get("claims");
                    LOG.debug("Deserialized token with claims from new Base64-encoded JSON format");
                    return new TokenPair(accessToken, new Date(MAX_VALUE), Optional.empty(), Optional.of(claims));
                }
            }
            catch (IllegalArgumentException | IOException e) {
                // Not Base64-encoded JSON, treat as plain access token (backward compatibility)
                LOG.debug("Token is not in new Base64-encoded JSON format, treating as plain access token (old format or migration in progress)");
            }

            // Fallback: treat as plain access token (backward compatibility)
            LOG.debug("Using plain access token format (no claims available, will need to query on authentication)");
            return TokenPair.accessToken(token);
        }

        @Override
        public String serialize(TokenPair tokenPair)
        {
            // If claims are present, serialize as Base64-encoded JSON with access token and claims
            if (tokenPair.getClaims().isPresent()) {
                try {
                    Map<String, Object> data = new HashMap<>();
                    data.put("accessToken", tokenPair.getAccessToken());
                    data.put("claims", tokenPair.getClaims().get());
                    String json = objectMapper.writeValueAsString(data);
                    // Base64 encode to ensure cookie compatibility
                    return Base64.getEncoder().encodeToString(json.getBytes(UTF_8));
                }
                catch (JsonProcessingException e) {
                    throw new IllegalStateException("Failed to serialize token with claims", e);
                }
            }

            // Fallback: serialize as plain access token (backward compatibility)
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
        private final Optional<Map<String, Object>> claims;

        private TokenPair(String accessToken, Date expiration, Optional<String> refreshToken)
        {
            this(accessToken, expiration, refreshToken, Optional.empty());
        }

        private TokenPair(String accessToken, Date expiration, Optional<String> refreshToken, Optional<Map<String, Object>> claims)
        {
            this.accessToken = requireNonNull(accessToken, "accessToken is nul");
            this.expiration = requireNonNull(expiration, "expiration is null");
            this.refreshToken = requireNonNull(refreshToken, "refreshToken is null");
            this.claims = requireNonNull(claims, "claims is null");
        }

        public static TokenPair accessToken(String accessToken)
        {
            return new TokenPair(accessToken, new Date(MAX_VALUE), Optional.empty());
        }

        public static TokenPair fromOAuth2Response(Response tokens)
        {
            requireNonNull(tokens, "tokens is null");
            return new TokenPair(
                    tokens.getAccessToken(),
                    Date.from(tokens.getExpiration()),
                    tokens.getRefreshToken(),
                    tokens.getClaims());
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

        /**
         * Returns the user claims from ID token (for OIDC) or UserInfo endpoint (for OAuth2).
         * These claims should be used for extracting the principal field, not the access token.
         *
         * @return Optional containing claims map, or empty if not available
         */
        public Optional<Map<String, Object>> getClaims()
        {
            return claims;
        }

        public static TokenPair withAccessAndRefreshTokens(String accessToken, Date expiration, @Nullable String refreshToken)
        {
            return new TokenPair(accessToken, expiration, Optional.ofNullable(refreshToken));
        }
    }
}
