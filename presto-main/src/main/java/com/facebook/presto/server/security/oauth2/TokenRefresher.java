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
import com.facebook.presto.server.security.oauth2.TokenPairSerializer.TokenPair;

import java.util.Optional;
import java.util.UUID;

import static com.facebook.presto.server.security.oauth2.OAuth2TokenExchange.hashAuthId;
import static java.util.Objects.requireNonNull;

public class TokenRefresher
{
    private final TokenPairSerializer tokenAssembler;
    private final OAuth2TokenHandler tokenHandler;
    private final OAuth2Client client;

    public TokenRefresher(TokenPairSerializer tokenAssembler, OAuth2TokenHandler tokenHandler, OAuth2Client client)
    {
        this.tokenAssembler = requireNonNull(tokenAssembler, "tokenAssembler is null");
        this.tokenHandler = requireNonNull(tokenHandler, "tokenHandler is null");
        this.client = requireNonNull(client, "oAuth2Client is null");
    }

    public Optional<UUID> refreshToken(TokenPair tokenPair)
    {
        requireNonNull(tokenPair, "tokenPair is null");

        Optional<String> refreshToken = tokenPair.getRefreshToken();
        if (refreshToken.isPresent()) {
            UUID refreshingId = UUID.randomUUID();
            try {
                refreshToken(refreshToken.get(), refreshingId);
                return Optional.of(refreshingId);
            }
            // If Refresh token has expired then restart the flow
            catch (RuntimeException exception) {
                return Optional.empty();
            }
        }
        return Optional.empty();
    }

    private void refreshToken(String refreshToken, UUID refreshingId)
    {
        try {
            Response response = client.refreshTokens(refreshToken);
            String serializedToken = tokenAssembler.serialize(TokenPair.fromOAuth2Response(response));
            tokenHandler.setAccessToken(hashAuthId(refreshingId), serializedToken);
        }
        catch (ChallengeFailedException e) {
            tokenHandler.setTokenExchangeError(hashAuthId(refreshingId), "Token refreshing has failed: " + e.getMessage());
        }
    }
}
