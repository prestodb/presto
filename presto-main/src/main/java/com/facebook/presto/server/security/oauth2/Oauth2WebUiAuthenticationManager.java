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

import com.facebook.airlift.http.server.AuthenticationException;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.server.security.WebUiAuthenticationManager;
import com.facebook.presto.server.security.oauth2.TokenPairSerializer.TokenPair;

import javax.inject.Inject;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.UriBuilder;

import java.io.IOException;
import java.security.Principal;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

import static com.facebook.presto.server.security.AuthenticationFilter.withPrincipal;
import static com.facebook.presto.server.security.oauth2.OAuth2CallbackResource.CALLBACK_ENDPOINT;
import static com.facebook.presto.server.security.oauth2.OAuth2Utils.getSchemeUriBuilder;
import static java.util.Objects.requireNonNull;

public class Oauth2WebUiAuthenticationManager
        implements WebUiAuthenticationManager
{
    private static final Logger logger = Logger.get(Oauth2WebUiAuthenticationManager.class);
    private final OAuth2Service oAuth2Service;
    private final OAuth2Authenticator oAuth2Authenticator;
    private final TokenPairSerializer tokenPairSerializer;
    private final OAuth2Client client;
    private final Optional<Duration> tokenExpiration;

    @Inject
    public Oauth2WebUiAuthenticationManager(OAuth2Service oAuth2Service, OAuth2Authenticator oAuth2Authenticator, TokenPairSerializer tokenPairSerializer, OAuth2Client client, @ForRefreshTokens Optional<Duration> tokenExpiration)
    {
        this.oAuth2Service = requireNonNull(oAuth2Service, "oauth2Service is null");
        this.oAuth2Authenticator = requireNonNull(oAuth2Authenticator, "oauth2Authenticator is null");
        this.tokenPairSerializer = requireNonNull(tokenPairSerializer, "tokenPairSerializer is null");
        this.client = requireNonNull(client, "oauth2Client is null");
        this.tokenExpiration = requireNonNull(tokenExpiration, "tokenExpiration is null");
    }

    public void handleRequest(HttpServletRequest request, HttpServletResponse response, FilterChain nextFilter)
            throws IOException, ServletException
    {
        try {
            Principal principal = this.oAuth2Authenticator.authenticate(request);
            nextFilter.doFilter(withPrincipal(request, principal), response);
        }
        catch (AuthenticationException e) {
            needAuthentication(request, response);
        }
    }

    private Optional<TokenPair> getTokenPair(HttpServletRequest request)
    {
        try {
            Optional<String> token = this.oAuth2Authenticator.extractTokenFromCookie(request);
            if (token.isPresent()) {
                return Optional.ofNullable(tokenPairSerializer.deserialize(token.get()));
            }
            else {
                return Optional.empty();
            }
        }
        catch (Exception e) {
            logger.error(e, "Exception occurred during token pair deserialization");
            return Optional.empty();
        }
    }

    private void needAuthentication(HttpServletRequest request, HttpServletResponse response)
            throws IOException
    {
        Optional<TokenPairSerializer.TokenPair> tokenPair = getTokenPair(request);
        Optional<String> refreshToken = tokenPair.flatMap(TokenPair::getRefreshToken);
        if (refreshToken.isPresent()) {
            try {
                OAuth2Client.Response refreshRes = client.refreshTokens(refreshToken.get());
                String serializeToken = tokenPairSerializer.serialize(TokenPair.fromOAuth2Response(refreshRes));
                UriBuilder builder = getSchemeUriBuilder(request);
                Cookie newCookie = NonceCookie.toServletCookie(OAuthWebUiCookie.create(serializeToken, tokenExpiration.map(expiration -> Instant.now().plus(expiration)).orElse(refreshRes.getExpiration())));
                response.addCookie(newCookie);
                response.sendRedirect(builder.build().toString());
            }
            catch (ChallengeFailedException e) {
                logger.error(e, "Token refresh challenge has failed");
                this.startOauth2Challenge(request, response);
            }
        }
        else {
            this.startOauth2Challenge(request, response);
        }
    }

    private void startOauth2Challenge(HttpServletRequest request, HttpServletResponse response)
            throws IOException
    {
        UriBuilder builder = getSchemeUriBuilder(request);
        this.oAuth2Service.startOAuth2Challenge(builder.build().resolve(CALLBACK_ENDPOINT), Optional.empty(), response);
    }
}
