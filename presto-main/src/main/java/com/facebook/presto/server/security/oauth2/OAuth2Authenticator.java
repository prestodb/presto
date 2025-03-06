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
import com.facebook.airlift.http.server.Authenticator;
import com.facebook.airlift.http.server.BasicPrincipal;
import com.facebook.airlift.log.Logger;
import org.apache.commons.lang3.StringUtils;

import javax.inject.Inject;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;

import java.net.URI;
import java.security.Principal;
import java.sql.Date;
import java.time.Instant;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static com.facebook.presto.server.security.oauth2.OAuth2TokenExchangeResource.getInitiateUri;
import static com.facebook.presto.server.security.oauth2.OAuth2TokenExchangeResource.getTokenUri;
import static com.facebook.presto.server.security.oauth2.OAuth2Utils.getSchemeUriBuilder;
import static com.facebook.presto.server.security.oauth2.OAuthWebUiCookie.OAUTH2_COOKIE;
import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.net.HttpHeaders.AUTHORIZATION;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class OAuth2Authenticator
        implements Authenticator
{
    private static final Logger Log = Logger.get(OAuth2Authenticator.class);
    private final String principalField;
    private final OAuth2Client client;
    private final TokenPairSerializer tokenPairSerializer;
    private final TokenRefresher tokenRefresher;

    @Inject
    public OAuth2Authenticator(OAuth2Client client, OAuth2Config config, TokenRefresher tokenRefresher, TokenPairSerializer tokenPairSerializer)
    {
        this.client = requireNonNull(client, "service is null");
        this.principalField = config.getPrincipalField();
        requireNonNull(config, "oauth2Config is null");
        this.tokenRefresher = requireNonNull(tokenRefresher, "tokenRefresher is null");
        this.tokenPairSerializer = requireNonNull(tokenPairSerializer, "tokenPairSerializer is null");
    }

    public Principal authenticate(HttpServletRequest request) throws AuthenticationException
    {
        String token = extractToken(request);
        TokenPairSerializer.TokenPair tokenPair;
        try {
            tokenPair = tokenPairSerializer.deserialize(token);
        }
        catch (IllegalArgumentException e) {
            Log.error(e, "Failed to deserialize the OAuth token");
            throw needAuthentication(request, Optional.empty(), "Invalid Credentials");
        }

        if (tokenPair.getExpiration().before(Date.from(Instant.now()))) {
            throw needAuthentication(request, Optional.of(token), "Invalid Credentials");
        }
        Optional<Map<String, Object>> claims = client.getClaims(tokenPair.getAccessToken());

        if (!claims.isPresent()) {
            throw needAuthentication(request, Optional.ofNullable(token), "Invalid Credentials");
        }
        String principal = (String) claims.get().get(principalField);
        if (StringUtils.isEmpty(principal)) {
            Log.warn("The subject is not present we need to authenticate");
            needAuthentication(request, Optional.empty(), "Invalid Credentials");
        }

        return new BasicPrincipal(principal);
    }

    public String extractToken(HttpServletRequest request) throws AuthenticationException
    {
        Optional<String> cookieToken = this.extractTokenFromCookie(request);
        Optional<String> headerToken = this.extractTokenFromHeader(request);

        if (!cookieToken.isPresent() && !headerToken.isPresent()) {
            throw needAuthentication(request, Optional.empty(), "Invalid Credentials");
        }

        return cookieToken.orElseGet(() -> headerToken.get());
    }

    public Optional<String> extractTokenFromHeader(HttpServletRequest request)
    {
        String authHeader = nullToEmpty(request.getHeader(AUTHORIZATION));
        int space = authHeader.indexOf(' ');
        if ((space < 0) || !authHeader.substring(0, space).equalsIgnoreCase("bearer")) {
            return Optional.empty();
        }

        return Optional.ofNullable(authHeader.substring(space + 1).trim())
                .filter(t -> !t.isEmpty());
    }

    public static Optional<String> extractTokenFromCookie(HttpServletRequest request)
    {
        Cookie[] cookies = Optional.ofNullable(request.getCookies()).orElse(new Cookie[0]);
        return Optional.ofNullable(Arrays.stream(cookies)
                .filter(cookie -> cookie.getName().equals(OAUTH2_COOKIE))
                .findFirst()
                .map(c -> c.getValue())
                .orElse(null));
    }

    private AuthenticationException needAuthentication(HttpServletRequest request, Optional<String> currentToken, String message)
    {
        URI baseUri = getSchemeUriBuilder(request).build();
        return currentToken
                .map(tokenPairSerializer::deserialize)
                .flatMap(tokenRefresher::refreshToken)
                .map(refreshId -> baseUri.resolve(getTokenUri(refreshId)))
                .map(tokenUri -> new AuthenticationException(message, format("Bearer x_token_server=\"%s\"", tokenUri)))
                .orElseGet(() -> buildNeedAuthentication(request, message));
    }

    private AuthenticationException buildNeedAuthentication(HttpServletRequest request, String message)
    {
        UUID authId = UUID.randomUUID();
        URI baseUri = getSchemeUriBuilder(request).build();
        URI initiateUri = baseUri.resolve(getInitiateUri(authId));
        URI tokenUri = baseUri.resolve(getTokenUri(authId));

        return new AuthenticationException(message, format("Bearer x_redirect_server=\"%s\", x_token_server=\"%s\"", initiateUri, tokenUri));
    }
}
