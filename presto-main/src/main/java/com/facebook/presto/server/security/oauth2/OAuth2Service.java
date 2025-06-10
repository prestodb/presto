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
import com.google.common.io.Resources;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.JwtParser;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriBuilder;

import javax.inject.Inject;

import java.io.IOException;
import java.net.URI;
import java.security.Key;
import java.security.SecureRandom;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.TemporalAmount;
import java.util.Date;
import java.util.Optional;
import java.util.Random;

import static com.facebook.presto.server.security.oauth2.JwtUtil.newJwtBuilder;
import static com.facebook.presto.server.security.oauth2.JwtUtil.newJwtParserBuilder;
import static com.facebook.presto.server.security.oauth2.OAuth2Utils.getSchemeUriBuilder;
import static com.facebook.presto.server.security.oauth2.TokenPairSerializer.TokenPair.fromOAuth2Response;
import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.base.Verify.verify;
import static com.google.common.hash.Hashing.sha256;
import static io.jsonwebtoken.security.Keys.hmacShaKeyFor;
import static jakarta.ws.rs.core.Response.Status.BAD_REQUEST;
import static jakarta.ws.rs.core.Response.Status.FORBIDDEN;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.Instant.now;
import static java.util.Objects.requireNonNull;

public class OAuth2Service
{
    private static final Logger logger = Logger.get(OAuth2Service.class);

    public static final String OPENID_SCOPE = "openid";

    private static final String STATE_AUDIENCE_UI = "presto_oauth_ui";
    private static final String FAILURE_REPLACEMENT_TEXT = "<!-- ERROR_MESSAGE -->";
    private static final Random SECURE_RANDOM = new SecureRandom();
    public static final String HANDLER_STATE_CLAIM = "handler_state";

    private final OAuth2Client client;
    private final Optional<Duration> tokenExpiration;
    private final TokenPairSerializer tokenPairSerializer;

    private final String successHtml;
    private final String failureHtml;

    private final TemporalAmount challengeTimeout;
    private final Key stateHmac;
    private final JwtParser jwtParser;

    private final OAuth2TokenHandler tokenHandler;

    @Inject
    public OAuth2Service(
            OAuth2Client client,
            OAuth2Config oauth2Config,
            OAuth2TokenHandler tokenHandler,
            TokenPairSerializer tokenPairSerializer,
            @ForRefreshTokens Optional<Duration> tokenExpiration)
            throws IOException
    {
        this.client = requireNonNull(client, "client is null");
        requireNonNull(oauth2Config, "oauth2Config is null");

        this.successHtml = Resources.toString(Resources.getResource(getClass(), "/webapp/oauth2/success.html"), UTF_8);
        this.failureHtml = Resources.toString(Resources.getResource(getClass(), "/webapp/oauth2/failure.html"), UTF_8);
        verify(failureHtml.contains(FAILURE_REPLACEMENT_TEXT), "failure.html does not contain the replacement text");

        this.challengeTimeout = Duration.ofMillis(oauth2Config.getChallengeTimeout().toMillis());
        this.stateHmac = hmacShaKeyFor(oauth2Config.getStateKey()
                .map(key -> sha256().hashString(key, UTF_8).asBytes())
                .orElseGet(() -> secureRandomBytes(32)));
        this.jwtParser = newJwtParserBuilder()
                .setSigningKey(stateHmac)
                .requireAudience(STATE_AUDIENCE_UI)
                .build();

        this.tokenHandler = requireNonNull(tokenHandler, "tokenHandler is null");
        this.tokenPairSerializer = requireNonNull(tokenPairSerializer, "tokenPairSerializer is null");

        this.tokenExpiration = requireNonNull(tokenExpiration, "tokenExpiration is null");
    }

    public Response startOAuth2Challenge(URI callbackUri, Optional<String> handlerState)
    {
        Instant challengeExpiration = now().plus(challengeTimeout);
        String state = newJwtBuilder()
                .signWith(stateHmac)
                .setAudience(STATE_AUDIENCE_UI)
                .claim(HANDLER_STATE_CLAIM, handlerState.orElse(null))
                .setExpiration(Date.from(challengeExpiration))
                .compact();

        OAuth2Client.Request request = client.createAuthorizationRequest(state, callbackUri);
        Response.ResponseBuilder response = Response.seeOther(request.getAuthorizationUri());
        request.getNonce().ifPresent(nce -> response.cookie(NonceCookie.create(nce, challengeExpiration)));
        return response.build();
    }

    public void startOAuth2Challenge(URI callbackUri, Optional<String> handlerState, HttpServletResponse servletResponse)
            throws IOException
    {
        Instant challengeExpiration = now().plus(challengeTimeout);

        OAuth2Client.Request challengeRequest = this.startChallenge(callbackUri, handlerState);
        challengeRequest.getNonce().ifPresent(nce -> servletResponse.addCookie(NonceCookie.createServletCookie(nce, challengeExpiration)));
        servletResponseSeeOther(challengeRequest.getAuthorizationUri().toString(), servletResponse);
    }

    public void servletResponseSeeOther(String location, HttpServletResponse servletResponse)
            throws IOException
    {
        // 303 is preferred over a 302 when this response is received by a POST/PUT/DELETE and the redirect should be done via a GET instead of original method
        servletResponse.addHeader(HttpHeaders.LOCATION, location);
        servletResponse.sendError(HttpServletResponse.SC_SEE_OTHER);
    }

    private OAuth2Client.Request startChallenge(URI callbackUri, Optional<String> handlerState)
    {
        Instant challengeExpiration = now().plus(challengeTimeout);
        String state = newJwtBuilder()
                .signWith(stateHmac)
                .setAudience(STATE_AUDIENCE_UI)
                .claim(HANDLER_STATE_CLAIM, handlerState.orElse(null))
                .setExpiration(Date.from(challengeExpiration))
                .compact();

        return client.createAuthorizationRequest(state, callbackUri);
    }

    public Response handleOAuth2Error(String state, String error, String errorDescription, String errorUri)
    {
        try {
            Claims stateClaims = parseState(state);
            Optional.ofNullable(stateClaims.get(HANDLER_STATE_CLAIM, String.class))
                    .ifPresent(value ->
                            tokenHandler.setTokenExchangeError(value,
                                    format("Authentication response could not be verified: error=%s, errorDescription=%s, errorUri=%s",
                                            error, errorDescription, errorDescription)));
        }
        catch (ChallengeFailedException | RuntimeException e) {
            logger.error(e, "Authentication response could not be verified invalid state: state=%s", state);
            return Response.status(FORBIDDEN)
                    .entity(getInternalFailureHtml("Authentication response could not be verified"))
                    .cookie(NonceCookie.delete())
                    .build();
        }

        logger.error("OAuth server returned an error: error=%s, error_description=%s, error_uri=%s, state=%s", error, errorDescription, errorUri, state);
        return Response.ok()
                .entity(getCallbackErrorHtml(error))
                .cookie(NonceCookie.delete())
                .build();
    }

    public Response finishOAuth2Challenge(String state, String code, URI callbackUri, Optional<String> nonce, HttpServletRequest request)
    {
        Optional<String> handlerState;
        try {
            Claims stateClaims = parseState(state);
            handlerState = Optional.ofNullable(stateClaims.get(HANDLER_STATE_CLAIM, String.class));
        }
        catch (ChallengeFailedException | RuntimeException e) {
            logger.error(e, "Authentication response could not be verified invalid state: state=%s", state);
            return Response.status(BAD_REQUEST)
                    .entity(getInternalFailureHtml("Authentication response could not be verified"))
                    .cookie(NonceCookie.delete())
                    .build();
        }

        // Note: the Web UI may be disabled, so REST requests can not redirect to a success or error page inside the Web UI
        try {
            // fetch access token
            OAuth2Client.Response oauth2Response = client.getOAuth2Response(code, callbackUri, nonce);

            if (!handlerState.isPresent()) {
                UriBuilder uriBuilder = getSchemeUriBuilder(request);
                return Response
                        .seeOther(uriBuilder.build().resolve("/ui/"))
                        .cookie(
                                OAuthWebUiCookie.create(
                                        tokenPairSerializer.serialize(
                                                fromOAuth2Response(oauth2Response)),
                                                tokenExpiration
                                                        .map(expiration -> Instant.now().plus(expiration))
                                                        .orElse(oauth2Response.getExpiration())),
                                NonceCookie.delete())
                        .build();
            }

            tokenHandler.setAccessToken(handlerState.get(), tokenPairSerializer.serialize(fromOAuth2Response(oauth2Response)));

            Response.ResponseBuilder builder = Response.ok(getSuccessHtml());
            builder.cookie(
                    OAuthWebUiCookie.create(
                            tokenPairSerializer.serialize(fromOAuth2Response(oauth2Response)),
                            tokenExpiration.map(expiration -> Instant.now().plus(expiration))
                                    .orElse(oauth2Response.getExpiration())));

            return builder.cookie(NonceCookie.delete()).build();
        }
        catch (ChallengeFailedException | RuntimeException e) {
            logger.error(e, "Authentication response could not be verified: state=%s", state);

            handlerState.ifPresent(value ->
                    tokenHandler.setTokenExchangeError(value, format("Authentication response could not be verified: state=%s", value)));
            return Response.status(BAD_REQUEST)
                    .cookie(NonceCookie.delete())
                    .entity(getInternalFailureHtml("Authentication response could not be verified"))
                    .build();
        }
    }

    private Claims parseState(String state)
            throws ChallengeFailedException
    {
        try {
            return jwtParser
                    .parseClaimsJws(state)
                    .getBody();
        }
        catch (RuntimeException e) {
            throw new ChallengeFailedException("State validation failed", e);
        }
    }

    public String getSuccessHtml()
    {
        return successHtml;
    }

    public String getCallbackErrorHtml(String errorCode)
    {
        return failureHtml.replace(FAILURE_REPLACEMENT_TEXT, getOAuth2ErrorMessage(errorCode));
    }

    public String getInternalFailureHtml(String errorMessage)
    {
        return failureHtml.replace(FAILURE_REPLACEMENT_TEXT, nullToEmpty(errorMessage));
    }

    private static byte[] secureRandomBytes(int count)
    {
        byte[] bytes = new byte[count];
        SECURE_RANDOM.nextBytes(bytes);
        return bytes;
    }

    private static String getOAuth2ErrorMessage(String errorCode)
    {
        try {
            OAuth2ErrorCode code = OAuth2ErrorCode.fromString(errorCode);
            return code.getMessage();
        }
        catch (IllegalArgumentException e) {
            logger.error(e, "Unknown error code received code=%s", errorCode);
            return "OAuth2 unknown error code: " + errorCode;
        }
    }
}
