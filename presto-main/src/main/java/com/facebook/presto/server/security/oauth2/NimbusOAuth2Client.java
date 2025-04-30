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
import com.facebook.presto.server.security.oauth2.OAuth2ServerConfigProvider.OAuth2ServerConfig;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Ordering;
import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.jwk.source.RemoteJWKSet;
import com.nimbusds.jose.proc.BadJOSEException;
import com.nimbusds.jose.proc.JWSKeySelector;
import com.nimbusds.jose.proc.JWSVerificationKeySelector;
import com.nimbusds.jose.proc.SecurityContext;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.proc.DefaultJWTClaimsVerifier;
import com.nimbusds.jwt.proc.DefaultJWTProcessor;
import com.nimbusds.jwt.proc.JWTProcessor;
import com.nimbusds.oauth2.sdk.AccessTokenResponse;
import com.nimbusds.oauth2.sdk.AuthorizationCode;
import com.nimbusds.oauth2.sdk.AuthorizationCodeGrant;
import com.nimbusds.oauth2.sdk.AuthorizationGrant;
import com.nimbusds.oauth2.sdk.AuthorizationRequest;
import com.nimbusds.oauth2.sdk.ParseException;
import com.nimbusds.oauth2.sdk.RefreshTokenGrant;
import com.nimbusds.oauth2.sdk.Scope;
import com.nimbusds.oauth2.sdk.TokenRequest;
import com.nimbusds.oauth2.sdk.auth.ClientSecretBasic;
import com.nimbusds.oauth2.sdk.auth.Secret;
import com.nimbusds.oauth2.sdk.id.ClientID;
import com.nimbusds.oauth2.sdk.id.Issuer;
import com.nimbusds.oauth2.sdk.id.State;
import com.nimbusds.oauth2.sdk.token.AccessToken;
import com.nimbusds.oauth2.sdk.token.BearerAccessToken;
import com.nimbusds.oauth2.sdk.token.RefreshToken;
import com.nimbusds.oauth2.sdk.token.Tokens;
import com.nimbusds.openid.connect.sdk.AuthenticationRequest;
import com.nimbusds.openid.connect.sdk.Nonce;
import com.nimbusds.openid.connect.sdk.OIDCTokenResponse;
import com.nimbusds.openid.connect.sdk.UserInfoRequest;
import com.nimbusds.openid.connect.sdk.UserInfoResponse;
import com.nimbusds.openid.connect.sdk.claims.AccessTokenHash;
import com.nimbusds.openid.connect.sdk.claims.IDTokenClaimsSet;
import com.nimbusds.openid.connect.sdk.token.OIDCTokens;
import com.nimbusds.openid.connect.sdk.validators.AccessTokenValidator;
import com.nimbusds.openid.connect.sdk.validators.IDTokenValidator;
import com.nimbusds.openid.connect.sdk.validators.InvalidHashException;
import io.airlift.units.Duration;

import javax.inject.Inject;

import java.net.MalformedURLException;
import java.net.URI;
import java.time.Instant;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.hash.Hashing.sha256;
import static com.nimbusds.oauth2.sdk.ResponseType.CODE;
import static com.nimbusds.openid.connect.sdk.OIDCScopeValue.OPENID;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

public class NimbusOAuth2Client
        implements OAuth2Client
{
    private static final Logger LOG = Logger.get(NimbusAirliftHttpClient.class);

    private final Issuer issuer;
    private final ClientID clientId;
    private final ClientSecretBasic clientAuth;
    private final Scope scope;
    private final String principalField;
    private final Set<String> accessTokenAudiences;
    private final Duration maxClockSkew;
    private final NimbusHttpClient httpClient;
    private final OAuth2ServerConfigProvider serverConfigurationProvider;
    private volatile boolean loaded;
    private URI authUrl;
    private URI tokenUrl;
    private Optional<URI> userinfoUrl;
    private JWSKeySelector<SecurityContext> jwsKeySelector;
    private JWTProcessor<SecurityContext> accessTokenProcessor;
    private AuthorizationCodeFlow flow;

    @Inject
    public NimbusOAuth2Client(OAuth2Config oauthConfig, OAuth2ServerConfigProvider serverConfigurationProvider, NimbusHttpClient httpClient)
    {
        requireNonNull(oauthConfig, "oauthConfig is null");
        issuer = new Issuer(oauthConfig.getIssuer());
        clientId = new ClientID(oauthConfig.getClientId());
        clientAuth = new ClientSecretBasic(clientId, new Secret(oauthConfig.getClientSecret()));
        scope = Scope.parse(oauthConfig.getScopes());
        principalField = oauthConfig.getPrincipalField();
        maxClockSkew = oauthConfig.getMaxClockSkew();

        accessTokenAudiences = new HashSet<>(oauthConfig.getAdditionalAudiences());
        accessTokenAudiences.add(clientId.getValue());
        accessTokenAudiences.add(null); // A null value in the set allows JWTs with no audience

        this.serverConfigurationProvider = requireNonNull(serverConfigurationProvider, "serverConfigurationProvider is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
    }

    @Override
    public void load()
    {
        OAuth2ServerConfig config = serverConfigurationProvider.get();
        this.authUrl = config.getAuthUrl();
        this.tokenUrl = config.getTokenUrl();
        this.userinfoUrl = config.getUserinfoUrl();
        try {
            jwsKeySelector = new JWSVerificationKeySelector<>(
                    Stream.concat(JWSAlgorithm.Family.RSA.stream(), JWSAlgorithm.Family.EC.stream()).collect(toImmutableSet()),
                    new RemoteJWKSet<>(config.getJwksUrl().toURL(), httpClient));
        }
        catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }

        DefaultJWTProcessor<SecurityContext> processor = new DefaultJWTProcessor<>();
        processor.setJWSKeySelector(jwsKeySelector);
        DefaultJWTClaimsVerifier<SecurityContext> accessTokenVerifier = new DefaultJWTClaimsVerifier<>(
                accessTokenAudiences,
                new JWTClaimsSet.Builder()
                        .issuer(config.getAccessTokenIssuer().orElse(issuer.getValue()))
                        .build(),
                ImmutableSet.of(principalField),
                ImmutableSet.of());
        accessTokenVerifier.setMaxClockSkew((int) maxClockSkew.roundTo(SECONDS));
        processor.setJWTClaimsSetVerifier(accessTokenVerifier);
        accessTokenProcessor = processor;
        flow = scope.contains(OPENID) ? new OAuth2WithOidcExtensionsCodeFlow() : new OAuth2AuthorizationCodeFlow();
        loaded = true;
    }

    @Override
    public Request createAuthorizationRequest(String state, URI callbackUri)
    {
        checkState(loaded, "OAuth2 client not initialized");
        return flow.createAuthorizationRequest(state, callbackUri);
    }

    @Override
    public Response getOAuth2Response(String code, URI callbackUri, Optional<String> nonce)
            throws ChallengeFailedException
    {
        checkState(loaded, "OAuth2 client not initialized");
        return flow.getOAuth2Response(code, callbackUri, nonce);
    }

    @Override
    public Optional<Map<String, Object>> getClaims(String accessToken)
    {
        checkState(loaded, "OAuth2 client not initialized");
        return getJWTClaimsSet(accessToken).map(JWTClaimsSet::getClaims);
    }

    @Override
    public Response refreshTokens(String refreshToken)
            throws ChallengeFailedException
    {
        checkState(loaded, "OAuth2 client not initialized");
        return flow.refreshTokens(refreshToken);
    }

    private interface AuthorizationCodeFlow
    {
        Request createAuthorizationRequest(String state, URI callbackUri);

        Response getOAuth2Response(String code, URI callbackUri, Optional<String> nonce)
                throws ChallengeFailedException;

        Response refreshTokens(String refreshToken)
                throws ChallengeFailedException;
    }

    private class OAuth2AuthorizationCodeFlow
            implements AuthorizationCodeFlow
    {
        @Override
        public Request createAuthorizationRequest(String state, URI callbackUri)
        {
            return new Request(
                    new AuthorizationRequest.Builder(CODE, clientId)
                            .redirectionURI(callbackUri)
                            .scope(scope)
                            .endpointURI(authUrl)
                            .state(new State(state))
                            .build()
                            .toURI(),
                    Optional.empty());
        }

        @Override
        public Response getOAuth2Response(String code, URI callbackUri, Optional<String> nonce)
                throws ChallengeFailedException
        {
            checkArgument(!nonce.isPresent(), "Unexpected nonce provided");
            AccessTokenResponse tokenResponse = getTokenResponse(code, callbackUri, AccessTokenResponse::parse);
            Tokens tokens = tokenResponse.toSuccessResponse().getTokens();
            return toResponse(tokens, Optional.empty());
        }

        @Override
        public Response refreshTokens(String refreshToken)
                throws ChallengeFailedException
        {
            requireNonNull(refreshToken, "refreshToken is null");
            AccessTokenResponse tokenResponse = getTokenResponse(refreshToken, AccessTokenResponse::parse);
            return toResponse(tokenResponse.toSuccessResponse().getTokens(), Optional.of(refreshToken));
        }

        private Response toResponse(Tokens tokens, Optional<String> existingRefreshToken)
                throws ChallengeFailedException
        {
            AccessToken accessToken = tokens.getAccessToken();
            RefreshToken refreshToken = tokens.getRefreshToken();
            JWTClaimsSet claims = getJWTClaimsSet(accessToken.getValue()).orElseThrow(() -> new ChallengeFailedException("invalid access token"));
            return new Response(
                    accessToken.getValue(),
                    determineExpiration(getExpiration(accessToken), claims.getExpirationTime()),
                    buildRefreshToken(refreshToken, existingRefreshToken));
        }
    }

    private class OAuth2WithOidcExtensionsCodeFlow
            implements AuthorizationCodeFlow
    {
        private final IDTokenValidator idTokenValidator;

        public OAuth2WithOidcExtensionsCodeFlow()
        {
            idTokenValidator = new IDTokenValidator(issuer, clientId, jwsKeySelector, null);
            idTokenValidator.setMaxClockSkew((int) maxClockSkew.roundTo(SECONDS));
        }

        @Override
        public Request createAuthorizationRequest(String state, URI callbackUri)
        {
            String nonce = new Nonce().getValue();
            return new Request(
                    new AuthenticationRequest.Builder(CODE, scope, clientId, callbackUri)
                            .endpointURI(authUrl)
                            .state(new State(state))
                            .nonce(new Nonce(hashNonce(nonce)))
                            .build()
                            .toURI(),
                    Optional.of(nonce));
        }

        @Override
        public Response getOAuth2Response(String code, URI callbackUri, Optional<String> nonce)
                throws ChallengeFailedException
        {
            if (!nonce.isPresent()) {
                throw new ChallengeFailedException("Missing nonce");
            }

            OIDCTokenResponse tokenResponse = getTokenResponse(code, callbackUri, OIDCTokenResponse::parse);
            OIDCTokens tokens = tokenResponse.getOIDCTokens();
            validateTokens(tokens, nonce);
            return toResponse(tokens, Optional.empty());
        }

        @Override
        public Response refreshTokens(String refreshToken)
                throws ChallengeFailedException
        {
            OIDCTokenResponse tokenResponse = getTokenResponse(refreshToken, OIDCTokenResponse::parse);
            OIDCTokens tokens = tokenResponse.getOIDCTokens();
            validateTokens(tokens);
            return toResponse(tokens, Optional.of(refreshToken));
        }

        private Response toResponse(OIDCTokens tokens, Optional<String> existingRefreshToken)
                throws ChallengeFailedException
        {
            AccessToken accessToken = tokens.getAccessToken();
            RefreshToken refreshToken = tokens.getRefreshToken();
            JWTClaimsSet claims = getJWTClaimsSet(accessToken.getValue()).orElseThrow(() -> new ChallengeFailedException("invalid access token"));
            return new Response(
                    accessToken.getValue(),
                    determineExpiration(getExpiration(accessToken), claims.getExpirationTime()),
                    buildRefreshToken(refreshToken, existingRefreshToken));
        }

        private void validateTokens(OIDCTokens tokens, Optional<String> nonce)
                throws ChallengeFailedException
        {
            try {
                IDTokenClaimsSet idToken = idTokenValidator.validate(
                        tokens.getIDToken(),
                        nonce.map(this::hashNonce)
                                .map(Nonce::new)
                                .orElse(null));
                AccessTokenHash accessTokenHash = idToken.getAccessTokenHash();
                if (accessTokenHash != null) {
                    AccessTokenValidator.validate(tokens.getAccessToken(), ((JWSHeader) tokens.getIDToken().getHeader()).getAlgorithm(), accessTokenHash);
                }
            }
            catch (BadJOSEException | JOSEException | InvalidHashException e) {
                throw new ChallengeFailedException("Cannot validate tokens", e);
            }
        }

        private void validateTokens(OIDCTokens tokens)
                throws ChallengeFailedException
        {
            validateTokens(tokens, Optional.empty());
        }

        private String hashNonce(String nonce)
        {
            return sha256()
                    .hashString(nonce, UTF_8)
                    .toString();
        }
    }

    private <T extends AccessTokenResponse> T getTokenResponse(String code, URI callbackUri, NimbusAirliftHttpClient.Parser<T> parser)
            throws ChallengeFailedException
    {
        return getTokenResponse(new AuthorizationCodeGrant(new AuthorizationCode(code), callbackUri), parser);
    }

    private <T extends AccessTokenResponse> T getTokenResponse(String refreshToken, NimbusAirliftHttpClient.Parser<T> parser)
            throws ChallengeFailedException
    {
        return getTokenResponse(new RefreshTokenGrant(new RefreshToken(refreshToken)), parser);
    }

    private <T extends AccessTokenResponse> T getTokenResponse(AuthorizationGrant authorizationGrant, NimbusAirliftHttpClient.Parser<T> parser)
            throws ChallengeFailedException
    {
        T tokenResponse = httpClient.execute(new TokenRequest(tokenUrl, clientAuth, authorizationGrant, scope), parser);
        if (!tokenResponse.indicatesSuccess()) {
            throw new ChallengeFailedException("Error while fetching access token: " + tokenResponse.toErrorResponse().toJSONObject());
        }
        return tokenResponse;
    }

    private Optional<JWTClaimsSet> getJWTClaimsSet(String accessToken)
    {
        if (userinfoUrl.isPresent()) {
            return queryUserInfo(accessToken);
        }
        return parseAccessToken(accessToken);
    }

    private Optional<JWTClaimsSet> queryUserInfo(String accessToken)
    {
        try {
            UserInfoResponse response = httpClient.execute(new UserInfoRequest(userinfoUrl.get(), new BearerAccessToken(accessToken)), UserInfoResponse::parse);
            if (!response.indicatesSuccess()) {
                LOG.error("Received bad response from userinfo endpoint: " + response.toErrorResponse().getErrorObject());
                return Optional.empty();
            }
            return Optional.of(response.toSuccessResponse().getUserInfo().toJWTClaimsSet());
        }
        catch (ParseException | RuntimeException e) {
            LOG.error(e, "Received bad response from userinfo endpoint");
            return Optional.empty();
        }
    }

    private Optional<JWTClaimsSet> parseAccessToken(String accessToken)
    {
        try {
            return Optional.of(accessTokenProcessor.process(accessToken, null));
        }
        catch (java.text.ParseException | BadJOSEException | JOSEException e) {
            LOG.error(e, "Failed to parse JWT access token");
            return Optional.empty();
        }
    }

    private static Instant determineExpiration(Optional<Instant> validUntil, Date expiration)
            throws ChallengeFailedException
    {
        if (validUntil.isPresent()) {
            if (expiration != null) {
                return Ordering.natural().min(validUntil.get(), expiration.toInstant());
            }

            return validUntil.get();
        }

        if (expiration != null) {
            return expiration.toInstant();
        }

        throw new ChallengeFailedException("no valid expiration date");
    }

    private Optional<String> buildRefreshToken(RefreshToken refreshToken, Optional<String> existingRefreshToken)
    {
        Optional<String> firstOption = Optional.ofNullable(refreshToken)
                .map(RefreshToken::getValue);

        if (firstOption.isPresent()) {
            return firstOption;
        }
        else if (existingRefreshToken.isPresent()) {
            return existingRefreshToken;
        }
        else {
            return Optional.empty();
        }
    }

    private static Optional<Instant> getExpiration(AccessToken accessToken)
    {
        return accessToken.getLifetime() != 0 ? Optional.of(Instant.now().plusSeconds(accessToken.getLifetime())) : Optional.empty();
    }
}
