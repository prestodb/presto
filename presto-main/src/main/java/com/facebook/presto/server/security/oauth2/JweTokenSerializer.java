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

import com.facebook.airlift.units.Duration;
import com.nimbusds.jose.EncryptionMethod;
import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWEAlgorithm;
import com.nimbusds.jose.JWEHeader;
import com.nimbusds.jose.JWEObject;
import com.nimbusds.jose.KeyLengthException;
import com.nimbusds.jose.Payload;
import com.nimbusds.jose.crypto.AESDecrypter;
import com.nimbusds.jose.crypto.AESEncrypter;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.CompressionCodec;
import io.jsonwebtoken.CompressionException;
import io.jsonwebtoken.Header;
import io.jsonwebtoken.JwtBuilder;
import io.jsonwebtoken.JwtParser;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;

import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.time.Clock;
import java.util.Date;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.server.security.oauth2.JwtUtil.newJwtBuilder;
import static com.facebook.presto.server.security.oauth2.JwtUtil.newJwtParserBuilder;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class JweTokenSerializer
        implements TokenPairSerializer
{
    private static final JWEAlgorithm ALGORITHM = JWEAlgorithm.A256KW;
    private static final EncryptionMethod ENCRYPTION_METHOD = EncryptionMethod.A256CBC_HS512;
    private static final CompressionCodec COMPRESSION_CODEC = new ZstdCodec();
    private static final String ACCESS_TOKEN_KEY = "access_token";
    private static final String EXPIRATION_TIME_KEY = "expiration_time";
    private static final String REFRESH_TOKEN_KEY = "refresh_token";
    private final OAuth2Client client;
    private final Clock clock;
    private final String issuer;
    private final String audience;
    private final Duration tokenExpiration;
    private final JwtParser parser;
    private final AESEncrypter jweEncrypter;
    private final AESDecrypter jweDecrypter;
    private final String principalField;

    public JweTokenSerializer(
            RefreshTokensConfig config,
            OAuth2Client client,
            String issuer,
            String audience,
            String principalField,
            Clock clock,
            Duration tokenExpiration)
            throws KeyLengthException, NoSuchAlgorithmException
    {
        SecretKey secretKey = createKey(requireNonNull(config, "config is null"));
        this.jweEncrypter = new AESEncrypter(secretKey);
        this.jweDecrypter = new AESDecrypter(secretKey);
        this.client = requireNonNull(client, "client is null");
        this.issuer = requireNonNull(issuer, "issuer is null");
        this.principalField = requireNonNull(principalField, "principalField is null");
        this.audience = requireNonNull(audience, "issuer is null");
        this.clock = requireNonNull(clock, "clock is null");
        this.tokenExpiration = requireNonNull(tokenExpiration, "tokenExpiration is null");

        this.parser = newJwtParserBuilder()
                .setClock(() -> Date.from(clock.instant()))
                .requireIssuer(this.issuer)
                .requireAudience(this.audience)
                .setCompressionCodecResolver(JweTokenSerializer::resolveCompressionCodec)
                .build();
    }

    @Override
    public TokenPair deserialize(String token)
    {
        requireNonNull(token, "token is null");

        try {
            JWEObject jwe = JWEObject.parse(token);
            jwe.decrypt(jweDecrypter);
            Claims claims = parser.parseClaimsJwt(jwe.getPayload().toString()).getBody();
            return TokenPair.accessAndRefreshTokens(
                    claims.get(ACCESS_TOKEN_KEY, String.class),
                    claims.get(EXPIRATION_TIME_KEY, Date.class),
                    claims.get(REFRESH_TOKEN_KEY, String.class));
        }
        catch (ParseException ex) {
            throw new IllegalArgumentException("Malformed jwt token", ex);
        }
        catch (JOSEException ex) {
            throw new IllegalArgumentException("Decryption failed", ex);
        }
    }

    @Override
    public String serialize(TokenPair tokenPair)
    {
        requireNonNull(tokenPair, "tokenPair is null");

        Optional<Map<String, Object>> accessTokenClaims = client.getClaims(tokenPair.getAccessToken());
        if (!accessTokenClaims.isPresent()) {
            throw new IllegalArgumentException("Claims are missing");
        }
        Map<String, Object> claims = accessTokenClaims.get();
        if (!claims.containsKey(principalField)) {
            throw new IllegalArgumentException(format("%s field is missing", principalField));
        }
        JwtBuilder jwt = newJwtBuilder()
                .setExpiration(Date.from(clock.instant().plusMillis(tokenExpiration.toMillis())))
                .claim(principalField, claims.get(principalField).toString())
                .setAudience(audience)
                .setIssuer(issuer)
                .claim(ACCESS_TOKEN_KEY, tokenPair.getAccessToken())
                .claim(EXPIRATION_TIME_KEY, tokenPair.getExpiration())
                .claim(REFRESH_TOKEN_KEY, tokenPair.getRefreshToken().orElseThrow(JweTokenSerializer::throwExceptionForNonExistingRefreshToken))
                .compressWith(COMPRESSION_CODEC);

        try {
            JWEObject jwe = new JWEObject(
                    new JWEHeader(ALGORITHM, ENCRYPTION_METHOD),
                    new Payload(jwt.compact()));
            jwe.encrypt(jweEncrypter);
            return jwe.serialize();
        }
        catch (JOSEException ex) {
            throw new IllegalStateException("Encryption failed", ex);
        }
    }

    private static SecretKey createKey(RefreshTokensConfig config)
            throws NoSuchAlgorithmException
    {
        SecretKey signingKey = config.getSecretKey();
        if (signingKey == null) {
            KeyGenerator generator = KeyGenerator.getInstance("AES");
            generator.init(256);
            return generator.generateKey();
        }
        return signingKey;
    }

    private static RuntimeException throwExceptionForNonExistingRefreshToken()
    {
        throw new IllegalStateException("Expected refresh token to be present. Please check your identity provider setup, or disable refresh tokens");
    }

    private static CompressionCodec resolveCompressionCodec(Header header)
            throws CompressionException
    {
        if (header.getCompressionAlgorithm() != null) {
            checkState(header.getCompressionAlgorithm().equals(ZstdCodec.CODEC_NAME), "Unknown codec '%s' used for token compression", header.getCompressionAlgorithm());
            return COMPRESSION_CODEC;
        }
        return null;
    }
}
