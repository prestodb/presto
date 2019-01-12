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
package io.prestosql.server.security;

import com.google.common.base.CharMatcher;
import io.airlift.security.pem.PemReader;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jws;
import io.jsonwebtoken.JwsHeader;
import io.jsonwebtoken.JwtException;
import io.jsonwebtoken.JwtParser;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.SignatureException;
import io.jsonwebtoken.SigningKeyResolver;
import io.jsonwebtoken.UnsupportedJwtException;
import io.prestosql.spi.security.BasicPrincipal;

import javax.crypto.spec.SecretKeySpec;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;

import java.io.File;
import java.io.IOException;
import java.security.Key;
import java.security.Principal;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

import static com.google.common.base.CharMatcher.inRange;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.io.Files.asCharSource;
import static com.google.common.net.HttpHeaders.AUTHORIZATION;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.util.Base64.getMimeDecoder;
import static java.util.Objects.requireNonNull;

public class JsonWebTokenAuthenticator
        implements Authenticator
{
    private static final String DEFAULT_KEY = "default-key";
    private static final CharMatcher INVALID_KID_CHARS = inRange('a', 'z').or(inRange('A', 'Z')).or(inRange('0', '9')).or(CharMatcher.anyOf("_-")).negate();
    private static final String KEY_ID_VARIABLE = "${KID}";

    private final JwtParser jwtParser;
    private final Function<JwsHeader<?>, Key> keyLoader;

    @Inject
    public JsonWebTokenAuthenticator(JsonWebTokenConfig config)
    {
        requireNonNull(config, "config is null");

        if (config.getKeyFile().contains(KEY_ID_VARIABLE)) {
            keyLoader = new DynamicKeyLoader(config.getKeyFile());
        }
        else {
            keyLoader = new StaticKeyLoader(config.getKeyFile());
        }

        JwtParser jwtParser = Jwts.parser()
                .setSigningKeyResolver(new SigningKeyResolver()
                {
                    // interface uses raw types and this can not be fixed here
                    @SuppressWarnings("rawtypes")
                    @Override
                    public Key resolveSigningKey(JwsHeader header, Claims claims)
                    {
                        return keyLoader.apply(header);
                    }

                    @SuppressWarnings("rawtypes")
                    @Override
                    public Key resolveSigningKey(JwsHeader header, String plaintext)
                    {
                        return keyLoader.apply(header);
                    }
                });

        if (config.getRequiredIssuer() != null) {
            jwtParser.requireIssuer(config.getRequiredIssuer());
        }
        if (config.getRequiredAudience() != null) {
            jwtParser.requireAudience(config.getRequiredAudience());
        }
        this.jwtParser = jwtParser;
    }

    @Override
    public Principal authenticate(HttpServletRequest request)
            throws AuthenticationException
    {
        String header = nullToEmpty(request.getHeader(AUTHORIZATION));

        int space = header.indexOf(' ');
        if ((space < 0) || !header.substring(0, space).equalsIgnoreCase("bearer")) {
            throw needAuthentication(null);
        }
        String token = header.substring(space + 1).trim();
        if (token.isEmpty()) {
            throw needAuthentication(null);
        }

        try {
            Jws<Claims> claimsJws = jwtParser.parseClaimsJws(token);
            String subject = claimsJws.getBody().getSubject();
            return new BasicPrincipal(subject);
        }
        catch (JwtException e) {
            throw needAuthentication(e.getMessage());
        }
        catch (RuntimeException e) {
            throw new RuntimeException("Authentication error", e);
        }
    }

    private static AuthenticationException needAuthentication(String message)
    {
        return new AuthenticationException(message, "Bearer realm=\"Presto\", token_type=\"JWT\"");
    }

    private static class StaticKeyLoader
            implements Function<JwsHeader<?>, Key>
    {
        private final LoadedKey key;

        public StaticKeyLoader(String keyFile)
        {
            requireNonNull(keyFile, "keyFile is null");
            checkArgument(!keyFile.contains(KEY_ID_VARIABLE));
            this.key = loadKeyFile(new File(keyFile));
        }

        @Override
        public Key apply(JwsHeader<?> header)
        {
            SignatureAlgorithm algorithm = SignatureAlgorithm.forName(header.getAlgorithm());
            return key.getKey(algorithm);
        }
    }

    private static class DynamicKeyLoader
            implements Function<JwsHeader<?>, Key>
    {
        private final String keyFile;
        private final ConcurrentMap<String, LoadedKey> keys = new ConcurrentHashMap<>();

        public DynamicKeyLoader(String keyFile)
        {
            requireNonNull(keyFile, "keyFile is null");
            checkArgument(keyFile.contains(KEY_ID_VARIABLE));
            this.keyFile = keyFile;
        }

        @Override
        public Key apply(JwsHeader<?> header)
        {
            String keyId = getKeyId(header);
            SignatureAlgorithm algorithm = SignatureAlgorithm.forName(header.getAlgorithm());
            return keys.computeIfAbsent(keyId, this::loadKey).getKey(algorithm);
        }

        private static String getKeyId(JwsHeader<?> header)
        {
            String keyId = header.getKeyId();
            if (keyId == null) {
                // allow for migration from system not using kid
                return DEFAULT_KEY;
            }
            keyId = INVALID_KID_CHARS.replaceFrom(keyId, '_');
            return keyId;
        }

        private LoadedKey loadKey(String keyId)
        {
            return loadKeyFile(new File(keyFile.replace(KEY_ID_VARIABLE, keyId)));
        }
    }

    private static LoadedKey loadKeyFile(File file)
    {
        if (!file.canRead()) {
            throw new SignatureException("Unknown signing key ID");
        }

        // try to load the key as a PEM encoded public key
        try {
            return new LoadedKey(PemReader.loadPublicKey(file));
        }
        catch (Exception ignored) {
        }

        // try to load the key as a base64 encoded HMAC key
        try {
            String base64Key = asCharSource(file, US_ASCII).read();
            byte[] rawKey = getMimeDecoder().decode(base64Key.getBytes(US_ASCII));
            return new LoadedKey(rawKey);
        }
        catch (IOException ignored) {
        }

        throw new SignatureException("Unknown signing key id");
    }

    private static class LoadedKey
    {
        private final Key publicKey;
        private final byte[] hmacKey;

        public LoadedKey(Key publicKey)
        {
            this.publicKey = requireNonNull(publicKey, "publicKey is null");
            this.hmacKey = null;
        }

        public LoadedKey(byte[] hmacKey)
        {
            this.hmacKey = requireNonNull(hmacKey, "hmacKey is null");
            this.publicKey = null;
        }

        public Key getKey(SignatureAlgorithm algorithm)
        {
            if (algorithm.isHmac()) {
                if (hmacKey == null) {
                    throw new UnsupportedJwtException(format("JWT is signed with %s, but no HMAC key is configured", algorithm));
                }
                return new SecretKeySpec(hmacKey, algorithm.getJcaName());
            }

            if (publicKey == null) {
                throw new UnsupportedJwtException(format("JWT is signed with %s, but no key is configured", algorithm));
            }
            return publicKey;
        }
    }
}
