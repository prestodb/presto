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
package io.prestosql.proxy;

import io.airlift.security.pem.PemReader;
import io.jsonwebtoken.JwtBuilder;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;

import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.PrivateKey;
import java.security.interfaces.RSAPrivateKey;
import java.time.ZonedDateTime;
import java.util.Base64;
import java.util.Date;
import java.util.Optional;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkState;
import static io.jsonwebtoken.JwsHeader.KEY_ID;
import static java.nio.file.Files.readAllBytes;

public class JsonWebTokenHandler
{
    private final Optional<Consumer<JwtBuilder>> jwtSigner;
    private final Optional<String> jwtKeyId;
    private final Optional<String> jwtIssuer;
    private final Optional<String> jwtAudience;

    @Inject
    public JsonWebTokenHandler(JwtHandlerConfig config)
    {
        this.jwtSigner = setupJwtSigner(config.getJwtKeyFile(), config.getJwtKeyFilePassword());
        this.jwtKeyId = Optional.ofNullable(config.getJwtKeyId());
        this.jwtIssuer = Optional.ofNullable(config.getJwtIssuer());
        this.jwtAudience = Optional.ofNullable(config.getJwtAudience());
    }

    public boolean isConfigured()
    {
        return jwtSigner.isPresent();
    }

    public String getBearerToken(String subject)
    {
        checkState(jwtSigner.isPresent(), "not configured");

        JwtBuilder jwt = Jwts.builder()
                .setSubject(subject)
                .setExpiration(Date.from(ZonedDateTime.now().plusMinutes(5).toInstant()));

        jwtSigner.get().accept(jwt);
        jwtKeyId.ifPresent(keyId -> jwt.setHeaderParam(KEY_ID, keyId));
        jwtIssuer.ifPresent(jwt::setIssuer);
        jwtAudience.ifPresent(jwt::setAudience);

        return jwt.compact();
    }

    private static Optional<Consumer<JwtBuilder>> setupJwtSigner(File file, String password)
    {
        if (file == null) {
            return Optional.empty();
        }

        try {
            PrivateKey key = PemReader.loadPrivateKey(file, Optional.ofNullable(password));
            if (!(key instanceof RSAPrivateKey)) {
                throw new IOException("Only RSA private keys are supported");
            }
            return Optional.of(jwt -> jwt.signWith(SignatureAlgorithm.RS256, key));
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to load key file: " + file, e);
        }
        catch (GeneralSecurityException ignored) {
        }

        try {
            byte[] base64Key = readAllBytes(file.toPath());
            byte[] key = Base64.getMimeDecoder().decode(base64Key);
            return Optional.of(jwt -> jwt.signWith(SignatureAlgorithm.HS256, key));
        }
        catch (IOException | IllegalArgumentException e) {
            throw new RuntimeException("Failed to load key file: " + file, e);
        }
    }
}
