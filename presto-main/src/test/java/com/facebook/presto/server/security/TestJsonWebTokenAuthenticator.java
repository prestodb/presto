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
package com.facebook.presto.server.security;

import com.facebook.airlift.http.server.AuthenticationException;
import com.facebook.presto.server.MockHttpServletRequest;
import com.facebook.presto.spi.security.AuthorizedIdentity;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import io.jsonwebtoken.Jwts;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import javax.servlet.http.HttpServletRequest;

import java.io.IOException;
import java.nio.file.Path;
import java.security.Principal;

import static com.facebook.presto.server.security.ServletSecurityUtils.AUTHORIZED_IDENTITY_ATTRIBUTE;
import static com.facebook.presto.server.security.ServletSecurityUtils.authorizedIdentity;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static com.google.common.io.Files.createTempDir;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static com.google.common.net.HttpHeaders.AUTHORIZATION;
import static io.jsonwebtoken.JwsHeader.KEY_ID;
import static io.jsonwebtoken.SignatureAlgorithm.HS256;
import static io.jsonwebtoken.security.Keys.secretKeyFor;
import static java.nio.file.Files.readAllBytes;
import static java.util.Base64.getMimeDecoder;
import static java.util.Base64.getMimeEncoder;

public class TestJsonWebTokenAuthenticator
{
    private static final String KEY_ID_FOO = "foo";
    private static final String TEST_PRINCIPAL = "testPrincipal";

    private Path temporaryDirectory;
    private Path keyFile;
    private JsonWebTokenConfig jsonWebTokenConfig;

    @BeforeTest
    public void setup()
            throws IOException
    {
        temporaryDirectory = createTempDir().toPath();
        keyFile = temporaryDirectory.resolve(KEY_ID_FOO + ".key");
        byte[] key = getMimeEncoder().encode(secretKeyFor(HS256).getEncoded());
        Files.write(key, keyFile.toFile());
        jsonWebTokenConfig = new JsonWebTokenConfig().setKeyFile(keyFile.toAbsolutePath().toString());
    }

    @AfterTest(alwaysRun = true)
    public void cleanup()
            throws IOException
    {
        deleteRecursively(temporaryDirectory, ALLOW_INSECURE);
    }

    @Test
    public void testJsonWebTokenWithAuthorizedUserClaim()
            throws IOException, AuthenticationException
    {
        AuthorizedIdentity authorizedIdentity = new AuthorizedIdentity("user", "reasonForSelect", false);
        String jsonWebToken = createJsonWebToken(keyFile, TEST_PRINCIPAL, authorizedIdentity);
        HttpServletRequest request = new MockHttpServletRequest(
                ImmutableListMultimap.of(AUTHORIZATION, "Bearer " + jsonWebToken),
                "remoteAddress",
                ImmutableMap.of());
        Principal principal = new JsonWebTokenAuthenticator(jsonWebTokenConfig).authenticate(request);

        assertEquals(principal.getName(), TEST_PRINCIPAL);
        assertEquals(authorizedIdentity(request).get(), authorizedIdentity);
    }

    private static String createJsonWebToken(Path keyFile, String principal, AuthorizedIdentity authorizedIdentity)
            throws IOException
    {
        byte[] key = getMimeDecoder().decode(readAllBytes(keyFile.toAbsolutePath()));
        return Jwts.builder()
                .signWith(HS256, key)
                .setHeaderParam(KEY_ID, KEY_ID_FOO)
                .setSubject(principal)
                .claim(AUTHORIZED_IDENTITY_ATTRIBUTE, authorizedIdentity)
                .compact();
    }
}
