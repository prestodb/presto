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

import com.google.common.base.Splitter;
import io.prestosql.spi.security.AccessDeniedException;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;

import java.security.Principal;
import java.util.Base64;
import java.util.List;

import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.net.HttpHeaders.AUTHORIZATION;
import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static java.util.Objects.requireNonNull;

public class PasswordAuthenticator
        implements Authenticator
{
    private final PasswordAuthenticatorManager authenticatorManager;

    @Inject
    public PasswordAuthenticator(PasswordAuthenticatorManager authenticatorManager)
    {
        this.authenticatorManager = requireNonNull(authenticatorManager, "authenticatorManager is null");
        authenticatorManager.setRequired();
    }

    @Override
    public Principal authenticate(HttpServletRequest request)
            throws AuthenticationException
    {
        // This handles HTTP basic auth per RFC 7617. The header contains the
        // case-insensitive "Basic" scheme followed by a Base64 encoded "user:pass".
        String header = nullToEmpty(request.getHeader(AUTHORIZATION));

        int space = header.indexOf(' ');
        if ((space < 0) || !header.substring(0, space).equalsIgnoreCase("basic")) {
            throw needAuthentication(null);
        }
        String credentials = decodeCredentials(header.substring(space + 1).trim());

        List<String> parts = Splitter.on(':').limit(2).splitToList(credentials);
        if (parts.size() != 2 || parts.stream().anyMatch(String::isEmpty)) {
            throw new AuthenticationException("Malformed decoded credentials");
        }
        String user = parts.get(0);
        String password = parts.get(1);

        try {
            return authenticatorManager.getAuthenticator().createAuthenticatedPrincipal(user, password);
        }
        catch (AccessDeniedException e) {
            throw needAuthentication(e.getMessage());
        }
        catch (RuntimeException e) {
            throw new RuntimeException("Authentication error", e);
        }
    }

    private static String decodeCredentials(String credentials)
            throws AuthenticationException
    {
        // The original basic auth RFC 2617 did not specify a character set.
        // Many clients, including the Presto CLI and JDBC driver, use ISO-8859-1.
        // RFC 7617 allows the server to specify UTF-8 as the character set during
        // the challenge, but this doesn't help as most clients pre-authenticate.
        try {
            return new String(Base64.getDecoder().decode(credentials), ISO_8859_1);
        }
        catch (IllegalArgumentException e) {
            throw new AuthenticationException("Invalid base64 encoded credentials");
        }
    }

    private static AuthenticationException needAuthentication(String message)
    {
        return new AuthenticationException(message, "Basic realm=\"Presto\"");
    }
}
