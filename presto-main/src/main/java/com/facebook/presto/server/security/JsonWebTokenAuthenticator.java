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
import com.facebook.airlift.http.server.Authenticator;
import io.jsonwebtoken.JwtException;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;

import java.security.Principal;

import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.net.HttpHeaders.AUTHORIZATION;
import static java.util.Objects.requireNonNull;

public class JsonWebTokenAuthenticator
        implements Authenticator
{
    private JWTAuthenticatorManager authenticatorManager;

    @Inject
    public JsonWebTokenAuthenticator(JWTAuthenticatorManager authenticatorManager)
    {
        this.authenticatorManager = requireNonNull(authenticatorManager, "authenticatorManager is null");
        authenticatorManager.setRequired();
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
            return authenticatorManager.getAuthenticator().createAuthenticatedPrincipal(token, request);
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
}
