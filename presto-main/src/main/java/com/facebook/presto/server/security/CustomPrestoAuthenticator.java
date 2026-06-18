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
import com.facebook.airlift.log.Logger;
import com.facebook.presto.spi.security.AccessDeniedException;
import com.facebook.presto.spi.security.AuthenticatorNotApplicableException;
import jakarta.inject.Inject;
import jakarta.servlet.http.HttpServletRequest;

import java.security.Principal;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Collections.list;
import static java.util.Objects.requireNonNull;

public class CustomPrestoAuthenticator
        implements Authenticator
{
    private static final Logger log = Logger.get(CustomPrestoAuthenticator.class);

    private PrestoAuthenticatorManager authenticatorManager;

    @Inject
    public CustomPrestoAuthenticator(PrestoAuthenticatorManager authenticatorManager)
    {
        this.authenticatorManager = requireNonNull(authenticatorManager, "authenticatorManager is null");
    }

    @Override
    public Principal authenticate(HttpServletRequest request)
            throws AuthenticationException
    {
        try {
            // Extracting headers into a Map
            Map<String, List<String>> headers = getHeadersMap(request);

            // Passing the header map to the authenticator (instead of HttpServletRequest)
            return authenticatorManager.getAuthenticator().createAuthenticatedPrincipal(headers);
        }
        catch (AuthenticatorNotApplicableException e) {
            // Presto will gracefully handle this exception and will not propagate it back to the client
            log.debug(e, e.getMessage());
            throw needAuthentication();
        }
        catch (AccessDeniedException e) {
            throw new AuthenticationException(e.getMessage());
        }
    }

    private static AuthenticationException needAuthentication()
    {
        return new AuthenticationException(null);
    }

    // Utility method to extract headers from HttpServletRequest
    private Map<String, List<String>> getHeadersMap(HttpServletRequest request)
    {
        return list(request.getHeaderNames())
                .stream()
                .collect(toImmutableMap(
                        headerName -> headerName,
                        headerName -> list(request.getHeaders(headerName))));
    }
}
