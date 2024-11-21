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

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;

import java.security.Principal;

import static java.util.Objects.requireNonNull;

public class PrestoAuthenticator
        implements Authenticator
{
    private PrestoAuthenticatorManager authenticatorManager;

    @Inject
    public PrestoAuthenticator(PrestoAuthenticatorManager authenticatorManager)
    {
        this.authenticatorManager = requireNonNull(authenticatorManager, "authenticatorManager is null");
        authenticatorManager.setRequired();
    }

    @Override
    public Principal authenticate(HttpServletRequest request)
            throws AuthenticationException
    {
        try {
            return authenticatorManager.getAuthenticator().createAuthenticatedPrincipal(request);
        }
        catch (RuntimeException e) {
            throw new RuntimeException("Authentication error", e);
        }
    }
}
