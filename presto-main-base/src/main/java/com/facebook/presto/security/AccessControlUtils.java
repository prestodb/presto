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
package com.facebook.presto.security;

import com.facebook.presto.server.SessionContext;
import com.facebook.presto.server.security.SecurityConfig;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.security.AccessControl;
import com.facebook.presto.spi.security.AccessControlContext;
import com.facebook.presto.spi.security.AccessDeniedException;
import com.facebook.presto.spi.security.AuthorizedIdentity;
import com.facebook.presto.spi.security.Identity;

import java.util.Optional;

public class AccessControlUtils
{
    private AccessControlUtils() {}

    /**
     * Uses checkCanSetUser API to check delegation permissions
     *
     * @throws AccessDeniedException if not allowed
     */
    public static void checkPermissions(AccessControl accessControl, SecurityConfig securityConfig, QueryId queryId, SessionContext sessionContext)
    {
        Identity identity = sessionContext.getIdentity();
        if (!securityConfig.isAuthorizedIdentitySelectionEnabled()) {
            accessControl.checkCanSetUser(
                    identity,
                    new AccessControlContext(
                            queryId,
                            Optional.ofNullable(sessionContext.getClientInfo()),
                            sessionContext.getClientTags(),
                            Optional.ofNullable(sessionContext.getSource()),
                            WarningCollector.NOOP,
                            sessionContext.getRuntimeStats(),
                            Optional.empty(),
                            Optional.ofNullable(sessionContext.getCatalog()),
                            Optional.ofNullable(sessionContext.getSchema()),
                            getSqlText(sessionContext, securityConfig)),
                    identity.getPrincipal(),
                    identity.getUser());
        }
    }

    /**
     * When selectAuthorizedIdentity API is enabled,
     * 1. Check the delegation permission, which is inside the API call
     * 2. Select and return the authorized identity
     *
     * @throws AccessDeniedException if not allowed
     */
    public static Optional<AuthorizedIdentity> getAuthorizedIdentity(AccessControl accessControl, SecurityConfig securityConfig, QueryId queryId, SessionContext sessionContext)
    {
        if (securityConfig.isAuthorizedIdentitySelectionEnabled()) {
            Identity identity = sessionContext.getIdentity();
            AuthorizedIdentity authorizedIdentity = accessControl.selectAuthorizedIdentity(
                    identity,
                    new AccessControlContext(
                            queryId,
                            Optional.ofNullable(sessionContext.getClientInfo()),
                            sessionContext.getClientTags(),
                            Optional.ofNullable(sessionContext.getSource()),
                            WarningCollector.NOOP,
                            sessionContext.getRuntimeStats(),
                            Optional.empty(),
                            Optional.ofNullable(sessionContext.getCatalog()),
                            Optional.ofNullable(sessionContext.getSchema()),
                            getSqlText(sessionContext, securityConfig)),
                    identity.getUser(),
                    sessionContext.getCertificates());
            return Optional.of(authorizedIdentity);
        }
        return Optional.empty();
    }

    private static Optional<String> getSqlText(SessionContext sessionContext, SecurityConfig securityConfig)
    {
        if (securityConfig.isEnableSqlQueryTextContextField()) {
            return Optional.of(sessionContext.getSqlText());
        }
        return Optional.empty();
    }
}
