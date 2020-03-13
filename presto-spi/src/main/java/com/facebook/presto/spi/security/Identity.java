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
package com.facebook.presto.spi.security;

import java.security.Principal;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;

public class Identity
{
    private final String user;
    private final Optional<Principal> principal;
    private final Map<String, SelectedRole> roles;
    private final Map<String, String> extraCredentials;

    /**
     * extraAuthenticators is used when short-lived access token has to be refreshed periodically.
     * Otherwise, extraCredentials should be used to store pre-fetched long-lived access token.
     *
     * extraAuthenticators will not be serialized. It has to be injected on Presto worker directly.
     */
    private final Map<String, TokenAuthenticator> extraAuthenticators;

    public Identity(String user, Optional<Principal> principal)
    {
        this(user, principal, emptyMap(), emptyMap(), emptyMap());
    }

    public Identity(
            String user,
            Optional<Principal> principal,
            Map<String, SelectedRole> roles,
            Map<String, String> extraCredentials,
            Map<String, TokenAuthenticator> extraAuthenticators)
    {
        this.user = requireNonNull(user, "user is null");
        this.principal = requireNonNull(principal, "principal is null");
        this.roles = unmodifiableMap(requireNonNull(roles, "roles is null"));
        this.extraCredentials = unmodifiableMap(new HashMap<>(requireNonNull(extraCredentials, "extraCredentials is null")));
        this.extraAuthenticators = unmodifiableMap(new HashMap<>(requireNonNull(extraAuthenticators, "extraAuthenticators is null")));
    }

    public String getUser()
    {
        return user;
    }

    public Optional<Principal> getPrincipal()
    {
        return principal;
    }

    public Map<String, SelectedRole> getRoles()
    {
        return roles;
    }

    public Map<String, String> getExtraCredentials()
    {
        return extraCredentials;
    }

    public Map<String, TokenAuthenticator> getExtraAuthenticators()
    {
        return extraAuthenticators;
    }

    public ConnectorIdentity toConnectorIdentity()
    {
        return new ConnectorIdentity(
                user,
                principal,
                Optional.empty(),
                extraCredentials,
                extraAuthenticators);
    }

    public ConnectorIdentity toConnectorIdentity(String catalog)
    {
        requireNonNull(catalog, "catalog is null");
        return new ConnectorIdentity(
                user,
                principal,
                Optional.ofNullable(roles.get(catalog)),
                extraCredentials,
                extraAuthenticators);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Identity identity = (Identity) o;
        return Objects.equals(user, identity.user);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(user);
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("Identity{");
        sb.append("user='").append(user).append('\'');
        principal.ifPresent(principal -> sb.append(", principal=").append(principal));
        sb.append(", roles=").append(roles);
        sb.append(", extraCredentials=").append(extraCredentials.keySet());
        sb.append(", extraAuthenticators=").append(extraAuthenticators.keySet());
        sb.append('}');
        return sb.toString();
    }
}
