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
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;

public class ConnectorIdentity
{
    private final String user;
    private final Optional<Principal> principal;
    private final Optional<SelectedRole> role;
    private final Map<String, String> extraCredentials;
    private final Map<String, TokenAuthenticator> extraAuthenticators;
    private final Optional<String> selectedUser;
    private final Optional<String> reasonForSelect;
    private final List<X509Certificate> certificates;

    public ConnectorIdentity(String user, Optional<Principal> principal, Optional<SelectedRole> role)
    {
        this(user, principal, role, emptyMap(), emptyMap(), Optional.empty(), Optional.empty(), emptyList());
    }

    public ConnectorIdentity(
            String user,
            Optional<Principal> principal,
            Optional<SelectedRole> role,
            Map<String, String> extraCredentials,
            Map<String, TokenAuthenticator> extraAuthenticators,
            Optional<String> selectedUser,
            Optional<String> reasonForSelect)
    {
        this(user, principal, role, extraCredentials, extraAuthenticators, selectedUser, reasonForSelect, emptyList());
    }

    public ConnectorIdentity(
            String user,
            Optional<Principal> principal,
            Optional<SelectedRole> role,
            Map<String, String> extraCredentials,
            Map<String, TokenAuthenticator> extraAuthenticators,
            Optional<String> selectedUser,
            Optional<String> reasonForSelect,
            List<X509Certificate> certificates)
    {
        this.user = requireNonNull(user, "user is null");
        this.principal = requireNonNull(principal, "principal is null");
        this.role = requireNonNull(role, "role is null");
        this.extraCredentials = unmodifiableMap(new HashMap<>(requireNonNull(extraCredentials, "extraCredentials is null")));
        this.extraAuthenticators = unmodifiableMap(new HashMap<>(requireNonNull(extraAuthenticators, "extraAuthenticators is null")));
        this.selectedUser = requireNonNull(selectedUser, "selectedUser is null");
        this.reasonForSelect = requireNonNull(reasonForSelect, "reasonForSelect is null");
        this.certificates = unmodifiableList(new ArrayList<>(requireNonNull(certificates, "certificates is null")));
    }

    public String getUser()
    {
        return user;
    }

    public Optional<Principal> getPrincipal()
    {
        return principal;
    }

    public Optional<SelectedRole> getRole()
    {
        return role;
    }

    public Map<String, String> getExtraCredentials()
    {
        return extraCredentials;
    }

    public Map<String, TokenAuthenticator> getExtraAuthenticators()
    {
        return extraAuthenticators;
    }

    public Optional<String> getSelectedUser()
    {
        return selectedUser;
    }

    public Optional<String> getReasonForSelect()
    {
        return reasonForSelect;
    }

    public List<X509Certificate> getCertificates()
    {
        return certificates;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("ConnectorIdentity{");
        sb.append("user='").append(user).append('\'');
        principal.ifPresent(principal -> sb.append(", principal=").append(principal));
        role.ifPresent(role -> sb.append(", role=").append(role));
        sb.append(", extraCredentials=").append(extraCredentials.keySet());
        sb.append(", extraAuthenticators=").append(extraAuthenticators.keySet());
        selectedUser.ifPresent(user -> sb.append(", selectedUser=").append(user));
        reasonForSelect.ifPresent(
                reason -> sb.append(", reasonForSelect=").append(reason));
        sb.append('}');
        return sb.toString();
    }
}
