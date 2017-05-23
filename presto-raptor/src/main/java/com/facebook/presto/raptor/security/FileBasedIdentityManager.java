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
package com.facebook.presto.raptor.security;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.security.Identity;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.raptor.RaptorErrorCode.RAPTOR_ERROR;

public class FileBasedIdentityManager
        implements IdentityManager
{
    private final Map<String, Role> roles;
    private final Map<String, SchemaOwners> schemaOwners;

    @Inject
    public FileBasedIdentityManager(FileBasedIdentityConfig config)
    {
        ObjectMapper mapper = new ObjectMapper();
        try {
            this.roles = mapper.readValue(Paths.get(config.getGroupsFileName()).toFile(), new TypeReference<Map<String, Role>>() {});
            this.schemaOwners = mapper.readValue(Paths.get(config.getSchemaOwnersFileName()).toFile(), new TypeReference<Map<String, SchemaOwners>>() {});
        }
        catch (IOException e) {
            throw new PrestoException(RAPTOR_ERROR, "failed to load identity config", e);
        }
    }

    @Override
    public boolean belongsToRole(ConnectorTransactionHandle transaction, Identity identity, String role)
    {
        if (role.equalsIgnoreCase(PUBLIC_ROLE)) {
            return true;
        }

        Role roleToCheck = roles.get(role);

        return roleToCheck != null && roleToCheck.hasUser(identity.getUser());
    }

    @Override
    public boolean isSchemaOwner(ConnectorTransactionHandle transaction, Identity identity, String schemaName)
    {
        SchemaOwners owners = schemaOwners.get(schemaName);
        if (owners == null) {
            return false;
        }
        if (owners.getUsers().contains(identity.getUser())) {
            return true;
        }

        return owners.getRoles().stream()
                .map(roles::get)
                .anyMatch(g -> g != null && g.hasUser(identity.getUser()));
    }

    private static class Role
    {
        private final Set<String> users;

        @JsonCreator
        Role(@JsonProperty("users") Set<String> users)
        {
            this.users = users;
        }

        public Set<String> getUsers()
        {
            return users;
        }

        public boolean hasUser(String user)
        {
            return users.contains(user);
        }
    }

    private static class SchemaOwners
    {
        private final Set<String> roles;
        private final Set<String> users;

        @JsonCreator
        SchemaOwners(@JsonProperty("roles") Set<String> roles, @JsonProperty("users") Set<String> users)
        {
            this.roles = roles;
            this.users = users;
        }

        public Set<String> getRoles()
        {
            return roles;
        }

        public Set<String> getUsers()
        {
            return users;
        }
    }
}
