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
package com.facebook.presto.raptor.metadata;

import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.security.GrantInfo;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.security.PrivilegeInfo;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

public class RaptorGrantInfo
{
    private final Set<RaptorPrivilegeInfo> privilegeInfo;
    private final Identity grantee;
    private final SchemaTableName schemaTableName;
    private final Optional<Identity> grantor;
    private final Optional<Boolean> withHierarchy;

    public RaptorGrantInfo(Set<RaptorPrivilegeInfo> privilegeInfo, Identity grantee, SchemaTableName schemaTableName, Optional<Identity> grantor, Optional<Boolean> withHierarchy)
    {
        this.privilegeInfo = requireNonNull(privilegeInfo, "privilegeInfo is null");
        this.grantee = requireNonNull(grantee, "grantee is null");
        this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
        this.grantor = requireNonNull(grantor, "grantor is null");
        this.withHierarchy = requireNonNull(withHierarchy, "withHierarchy is null");
    }

    public Set<RaptorPrivilegeInfo> getPrivilegeInfo()
    {
        return privilegeInfo;
    }

    public Identity getGrantee()
    {
        return grantee;
    }

    public SchemaTableName getSchemaTableName()
    {
        return schemaTableName;
    }

    public Optional<Identity> getGrantor()
    {
        return grantor;
    }

    public Optional<Boolean> getWithHierarchy()
    {
        return withHierarchy;
    }

    public GrantInfo toGrantInfo()
    {
        Set<PrivilegeInfo> privileges = privilegeInfo.stream()
                .map(RaptorPrivilegeInfo::toPrivilegeInfo)
                .flatMap(Set::stream)
                .collect(toImmutableSet());

        return new GrantInfo(privileges, grantee, schemaTableName, grantor, withHierarchy);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(privilegeInfo, grantee, schemaTableName, grantor, withHierarchy);
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
        com.facebook.presto.spi.security.GrantInfo grantInfo = (com.facebook.presto.spi.security.GrantInfo) o;
        return Objects.equals(privilegeInfo, grantInfo.getPrivilegeInfo()) &&
                Objects.equals(grantee, grantInfo.getIdentity()) &&
                Objects.equals(schemaTableName, grantInfo.getSchemaTableName()) &&
                Objects.equals(grantor, grantInfo.getGrantor()) &&
                Objects.equals(withHierarchy, grantInfo.getWithHierarchy());
    }

    public static class Mapper
            implements ResultSetMapper<RaptorGrantInfo>
    {
        @Override
        public RaptorGrantInfo map(int index, ResultSet r, StatementContext ctx)
                throws SQLException
        {
            int privilegeMask = r.getInt("privilege_mask");
            boolean grantOption = r.getBoolean("is_grantable");
            Set<RaptorPrivilegeInfo> privileges = RaptorPrivilegeInfo.fromMaskValue(privilegeMask, grantOption);

            SchemaTableName name = new SchemaTableName(
                    r.getString("schema_name"),
                    r.getString("table_name"));

            Identity grantee = r.getString("grantee") == null ? null : new Identity(r.getString("grantee"), Optional.empty());
            Identity grantor = r.getString("grantor") == null ? null : new Identity(r.getString("grantor"), Optional.empty());

            return new RaptorGrantInfo(privileges,
                    grantee,
                    name,
                    Optional.ofNullable(grantor),
                    Optional.ofNullable(r.getBoolean("with_hierarchy")));
        }
    }
}
