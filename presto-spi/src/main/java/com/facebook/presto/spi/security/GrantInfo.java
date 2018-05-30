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

import com.facebook.presto.spi.SchemaTableName;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public class GrantInfo
{
    private final Set<PrivilegeInfo> privilegeInfo;
    private final Identity grantee;
    private final SchemaTableName schemaTableName;
    private final Optional<Identity> grantor;
    private final Optional<Boolean> withHierarchy;

    public GrantInfo(Set<PrivilegeInfo> privilegeInfo, Identity grantee, SchemaTableName schemaTableName, Optional<Identity> grantor, Optional<Boolean> withHierarchy)
    {
        this.privilegeInfo = privilegeInfo;
        this.grantee = grantee;
        this.schemaTableName = schemaTableName;
        this.grantor = grantor;
        this.withHierarchy = withHierarchy;
    }

    public Set<PrivilegeInfo> getPrivilegeInfo()
    {
        return privilegeInfo;
    }

    public Identity getIdentity()
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
        GrantInfo grantInfo = (GrantInfo) o;
        return Objects.equals(privilegeInfo, grantInfo.getPrivilegeInfo()) &&
                Objects.equals(grantee, grantInfo.getIdentity()) &&
                Objects.equals(schemaTableName, grantInfo.getSchemaTableName()) &&
                Objects.equals(grantor, grantInfo.getGrantor()) &&
                Objects.equals(withHierarchy, grantInfo.getWithHierarchy());
    }
}
