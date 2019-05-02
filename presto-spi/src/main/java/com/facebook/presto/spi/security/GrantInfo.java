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

import static java.util.Objects.requireNonNull;

public class GrantInfo
{
    private final PrivilegeInfo privilegeInfo;
    private final PrestoPrincipal grantee;
    private final SchemaTableName schemaTableName;
    private final Optional<PrestoPrincipal> grantor;
    private final Optional<Boolean> withHierarchy;

    public GrantInfo(PrivilegeInfo privilegeInfo, PrestoPrincipal grantee, SchemaTableName schemaTableName, Optional<PrestoPrincipal> grantor, Optional<Boolean> withHierarchy)
    {
        this.privilegeInfo = requireNonNull(privilegeInfo, "privilegeInfo is null");
        this.grantee = requireNonNull(grantee, "grantee is null");
        this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
        this.grantor = requireNonNull(grantor, "grantor is null");
        this.withHierarchy = requireNonNull(withHierarchy, "withHierarchy is null");
    }

    public PrivilegeInfo getPrivilegeInfo()
    {
        return privilegeInfo;
    }

    public PrestoPrincipal getGrantee()
    {
        return grantee;
    }

    public SchemaTableName getSchemaTableName()
    {
        return schemaTableName;
    }

    public Optional<PrestoPrincipal> getGrantor()
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
                Objects.equals(grantee, grantInfo.getGrantee()) &&
                Objects.equals(schemaTableName, grantInfo.getSchemaTableName()) &&
                Objects.equals(grantor, grantInfo.getGrantor()) &&
                Objects.equals(withHierarchy, grantInfo.getWithHierarchy());
    }
}
