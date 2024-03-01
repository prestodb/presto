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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class RoleGrant
{
    private final PrestoPrincipal grantee;
    private final String roleName;
    private final boolean grantable;

    @JsonCreator
    public RoleGrant(@JsonProperty("grantee") PrestoPrincipal grantee, @JsonProperty("roleName") String roleName, @JsonProperty("grantable") boolean grantable)
    {
        this.grantee = requireNonNull(grantee, "grantee is null");
        this.roleName = requireNonNull(roleName, "roleName is null").toLowerCase(ENGLISH);
        this.grantable = grantable;
    }

    @JsonProperty
    public String getRoleName()
    {
        return roleName;
    }

    @JsonProperty
    public PrestoPrincipal getGrantee()
    {
        return grantee;
    }

    @JsonProperty
    public boolean isGrantable()
    {
        return grantable;
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
        RoleGrant roleGrant = (RoleGrant) o;
        return grantable == roleGrant.grantable &&
                Objects.equals(grantee, roleGrant.grantee) &&
                Objects.equals(roleName, roleGrant.roleName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(grantee, roleName, grantable);
    }

    @Override
    public String toString()
    {
        return "RoleGrant{" +
                "grantee=" + grantee +
                ", roleName='" + roleName + '\'' +
                ", grantable=" + grantable +
                '}';
    }
}
