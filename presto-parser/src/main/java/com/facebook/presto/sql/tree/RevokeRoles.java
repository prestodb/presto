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
package com.facebook.presto.sql.tree;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;

public class RevokeRoles
        extends Statement
{
    private final Set<String> roles;
    private final Set<PrincipalSpecification> grantees;
    private final boolean adminOptionFor;
    private final Optional<GrantorSpecification> grantor;
    private final Optional<String> catalog;

    public RevokeRoles(
            NodeLocation location,
            Set<String> roles,
            Set<PrincipalSpecification> grantees,
            boolean adminOptionFor,
            Optional<GrantorSpecification> grantor,
            Optional<String> catalog)
    {
        this(Optional.of(location), roles, grantees, adminOptionFor, grantor, catalog);
    }

    public RevokeRoles(
            Set<String> roles,
            Set<PrincipalSpecification> grantees,
            boolean adminOptionFor,
            Optional<GrantorSpecification> grantor,
            Optional<String> catalog)
    {
        this(Optional.empty(), roles, grantees, adminOptionFor, grantor, catalog);
    }

    private RevokeRoles(
            Optional<NodeLocation> location,
            Set<String> roles,
            Set<PrincipalSpecification> grantees,
            boolean adminOptionFor,
            Optional<GrantorSpecification> grantor,
            Optional<String> catalog)
    {
        super(location);
        this.roles = ImmutableSet.copyOf(requireNonNull(roles, "roles is null").stream().map(r -> r.toLowerCase(ENGLISH)).collect(toSet()));
        this.grantees = ImmutableSet.copyOf(requireNonNull(grantees, "grantees is null"));
        this.adminOptionFor = adminOptionFor;
        this.grantor = requireNonNull(grantor, "grantor is null");
        this.catalog = requireNonNull(catalog, "catalog is null").map(c -> c.toLowerCase(ENGLISH));
    }

    public Set<String> getRoles()
    {
        return roles;
    }

    public Set<PrincipalSpecification> getGrantees()
    {
        return grantees;
    }

    public boolean isAdminOptionFor()
    {
        return adminOptionFor;
    }

    public Optional<GrantorSpecification> getGrantor()
    {
        return grantor;
    }

    public Optional<String> getCatalog()
    {
        return catalog;
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return ImmutableList.of();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitRevokeRoles(this, context);
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
        RevokeRoles that = (RevokeRoles) o;
        return adminOptionFor == that.adminOptionFor &&
                Objects.equals(roles, that.roles) &&
                Objects.equals(grantees, that.grantees) &&
                Objects.equals(grantor, that.grantor) &&
                Objects.equals(catalog, that.catalog);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(roles, grantees, adminOptionFor, grantor, catalog);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("roles", roles)
                .add("grantees", grantees)
                .add("adminOptionFor", adminOptionFor)
                .add("grantor", grantor)
                .add("catalog", catalog)
                .toString();
    }
}
