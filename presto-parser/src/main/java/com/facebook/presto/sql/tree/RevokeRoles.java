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
import static java.util.Objects.requireNonNull;

public class RevokeRoles
        extends Statement
{
    private final Set<Identifier> roles;
    private final Set<PrincipalSpecification> grantees;
    private final boolean adminOptionFor;
    private final Optional<GrantorSpecification> grantor;

    public RevokeRoles(
            NodeLocation location,
            Set<Identifier> roles,
            Set<PrincipalSpecification> grantees,
            boolean adminOptionFor,
            Optional<GrantorSpecification> grantor)
    {
        this(Optional.of(location), roles, grantees, adminOptionFor, grantor);
    }

    public RevokeRoles(
            Set<Identifier> roles,
            Set<PrincipalSpecification> grantees,
            boolean adminOptionFor,
            Optional<GrantorSpecification> grantor)
    {
        this(Optional.empty(), roles, grantees, adminOptionFor, grantor);
    }

    private RevokeRoles(
            Optional<NodeLocation> location,
            Set<Identifier> roles,
            Set<PrincipalSpecification> grantees,
            boolean adminOptionFor,
            Optional<GrantorSpecification> grantor)
    {
        super(location);
        this.roles = ImmutableSet.copyOf(requireNonNull(roles, "roles is null"));
        this.grantees = ImmutableSet.copyOf(requireNonNull(grantees, "grantees is null"));
        this.adminOptionFor = adminOptionFor;
        this.grantor = requireNonNull(grantor, "grantor is null");
    }

    public Set<Identifier> getRoles()
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
                Objects.equals(grantor, that.grantor);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(roles, grantees, adminOptionFor, grantor);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("roles", roles)
                .add("grantees", grantees)
                .add("adminOptionFor", adminOptionFor)
                .add("grantor", grantor)
                .toString();
    }
}
