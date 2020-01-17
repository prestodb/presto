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

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class GrantorSpecification
{
    public enum Type
    {
        PRINCIPAL, CURRENT_USER, CURRENT_ROLE
    }

    private final Type type;
    private final Optional<PrincipalSpecification> principal;

    public GrantorSpecification(Type type, Optional<PrincipalSpecification> principal)
    {
        this.type = requireNonNull(type, "type is null");
        this.principal = requireNonNull(principal, "principal is null");
    }

    public Type getType()
    {
        return type;
    }

    public Optional<PrincipalSpecification> getPrincipal()
    {
        return principal;
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
        GrantorSpecification that = (GrantorSpecification) o;
        return type == that.type &&
                Objects.equals(principal, that.principal);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(type, principal);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("type", type)
                .add("principal", principal)
                .toString();
    }
}
