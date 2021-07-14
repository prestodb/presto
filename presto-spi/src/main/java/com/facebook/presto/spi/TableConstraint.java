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
package com.facebook.presto.spi;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "@type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = PrimaryKeyConstraint.class, name = "primarykeyconstraint"),
        @JsonSubTypes.Type(value = UniqueConstraint.class, name = "uniqueconstraint")})
public abstract class TableConstraint<T>
{
    private final Optional<String> name;
    private final boolean enforced;
    private final boolean rely;

    public TableConstraint(boolean enforced, boolean rely)
    {
        this(Optional.empty(), enforced, rely);
    }

    public TableConstraint(String name, boolean enforced, boolean rely)
    {
        this(Optional.of(requireNonNull(name, "name is null")), enforced, rely);
    }

    protected TableConstraint(Optional<String> name, boolean enforced, boolean rely)
    {
        this.enforced = requireNonNull(enforced, "enabled is null.");
        this.rely = requireNonNull(rely, "rely is null.");
        this.name = requireNonNull(name, "name is null.");
    }

    public abstract <T, R> Optional<TableConstraint<R>> rebaseConstraint(Map<T, R> assignments);

    @JsonProperty
    public Optional<String> getName()
    {
        return name;
    }

    @JsonProperty
    public boolean isEnforced()
    {
        return enforced;
    }

    @JsonProperty
    public boolean isRely()
    {
        return rely;
    }

    @JsonProperty
    public abstract Set<T> getColumns();

    @Override
    public int hashCode()
    {
        return Objects.hash(getName(), getColumns(), isEnforced(), isRely());
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        TableConstraint constraint = (TableConstraint) obj;
        return Objects.equals(this.getName(), constraint.getName()) &&
                Objects.equals(this.getColumns(), constraint.getColumns()) &&
                Objects.equals(this.isEnforced(), constraint.isEnforced()) &&
                Objects.equals(this.isRely(), constraint.isRely());
    }
}
