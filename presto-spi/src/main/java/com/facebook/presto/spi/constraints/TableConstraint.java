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
package com.facebook.presto.spi.constraints;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static java.util.Collections.unmodifiableSet;
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
    private final Set<T> columns;

    protected TableConstraint(Optional<String> name, Set<T> columnNames, boolean enforced, boolean rely)
    {
        this.enforced = requireNonNull(enforced, "enabled is null");
        this.rely = requireNonNull(rely, "rely is null");
        this.name = requireNonNull(name, "name is null");
        requireNonNull(columnNames, "columnNames is null");
        if (columnNames.isEmpty()) {
            throw new IllegalArgumentException("columnNames is empty.");
        }
        this.columns = unmodifiableSet(new HashSet<>(columnNames));
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
    public Set<T> getColumns()
    {
        return columns;
    }

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

    @Override
    public String toString()
    {
        StringBuilder stringBuilder = new StringBuilder(this.getClass().getSimpleName());
        stringBuilder.append(" {");
        stringBuilder.append("name='").append(name.orElse("null")).append('\'');
        stringBuilder.append(", columns='").append(columns).append('\'');
        stringBuilder.append(", enforced='").append(enforced).append('\'');
        stringBuilder.append(", rely='").append(rely).append('\'');
        stringBuilder.append('}');
        return stringBuilder.toString();
    }
}
