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

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "@type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = PrimaryKeyConstraint.class, name = "primarykeyconstraint"),
        @JsonSubTypes.Type(value = UniqueConstraint.class, name = "uniqueconstraint"),
        @JsonSubTypes.Type(value = NotNullConstraint.class, name = "notnullconstraint")})
public abstract class TableConstraint<T>
{
    private final Optional<String> name;
    private final boolean enabled;
    private final boolean rely;
    private final boolean enforced;
    private final LinkedHashSet<T> columns;

    protected TableConstraint(Optional<String> name, LinkedHashSet<T> columnNames, boolean enabled, boolean rely, boolean enforced)
    {
        this.enabled = enabled;
        this.rely = rely;
        this.enforced = enforced;
        this.name = requireNonNull(name, "name is null");
        requireNonNull(columnNames, "columnNames is null");
        if (columnNames.isEmpty()) {
            throw new IllegalArgumentException("columnNames is empty.");
        }
        this.columns = new LinkedHashSet<>(columnNames);
    }

    public abstract <T, R> Optional<TableConstraint<R>> rebaseConstraint(Map<T, R> assignments);

    @JsonProperty
    public Optional<String> getName()
    {
        return name;
    }

    @JsonProperty
    public boolean isEnabled()
    {
        return enabled;
    }

    @JsonProperty
    public boolean isRely()
    {
        return rely;
    }

    @JsonProperty
    public boolean isEnforced()
    {
        return enforced;
    }

    @JsonProperty
    public LinkedHashSet<T> getColumns()
    {
        return columns;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getName(), getColumns(), isEnabled(), isRely(), isEnforced());
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
                Objects.equals(this.isEnabled(), constraint.isEnabled()) &&
                Objects.equals(this.isRely(), constraint.isRely()) &&
                Objects.equals(this.isEnforced(), constraint.isEnforced());
    }

    @Override
    public String toString()
    {
        StringBuilder stringBuilder = new StringBuilder(this.getClass().getSimpleName());
        stringBuilder.append(" {");
        stringBuilder.append("name='").append(name.orElse("null")).append('\'');
        stringBuilder.append(", columns='").append(columns).append('\'');
        stringBuilder.append(", enforced='").append(enabled).append('\'');
        stringBuilder.append(", rely='").append(rely).append('\'');
        stringBuilder.append(", validate='").append(enforced).append('\'');
        stringBuilder.append('}');
        return stringBuilder.toString();
    }
}
