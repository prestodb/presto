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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class UniqueConstraint<T>
        extends TableConstraint<T>
{
    private final Set<T> columns;

    public UniqueConstraint(Set<T> columnNames, boolean enabled, boolean rely)
    {
        this(Optional.empty(), columnNames, enabled, rely);
    }

    @JsonCreator
    public UniqueConstraint(
            @JsonProperty("name") String name,
            @JsonProperty("columns") Set<T> columnNames,
            @JsonProperty("enforced") boolean enabled,
            @JsonProperty("rely") boolean rely)
    {
        this(Optional.of(name), columnNames, enabled, rely);
    }

    protected UniqueConstraint(Optional<String> name, Set<T> columnNames, boolean enabled, boolean rely)
    {
        super(name, enabled, rely);
        requireNonNull(columnNames, "columnNames is null.");
        if (columnNames.isEmpty()) {
            throw new IllegalArgumentException("columnNames is empty.");
        }
        this.columns = Collections.unmodifiableSet(new HashSet<>(columnNames));
    }

    @Override
    public <T, R> Optional<TableConstraint<R>> rebaseConstraint(Map<T, R> assignments)
    {
        if (this.getColumns().stream().allMatch(col -> assignments.containsKey(col))) {
            return Optional.of(new UniqueConstraint<R>(getName(),
                    this.getColumns().stream().map(col -> assignments.get(col)).collect(Collectors.toSet()),
                    this.isEnforced(),
                    this.isRely()));
        }
        else {
            return Optional.empty();
        }
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("UniqueConstraint{");
        getName().ifPresent(name -> sb.append("name=").append(name));
        sb.append(",columns=").append(getColumns());
        sb.append(",enforced=").append(isEnforced());
        sb.append(",rely=").append(isRely());
        sb.append('}');
        return sb.toString();
    }

    public Set<T> getColumns()
    {
        return columns;
    }
}
