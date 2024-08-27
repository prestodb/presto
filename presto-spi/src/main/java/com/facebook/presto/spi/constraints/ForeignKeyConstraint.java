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

import com.facebook.presto.common.QualifiedObjectName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toCollection;

public class ForeignKeyConstraint<T>
        extends TableConstraint<T>
{
    private final List<T> referencedColumnNames;
    private final QualifiedObjectName referencedTableName;
    private final List<T> sourceColumnNames;

    @JsonCreator
    public ForeignKeyConstraint(@JsonProperty("name") Optional<String> name,
            @JsonProperty("sourceColumns") List<T> sourceColumnNames,
            @JsonProperty("referencedTableName") QualifiedObjectName referencedTableName,
            @JsonProperty("referencedColumns") List<T> referencedColumnNames,
            @JsonProperty("enabled") boolean enabled,
            @JsonProperty("rely") boolean rely,
            @JsonProperty("enforced") boolean enforced)
    {
        // FK constraints are not necessarily a set
        super(name, new LinkedHashSet<>(sourceColumnNames), enabled, rely, enforced);
        this.sourceColumnNames = sourceColumnNames;
        this.referencedTableName = referencedTableName;
        this.referencedColumnNames = referencedColumnNames;
    }

    @JsonProperty
    public QualifiedObjectName getReferencedTableName()
    {
        return referencedTableName;
    }

    public List<T> getSourceColumnNames()
    {
        return sourceColumnNames;
    }

    @JsonProperty
    public List<T> getReferencedColumnNames()
    {
        return referencedColumnNames;
    }

    @Override
    public <T, R> Optional<TableConstraint<R>> rebaseConstraint(Map<T, R> assignments)
    {
        if (this.getColumns().stream().allMatch(assignments::containsKey)) {
            return Optional.of(new ForeignKeyConstraint<R>(getName(),
                    this.getSourceColumnNames().stream().map(assignments::get).collect(Collectors.toList()),
                    this.getReferencedTableName(),
                    this.getReferencedColumnNames().stream().map(assignments::get).collect(Collectors.toList()),
                    this.isEnabled(),
                    this.isRely(),
                    this.isEnforced()));
        }
        else {
            return Optional.empty();
        }
    }
}
