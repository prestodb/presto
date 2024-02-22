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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;

import static java.util.stream.Collectors.toCollection;

public class PrimaryKeyConstraint<T>
        extends UniqueConstraint<T>
{
    @JsonCreator
    public PrimaryKeyConstraint(@JsonProperty("name") Optional<String> name,
            @JsonProperty("columns") LinkedHashSet<T> columnNames,
            @JsonProperty("enabled") boolean enabled,
            @JsonProperty("rely") boolean rely,
            @JsonProperty("enforced") boolean enforced)
    {
        super(name, columnNames, enabled, rely, enforced);
    }

    @Override
    public <T, R> Optional<TableConstraint<R>> rebaseConstraint(Map<T, R> assignments)
    {
        if (this.getColumns().stream().allMatch(assignments::containsKey)) {
            return Optional.of(new PrimaryKeyConstraint<R>(this.getName(),
                    this.getColumns().stream().map(assignments::get).collect(toCollection(LinkedHashSet::new)),
                    this.isEnabled(),
                    this.isRely(),
                    this.isEnforced()));
        }
        else {
            return Optional.empty();
        }
    }
}
