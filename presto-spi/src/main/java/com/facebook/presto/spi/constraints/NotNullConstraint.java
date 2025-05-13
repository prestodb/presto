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

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;

import static java.util.stream.Collectors.toCollection;

public class NotNullConstraint<T>
        extends TableConstraint<T>
{
    public NotNullConstraint(T column)
    {
        this(Optional.empty(), new LinkedHashSet<T>(Collections.singleton(column)));
    }

    public NotNullConstraint(Optional<String> name, T column)
    {
        this(name, new LinkedHashSet<T>(Collections.singleton(column)));
    }
    @JsonCreator
    public NotNullConstraint(@JsonProperty("name") Optional<String> name,
            @JsonProperty("columns") LinkedHashSet<T> columnNames)
    {
        super(name, columnNames, true, true, true);
    }

    @Override
    public <T, R> Optional<TableConstraint<R>> rebaseConstraint(Map<T, R> assignments)
    {
        if (this.getColumns().stream().allMatch(assignments::containsKey)) {
            return Optional.of(new NotNullConstraint<R>(this.getName(), (LinkedHashSet<R>) this.getColumns().stream().map(assignments::get).collect(toCollection(LinkedHashSet::new))));
        }
        else {
            return Optional.empty();
        }
    }
}
