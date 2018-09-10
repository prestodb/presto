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
package com.facebook.presto.spi.predicate;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Set;

import static java.util.Objects.requireNonNull;

public class FieldSet<T>
{
    private final T column;
    private final Set<String> fields;

    @JsonCreator
    public FieldSet(
            @JsonProperty("column") T column,
            @JsonProperty("fields") Set<String> fields)
    {
        this.column = requireNonNull(column, "column is null");
        this.fields = requireNonNull(fields, "fields is null");
    }

    @JsonProperty
    public T getColumn()
    {
        return column;
    }

    @JsonProperty
    public Set<String> getFields()
    {
        return fields;
    }

    @Override
    public String toString()
    {
        return "[" + column.toString() + " : " + fields.toString() + "]";
    }
}
