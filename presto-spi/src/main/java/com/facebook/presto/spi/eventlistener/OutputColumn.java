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
package com.facebook.presto.spi.eventlistener;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class OutputColumn
{
    private final Column column;
    private final Set<SourceColumn> sourceColumns;

    @JsonCreator
    public OutputColumn(@JsonProperty("column") Column column, @JsonProperty("sourceColumns") Set<SourceColumn> sourceColumns)
    {
        this.column = requireNonNull(column, "column is null");
        //this.sourceColumns = ImmutableSet.copyOf(requireNonNull(sourceColumns, "sourceColumns is null"));
        this.sourceColumns = requireNonNull(sourceColumns, "sourceColumns is null");
    }

    @JsonProperty
    public Column getColumn()
    {
        return column;
    }

    @JsonProperty
    public Set<SourceColumn> getSourceColumns()
    {
        return sourceColumns;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(column, sourceColumns);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == this) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        OutputColumn entry = (OutputColumn) obj;
        return Objects.equals(column, entry.column) &&
                Objects.equals(sourceColumns, entry.sourceColumns);
    }

    @Override
    public String toString()
    {
        return "OutputColumn{" +
                "column=" + column +
                ", sourceColumns=" + sourceColumns +
                '}';
    }
}
