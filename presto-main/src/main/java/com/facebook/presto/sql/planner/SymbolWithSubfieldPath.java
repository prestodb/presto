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
package com.facebook.presto.sql.planner;

import com.facebook.presto.spi.SubfieldPath;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * Represents a series of subfield or array/map subscript expressions.
 * This may may occur as a key in a TupleDomain<Symbol> for representing
 * predicates on nested elements in a row
 */
public class SymbolWithSubfieldPath
        extends Symbol
{
    private final SubfieldPath path;

    @JsonCreator
    public SymbolWithSubfieldPath(@JsonProperty("path") SubfieldPath path)
    {
        super("");
        requireNonNull(path, "path is null");
        this.path = path;
    }

    @JsonProperty("path")
    public SubfieldPath getSubfieldPath()
    {
        return path;
    }

    @Override
    public String getName()
    {
        return toString();
    }

    @Override
    public String toString()
    {
        return path.toString();
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

        SymbolWithSubfieldPath other = (SymbolWithSubfieldPath) o;
        return Objects.equals(path, other.path);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(path);
    }
}
