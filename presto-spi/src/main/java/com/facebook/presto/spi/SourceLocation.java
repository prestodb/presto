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

import java.util.Objects;

public class SourceLocation
{
    private final int line;
    private final int column;

    @JsonCreator
    public SourceLocation(
            @JsonProperty("line") int line,
            @JsonProperty("column") int column)
    {
        this.line = line;
        this.column = column;
    }

    @Override
    public String toString()
    {
        return line + ":" + column;
    }

    @JsonProperty
    public int getLine()
    {
        return line;
    }

    @JsonProperty
    public int getColumn()
    {
        return column;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }

        if (obj == null) {
            return false;
        }

        if (getClass() != obj.getClass()) {
            return false;
        }

        SourceLocation that = (SourceLocation) obj;

        return line == that.line && column == that.column;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(line, column);
    }
}
