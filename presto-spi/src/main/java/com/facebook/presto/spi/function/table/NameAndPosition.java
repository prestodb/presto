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
package com.facebook.presto.spi.function.table;

import java.util.Objects;

import static com.facebook.presto.spi.function.table.Preconditions.checkArgument;
import static com.facebook.presto.spi.function.table.Preconditions.checkNotNullOrEmpty;

/**
 * This class represents a descriptor field reference.
 * `name` is the descriptor argument name, `position` is the zero-based field index.
 * <p>
 * The specified field contains a column name, as passed by the Table Function caller.
 * The column name is associated with an appropriate input table during the Analysis phase.
 * The Table Function is supposed to refer to input data using `NameAndPosition`,
 * and the engine should provide the requested column.
 */
public class NameAndPosition
{
    private final String name;
    private final int position;

    public NameAndPosition(String name, int position)
    {
        this.name = checkNotNullOrEmpty(name, "name");
        checkArgument(position >= 0, "position in descriptor must not be negative");
        this.position = position;
    }

    public String getName()
    {
        return name;
    }

    public int getPosition()
    {
        return position;
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
        NameAndPosition that = (NameAndPosition) o;
        return position == that.position && Objects.equals(name, that.name);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, position);
    }
}
