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
package io.prestosql.tests.tpch;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

// TODO merge this into MaterializedRow
class MaterializedTuple
{
    private final List<Object> values;

    public MaterializedTuple(List<Object> values)
    {
        this.values = requireNonNull(values, "values is null");
    }

    public static MaterializedTuple from(Object... values)
    {
        return new MaterializedTuple(Arrays.asList(values));
    }

    public List<Object> getValues()
    {
        return values;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(values);
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
        final MaterializedTuple other = (MaterializedTuple) obj;
        return Objects.equals(this.values, other.values);
    }
}
