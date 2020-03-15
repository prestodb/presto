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
package com.facebook.presto.sql.tree;

import com.facebook.presto.sql.tree.RoutineCharacteristics.NullCallClause;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class AlterRoutineCharacteristics
{
    private final Optional<NullCallClause> nullCallClause;

    public AlterRoutineCharacteristics(Optional<NullCallClause> nullCallClause)
    {
        this.nullCallClause = requireNonNull(nullCallClause, "nullCallClause is null");
    }

    public Optional<NullCallClause> getNullCallClause()
    {
        return nullCallClause;
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
        AlterRoutineCharacteristics that = (AlterRoutineCharacteristics) o;
        return Objects.equals(nullCallClause, that.nullCallClause);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(nullCallClause);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("nullCallClause", nullCallClause)
                .toString();
    }
}
