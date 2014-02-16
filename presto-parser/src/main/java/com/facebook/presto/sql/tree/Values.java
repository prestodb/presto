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

import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public final class Values
        extends QueryBody
{
    private final List<Row> rows;

    public Values(List<Row> rows)
    {
        checkNotNull(rows, "rows is null");
        this.rows = ImmutableList.copyOf(rows);
    }

    public List<Row> getRows()
    {
        return rows;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitValues(this, context);
    }

    @Override
    public String toString()
    {
        return "(" + Joiner.on(", ").join(rows) + ")";
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(rows);
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
        Values other = (Values) obj;
        return Objects.equal(this.rows, other.rows);
    }
}
