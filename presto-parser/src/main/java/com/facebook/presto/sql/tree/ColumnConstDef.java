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

import com.google.common.base.Objects;

import static com.google.common.base.Preconditions.checkNotNull;

public final class ColumnConstDef
        extends Statement
{
    private final ColumnConst columnConstraint;

    public ColumnConstDef(ColumnConst columnConstraint)
    {
        this.columnConstraint = checkNotNull(columnConstraint);
    }

    public ColumnConst getColumnConst()
    {
        return columnConstraint;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitColumnConstDef(this, context);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(columnConstraint);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        ColumnConstDef o = (ColumnConstDef) obj;
        return Objects.equal(columnConstraint, o.columnConstraint);
   }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("columnConstraint", columnConstraint)
                .toString();
    }
}
