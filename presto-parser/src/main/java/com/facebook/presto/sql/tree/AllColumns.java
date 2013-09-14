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

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

public class AllColumns
        extends SelectItem
{
    private final Optional<QualifiedName> prefix;

    public AllColumns()
    {
        prefix = Optional.absent();
    }

    public AllColumns(QualifiedName prefix)
    {
        Preconditions.checkNotNull(prefix, "prefix is null");
        this.prefix = Optional.of(prefix);
    }

    public Optional<QualifiedName> getPrefix()
    {
        return prefix;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitAllColumns(this, context);
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

        AllColumns that = (AllColumns) o;

        if (prefix != null ? !prefix.equals(that.prefix) : that.prefix != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        return prefix != null ? prefix.hashCode() : 0;
    }

    @Override
    public String toString()
    {
        if (prefix.isPresent()) {
            return prefix.get() + ".*";
        }

        return "*";
    }
}
