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

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

public class DropView
        extends Statement
{
    private final QualifiedName name;
    private final boolean existsPredicate;

    public DropView(QualifiedName name, boolean existsPredicate)
    {
        this.name = name;
        this.existsPredicate = existsPredicate;
    }

    public QualifiedName getName()
    {
        return name;
    }

    public boolean isExistsPredicate()
    {
        return existsPredicate;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitDropView(this, context);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, existsPredicate);
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
        DropView o = (DropView) obj;
        return Objects.equals(name, o.name)
                && (existsPredicate == o.existsPredicate);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("existsPredicate", existsPredicate)
                .toString();
    }
}
