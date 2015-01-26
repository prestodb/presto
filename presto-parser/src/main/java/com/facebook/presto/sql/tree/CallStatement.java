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

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

public class CallStatement
        extends ControlStatement
{
    private final QualifiedName name;
    private final List<Expression> expressions;

    public CallStatement(QualifiedName name, List<Expression> expressions)
    {
        this.name = checkNotNull(name, "name is null");
        this.expressions = checkNotNull(expressions, "expressions is null");
    }

    public QualifiedName getName()
    {
        return name;
    }

    public List<Expression> getExpressions()
    {
        return expressions;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitCallStatement(this, context);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, expressions);
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
        CallStatement o = (CallStatement) obj;
        return Objects.equals(name, o.name) &&
                Objects.equals(expressions, o.expressions);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("expressions", expressions)
                .toString();
    }
}
