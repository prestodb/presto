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

import static com.google.common.base.Preconditions.checkNotNull;

public class Cast
        extends Expression
{
    private final Expression expression;
    private final String type;

    public Cast(Expression expression, String type)
    {
        checkNotNull(expression, "expression is null");
        checkNotNull(type, "type is null");

        this.expression = expression;
        this.type = type.toUpperCase();
    }

    public Expression getExpression()
    {
        return expression;
    }

    public String getType()
    {
        return type;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitCast(this, context);
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

        Cast cast = (Cast) o;

        if (!expression.equals(cast.expression)) {
            return false;
        }
        if (!type.equals(cast.type)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = expression.hashCode();
        result = 31 * result + type.hashCode();
        return result;
    }
}
