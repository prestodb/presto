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

/**
 * NULLIF(V1,V2): CASE WHEN V1=V2 THEN NULL ELSE V1 END
 */
public class NullIfExpression
        extends Expression
{
    private final Expression first;
    private final Expression second;

    public NullIfExpression(Expression first, Expression second)
    {
        this.first = first;
        this.second = second;
    }

    public Expression getFirst()
    {
        return first;
    }

    public Expression getSecond()
    {
        return second;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitNullIfExpression(this, context);
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

        NullIfExpression that = (NullIfExpression) o;

        if (!first.equals(that.first)) {
            return false;
        }
        if (!second.equals(that.second)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = first.hashCode();
        result = 31 * result + second.hashCode();
        return result;
    }
}
