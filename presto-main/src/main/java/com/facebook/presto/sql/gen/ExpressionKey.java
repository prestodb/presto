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
package com.facebook.presto.sql.gen;

import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Node;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;

import java.util.IdentityHashMap;
import java.util.List;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class ExpressionKey
{
    private final Expression expression;
    private final List<Type> types;

    public ExpressionKey(Expression expression, final IdentityHashMap<Expression, Type> expressionTypes)
    {
        this.expression = checkNotNull(expression, "expression is null");
        checkNotNull(expressionTypes, "expressionTypes is null");

        // extract the types of every expression node
        final ImmutableList.Builder<Type> types = ImmutableList.builder();
        Type type = checkNotNull(expressionTypes.get(expression), "Expression types does not contain an entry for %s", expression);
        types.add(type);

        expression.accept(new DefaultTraversalVisitor<Void, Void>() {
            @Override
            public Void process(Node node, @Nullable Void context)
            {
                Expression expression = (Expression) node;
                Type type = expressionTypes.get(expression);
                checkState(expressionTypes.get(expression) != null, "Expression types does not contain an entry for %s", expression);
                types.add(type);

                super.process(node, context);

                return null;
            }
        }, null);
        this.types = types.build();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(expression, types);
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
        ExpressionKey other = (ExpressionKey) obj;
        return Objects.equals(this.expression, other.expression) &&
                Objects.equals(this.types, other.types);
    }

    @Override
    public String toString()
    {
        return com.google.common.base.Objects.toStringHelper(this)
                .add("expression", expression)
                .toString();
    }
}
