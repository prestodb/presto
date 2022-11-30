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
package com.facebook.presto.analyzer.crux.tree;

import static java.util.Objects.requireNonNull;

public class Expression
        extends SemanticTree
{
    private final Kind kind;
    private final Type type;
    private final ExpressionCardinality cardinality;

    public Expression(Kind kind,
            CodeLocation location,
            Type type,
            ExpressionCardinality cardinality)
    {
        super(SemanticTree.Kind.Expression, location);
        this.kind = requireNonNull(kind, "expression kind is null");
        this.type = requireNonNull(type, "type is null");
        this.cardinality = requireNonNull(cardinality, "cardinality is null");
    }

    public Kind getExpressionKind()
    {
        return this.kind;
    }

    public Type getType()
    {
        return type;
    }

    public ExpressionCardinality getCardinality()
    {
        return cardinality;
    }

    public boolean isLiteralExpression()
    {
        return this instanceof LiteralExpression;
    }

    public LiteralExpression asLiteralExpression()
    {
        return (LiteralExpression) this;
    }

    public enum Kind
    {
        LiteralExpression
    }
}
