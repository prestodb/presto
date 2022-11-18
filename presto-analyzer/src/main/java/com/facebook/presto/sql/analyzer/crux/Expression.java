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
package com.facebook.presto.sql.analyzer.crux;

import static java.util.Objects.requireNonNull;

public class Expression
        extends SemanticTree
{
    private final ExpressionKind expressionKind;
    private final Type type;

    protected Expression(ExpressionKind expressionKind, CodeLocation location, Type type)
    {
        super(SemanticTreeKind.EXPRESSION, location);
        this.expressionKind = requireNonNull(expressionKind, "expressionKind is null");
        this.type = requireNonNull(type, "type is null");
    }

    public ExpressionKind getExpressionKind()
    {
        return expressionKind;
    }

    public Type getType()
    {
        return type;
    }

    public boolean isLiteral()
    {
        return this instanceof LiteralExpression;
    }

    public LiteralExpression asLiteral()
    {
        return (LiteralExpression) this;
    }

    public boolean isCall()
    {
        return this instanceof CallExpression;
    }

    public CallExpression asCall()
    {
        return (CallExpression) this;
    }

    public boolean isFunction()
    {
        return this instanceof FunctionExpression;
    }

    public FunctionExpression asFunction()
    {
        return (FunctionExpression) this;
    }

    public boolean isColumnReference()
    {
        return this instanceof ColumnReferenceExpression;
    }

    public ColumnReferenceExpression asColumnReference()
    {
        return (ColumnReferenceExpression) this;
    }
}
