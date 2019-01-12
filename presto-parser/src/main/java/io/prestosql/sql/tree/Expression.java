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
package io.prestosql.sql.tree;

import io.prestosql.sql.ExpressionFormatter;

import java.util.Optional;

public abstract class Expression
        extends Node
{
    protected Expression(Optional<NodeLocation> location)
    {
        super(location);
    }

    /**
     * Accessible for {@link AstVisitor}, use {@link AstVisitor#process(Node, Object)} instead.
     */
    @Override
    protected <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitExpression(this, context);
    }

    @Override
    public final String toString()
    {
        return ExpressionFormatter.formatExpression(this, Optional.empty()); // This will not replace parameters, but we don't have access to them here
    }
}
