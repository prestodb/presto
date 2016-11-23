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
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class LambdaExpression
        extends Expression
{
    private final List<LambdaArgumentDeclaration> arguments;
    private final Expression body;

    public LambdaExpression(List<LambdaArgumentDeclaration> arguments, Expression body)
    {
        this(Optional.empty(), arguments, body);
    }

    public LambdaExpression(NodeLocation location, List<LambdaArgumentDeclaration> arguments, Expression body)
    {
        this(Optional.of(location), arguments, body);
    }

    private LambdaExpression(Optional<NodeLocation> location, List<LambdaArgumentDeclaration> arguments, Expression body)
    {
        super(location);
        this.arguments = requireNonNull(arguments, "arguments is null");
        this.body = requireNonNull(body, "body is null");
    }

    public List<LambdaArgumentDeclaration> getArguments()
    {
        return arguments;
    }

    public Expression getBody()
    {
        return body;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitLambdaExpression(this, context);
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
        LambdaExpression that = (LambdaExpression) obj;
        return Objects.equals(arguments, that.arguments) &&
                Objects.equals(body, that.body);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(arguments, body);
    }
}
