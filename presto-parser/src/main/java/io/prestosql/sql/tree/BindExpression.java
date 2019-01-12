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

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * "$INTERNAL$BIND"(value, targetFunction)
 * <p>
 * When invoked, the returned function inserts the given value as
 * the leading argument to the targetFunction.  The other arguments are
 * passed unchanged.
 * <p>
 * Bind is fundamentally first class, just like function applications.
 * It requires special treatment from the type system. There is no way
 * to write out the type of `bind`. The return type of `bind` is function.
 * Its type looks somewhat like:
 * <p><ul>
 * <li>X, (X) -> Y => () -> Y
 * <li>X1, (X1, X2) -> Y => (X2) -> Y
 * <li>X1, (X1, X2, X3) -> Y => (X2, X3) -> Y
 * <li>X1, (X1, X2, X3, X4) -> Y => (X2, X3, X4) -> Y
 * <li>...
 * </ul>
 * <p>
 * Lambda capturing is implemented through desugaring in Presto. This
 * expression facilitates desugaring.
 */
public class BindExpression
        extends Expression
{
    private final List<Expression> values;
    // Function expression must be of function type.
    // It is not necessarily a lambda. For example, it can be another bind expression.
    private final Expression function;

    public BindExpression(List<Expression> values, Expression function)
    {
        this(Optional.empty(), values, function);
    }

    public BindExpression(NodeLocation location, List<Expression> values, Expression function)
    {
        this(Optional.of(location), values, function);
    }

    private BindExpression(Optional<NodeLocation> location, List<Expression> values, Expression function)
    {
        super(location);
        this.values = requireNonNull(values, "value is null");
        this.function = requireNonNull(function, "function is null");
    }

    public List<Expression> getValues()
    {
        return values;
    }

    public Expression getFunction()
    {
        return function;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitBindExpression(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        ImmutableList.Builder<Node> nodes = ImmutableList.builder();
        return nodes.addAll(values)
                .add(function)
                .build();
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
        BindExpression that = (BindExpression) o;
        return Objects.equals(values, that.values) &&
                Objects.equals(function, that.function);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(values, function);
    }
}
