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

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class WhenClause
        extends Expression
{
    private final Expression operand;
    private final Expression result;

    public WhenClause(Expression operand, Expression result)
    {
        this(Optional.empty(), operand, result);
    }

    public WhenClause(NodeLocation location, Expression operand, Expression result)
    {
        this(Optional.of(location), operand, result);
    }

    private WhenClause(Optional<NodeLocation> location, Expression operand, Expression result)
    {
        super(location);
        this.operand = operand;
        this.result = result;
    }

    public Expression getOperand()
    {
        return operand;
    }

    public Expression getResult()
    {
        return result;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitWhenClause(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of(operand, result);
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

        WhenClause that = (WhenClause) o;
        return Objects.equals(operand, that.operand) &&
                Objects.equals(result, that.result);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(operand, result);
    }
}
