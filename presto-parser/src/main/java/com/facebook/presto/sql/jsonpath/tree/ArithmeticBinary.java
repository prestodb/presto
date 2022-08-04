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
package com.facebook.presto.sql.jsonpath.tree;

import static java.util.Objects.requireNonNull;

public class ArithmeticBinary
        extends PathNode
{
    private final Operator operator;
    private final PathNode left;
    private final PathNode right;

    public ArithmeticBinary(Operator operator, PathNode left, PathNode right)
    {
        this.operator = requireNonNull(operator, "operator is null");
        this.left = requireNonNull(left, "left is null");
        this.right = requireNonNull(right, "right is null");
    }

    @Override
    public <R, C> R accept(JsonPathTreeVisitor<R, C> visitor, C context)
    {
        return visitor.visitArithmeticBinary(this, context);
    }

    public Operator getOperator()
    {
        return operator;
    }

    public PathNode getLeft()
    {
        return left;
    }

    public PathNode getRight()
    {
        return right;
    }

    public enum Operator
    {
        ADD,
        SUBTRACT,
        MULTIPLY,
        DIVIDE,
        MODULUS
    }
}
