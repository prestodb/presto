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

public class ArithmeticUnary
        extends PathNode
{
    private final Sign sign;
    private final PathNode base;

    public ArithmeticUnary(Sign sign, PathNode base)
    {
        this.sign = requireNonNull(sign, "sign is null");
        this.base = requireNonNull(base, "base is null");
    }

    @Override
    public <R, C> R accept(JsonPathTreeVisitor<R, C> visitor, C context)
    {
        return visitor.visitArithmeticUnary(this, context);
    }

    public Sign getSign()
    {
        return sign;
    }

    public PathNode getBase()
    {
        return base;
    }

    public enum Sign
    {
        PLUS,
        MINUS
    }
}
