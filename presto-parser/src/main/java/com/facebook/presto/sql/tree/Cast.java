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

import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public final class Cast
        extends Expression
{
    private final Expression expression;
    private final String type;
    private final boolean safe;
    private final boolean typeOnly;

    public Cast(Expression expression, String type)
    {
        this(Optional.empty(), expression, type, false, false);
    }

    public Cast(Expression expression, String type, boolean safe)
    {
        this(Optional.empty(), expression, type, safe, false);
    }

    public Cast(Expression expression, String type, boolean safe, boolean typeOnly)
    {
        this(Optional.empty(), expression, type, safe, typeOnly);
    }

    public Cast(NodeLocation location, Expression expression, String type)
    {
        this(Optional.of(location), expression, type, false, false);
    }

    public Cast(NodeLocation location, Expression expression, String type, boolean safe)
    {
        this(Optional.of(location), expression, type, safe, false);
    }

    public Cast(NodeLocation location, Expression expression, String type, boolean safe, boolean typeOnly)
    {
        this(Optional.of(location), expression, type, safe, typeOnly);
    }

    private Cast(Optional<NodeLocation> location, Expression expression, String type, boolean safe, boolean typeOnly)
    {
        super(location);
        requireNonNull(expression, "expression is null");
        requireNonNull(type, "type is null");

        this.expression = expression;
        this.type = type.toLowerCase(ENGLISH);
        this.safe = safe;
        this.typeOnly = typeOnly;
    }

    public Expression getExpression()
    {
        return expression;
    }

    public String getType()
    {
        return type;
    }

    public boolean isSafe()
    {
        return safe;
    }

    public boolean isTypeOnly()
    {
        return typeOnly;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitCast(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of(expression);
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
        Cast o = (Cast) obj;
        return Objects.equals(this.expression, o.expression) &&
                Objects.equals(this.type, o.type) &&
                Objects.equals(this.safe, o.safe) &&
                Objects.equals(this.typeOnly, o.typeOnly);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(expression, type, safe, typeOnly);
    }
}
