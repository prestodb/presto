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

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class AtTimeZone
        extends Expression
{
    private final Expression value;
    private final Expression timeZone;

    public AtTimeZone(Expression value, Expression timeZone)
    {
        this(Optional.empty(), value, timeZone);
    }

    public AtTimeZone(NodeLocation location, Expression value, Expression timeZone)
    {
        this(Optional.of(location), value, timeZone);
    }

    private AtTimeZone(Optional<NodeLocation> location, Expression value, Expression timeZone)
    {
        super(location);
        checkArgument(timeZone instanceof IntervalLiteral || timeZone instanceof StringLiteral, "timeZone must be IntervalLiteral or StringLiteral");
        this.value = requireNonNull(value, "value is null");
        this.timeZone = requireNonNull(timeZone, "timeZone is null");
    }

    public Expression getValue()
    {
        return value;
    }

    public Expression getTimeZone()
    {
        return timeZone;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitAtTimeZone(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of(value, timeZone);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(value, timeZone);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        AtTimeZone atTimeZone = (AtTimeZone) obj;
        return Objects.equals(value, atTimeZone.value) && Objects.equals(timeZone, atTimeZone.timeZone);
    }
}
