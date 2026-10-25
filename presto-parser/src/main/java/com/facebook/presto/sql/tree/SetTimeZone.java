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

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class SetTimeZone
        extends Statement
{
    private final Optional<Expression> timeZone;

    public SetTimeZone(Optional<Expression> timeZone)
    {
        this(Optional.empty(), timeZone);
    }

    public SetTimeZone(NodeLocation location, Optional<Expression> timeZone)
    {
        this(Optional.of(location), timeZone);
    }

    private SetTimeZone(Optional<NodeLocation> location, Optional<Expression> timeZone)
    {
        super(location);
        this.timeZone = requireNonNull(timeZone, "timeZone is null");
    }

    public Optional<Expression> getTimeZone()
    {
        return timeZone;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitSetTimeZone(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return timeZone.<List<Node>>map(ImmutableList::of).orElse(ImmutableList.of());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(timeZone);
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
        SetTimeZone o = (SetTimeZone) obj;
        return Objects.equals(timeZone, o.timeZone);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("timeZone", timeZone.orElse(null))
                .toString();
    }
}
