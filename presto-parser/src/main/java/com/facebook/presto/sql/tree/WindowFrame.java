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

import com.google.common.base.Objects;
import com.google.common.base.Optional;

import static com.google.common.base.Preconditions.checkNotNull;

public class WindowFrame
        extends Node
{
    public enum Type
    {
        RANGE, ROWS
    }

    private final Type type;
    private final FrameBound start;
    private final Optional<FrameBound> end;

    public WindowFrame(Type type, FrameBound start, FrameBound end)
    {
        this.type = checkNotNull(type, "type is null");
        this.start = checkNotNull(start, "start is null");
        this.end = Optional.fromNullable(end);
    }

    public Type getType()
    {
        return type;
    }

    public FrameBound getStart()
    {
        return start;
    }

    public Optional<FrameBound> getEnd()
    {
        return end;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitWindowFrame(this, context);
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
        WindowFrame o = (WindowFrame) obj;
        return Objects.equal(type, o.type) &&
                Objects.equal(start, o.start) &&
                Objects.equal(end, o.end);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(type, start, end);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("type", type)
                .add("start", start)
                .add("end", end)
                .toString();
    }
}
