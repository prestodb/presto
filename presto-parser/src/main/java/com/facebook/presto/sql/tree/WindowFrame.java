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

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class WindowFrame
        extends Node
{
    public enum Type
    {
        RANGE, ROWS
    }

    private final Type type;
    private final FrameBound start;
    private final FrameBound end;

    public WindowFrame(Type type, FrameBound start, FrameBound end)
    {
        this(Optional.empty(), type, start, end);
    }

    public WindowFrame(NodeLocation location, Type type, FrameBound start, FrameBound end)
    {
        this(Optional.of(location), type, start, end);
    }

    private WindowFrame(Optional<NodeLocation> location, Type type, FrameBound start, FrameBound end)
    {
        super(location);
        this.type = requireNonNull(type, "type is null");
        this.start = requireNonNull(start, "start is null");
        this.end = requireNonNull(end, "end is null");
    }

    public static WindowFrame defaultFrame(NodeLocation nodeLocation)
    {
        return new WindowFrame(
                nodeLocation,
                Type.RANGE,
                new FrameBound(nodeLocation, FrameBound.Type.UNBOUNDED_PRECEDING),
                new FrameBound(nodeLocation, FrameBound.Type.CURRENT_ROW));
    }

    public Type getType()
    {
        return type;
    }

    public FrameBound getStart()
    {
        return start;
    }

    public FrameBound getEnd()
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
        return Objects.equals(type, o.type) &&
                Objects.equals(start, o.start) &&
                Objects.equals(end, o.end);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(type, start, end);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("type", type)
                .add("start", start)
                .add("end", end)
                .toString();
    }
}
