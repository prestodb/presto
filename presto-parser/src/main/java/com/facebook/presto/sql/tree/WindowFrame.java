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
                Objects.equal(start, o.end) &&
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
