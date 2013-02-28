package com.facebook.presto.sql.tree;

import com.google.common.base.Objects;
import com.google.common.base.Optional;

import static com.google.common.base.Preconditions.checkNotNull;

public class FrameBound
        extends Node
{
    public enum Type
    {
        UNBOUNDED_PRECEDING,
        PRECEDING,
        CURRENT_ROW,
        FOLLOWING,
        UNBOUNDED_FOLLOWING
    }

    private final Type type;
    private final Optional<Expression> value;

    public FrameBound(Type type)
    {
        this(type, null);
    }

    public FrameBound(Type type, Expression value)
    {
        this.type = checkNotNull(type, "type is null");
        this.value = Optional.fromNullable(value);
    }

    public Type getType()
    {
        return type;
    }

    public Optional<Expression> getValue()
    {
        return value;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitFrameBound(this, context);
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
        FrameBound o = (FrameBound) obj;
        return Objects.equal(type, o.type) &&
                Objects.equal(value, o.value);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(type, value);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("type", type)
                .add("value", value)
                .toString();
    }
}
