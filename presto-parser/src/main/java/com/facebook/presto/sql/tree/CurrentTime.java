package com.facebook.presto.sql.tree;

import static com.google.common.base.Preconditions.checkNotNull;

public class CurrentTime
    extends Expression
{
    private final Type type;
    private final Integer precision;

    public enum Type {
        TIME("current_time"),
        DATE("current_date"),
        TIMESTAMP("current_timestamp");

        private final String name;

        private Type(String name)
        {
            this.name = name;
        }

        public String getName()
        {
            return name;
        }
    }

    public CurrentTime(Type type)
    {
        this(type, null);
    }

    public CurrentTime(Type type, Integer precision)
    {
        checkNotNull(type, "type is null");
        this.type = type;
        this.precision = precision;
    }

    public Type getType()
    {
        return type;
    }

    public Integer getPrecision()
    {
        return precision;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitCurrentTime(this, context);
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

        CurrentTime that = (CurrentTime) o;

        if (precision != null ? !precision.equals(that.precision) : that.precision != null) {
            return false;
        }
        if (type != that.type) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = type.hashCode();
        result = 31 * result + (precision != null ? precision.hashCode() : 0);
        return result;
    }
}
