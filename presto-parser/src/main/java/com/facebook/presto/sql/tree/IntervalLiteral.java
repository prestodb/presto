package com.facebook.presto.sql.tree;

import com.google.common.base.Preconditions;

public class IntervalLiteral
        extends Literal
{
    public enum Sign
    {
        POSITIVE, NEGATIVE
    }

    private final String value;
    private final String type;
    private final Sign sign;

    private final long seconds;
    private final long months;
    private final boolean yearToMonth;

    public IntervalLiteral(String value, String type, Sign sign)
    {
        Preconditions.checkNotNull(value, "value is null");
        Preconditions.checkNotNull(type, "type is null");

        this.value = value;
        this.type = type;
        this.sign = sign;

        int signValue = (sign == Sign.POSITIVE) ? 1 : -1;
        switch (type.toUpperCase()) {
            case "YEAR":
                months = signValue * Long.parseLong(value) * 12;
                seconds = 0;
                yearToMonth = true;
                break;
            case "MONTH":
                months = signValue * Long.parseLong(value);
                seconds = 0;
                yearToMonth = true;
                break;
            case "DAY":
                months = 0;
                seconds = signValue * Long.parseLong(value) * 60 * 60 * 24;
                yearToMonth = false;
                break;
            case "HOUR":
                months = 0;
                seconds = signValue * Long.parseLong(value) * 60 * 60;
                yearToMonth = false;
                break;
            case "MINUTE":
                months = 0;
                seconds = signValue * Long.parseLong(value) * 60;
                yearToMonth = false;
                break;
            case "SECOND":
                months = 0;
                seconds = signValue * Long.parseLong(value);
                yearToMonth = false;
                break;
            default:
                throw new IllegalArgumentException("Unsupported INTERVAL type " + type);
        }
    }

    public String getValue()
    {
        return value;
    }

    public String getType()
    {
        return type;
    }

    public Sign getSign()
    {
        return sign;
    }

    public long getMonths()
    {
        return months;
    }

    public long getSeconds()
    {
        return seconds;
    }

    public boolean isYearToMonth()
    {
        return yearToMonth;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitIntervalLiteral(this, context);
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

        IntervalLiteral that = (IntervalLiteral) o;

        if (sign != that.sign) {
            return false;
        }
        if (!type.equals(that.type)) {
            return false;
        }
        if (!value.equals(that.value)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = value.hashCode();
        result = 31 * result + type.hashCode();
        result = 31 * result + (sign != null ? sign.hashCode() : 0);
        return result;
    }
}
