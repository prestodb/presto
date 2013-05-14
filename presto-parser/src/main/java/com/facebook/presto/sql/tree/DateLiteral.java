package com.facebook.presto.sql.tree;

import com.google.common.base.Preconditions;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class DateLiteral
        extends Literal
{
    public static final DateTimeFormatter DATE_FORMATTER = ISODateTimeFormat.date().withZoneUTC();

    private final String value;
    private final long unixTime;

    public DateLiteral(String value)
    {
        Preconditions.checkNotNull(value, "value is null");
        this.value = value;
        unixTime = MILLISECONDS.toSeconds(DATE_FORMATTER.parseMillis(value));
    }

    public String getValue()
    {
        return value;
    }

    public long getUnixTime()
    {
        return unixTime;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitDateLiteral(this, context);
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

        DateLiteral that = (DateLiteral) o;

        if (!value.equals(that.value)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        return value.hashCode();
    }
}
