package com.facebook.presto.sql.tree;

import com.google.common.base.Preconditions;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class TimeLiteral
        extends Literal
{
    public static final DateTimeFormatter DATE_TIME_FORMATTER;

    static {
        DATE_TIME_FORMATTER = new DateTimeFormatterBuilder()
                .appendHourOfDay(2)
                .appendLiteral(':')
                .appendMinuteOfHour(2)
                .appendOptional(new DateTimeFormatterBuilder()
                        .appendLiteral(':')
                        .appendSecondOfMinute(2)
                        .appendOptional(new DateTimeFormatterBuilder()
                                .appendLiteral('.')
                                .appendMillisOfSecond(1)
                                .toParser())
                        .toParser())
                .appendOptional(new DateTimeFormatterBuilder()
                        .appendTimeZoneOffset("Z", true, 1, 2)
                        .toParser())
                .appendOptional(new DateTimeFormatterBuilder()
                        .appendLiteral(' ')
                        .appendTimeZoneId()
                        .toParser())
                .toFormatter()
                .withZoneUTC();
    }

    private final String value;
    private final long unixTime;

    public TimeLiteral(String value)
    {
        Preconditions.checkNotNull(value, "value is null");
        this.value = value;
        unixTime = MILLISECONDS.toSeconds(DATE_TIME_FORMATTER.parseMillis(value));
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
        return visitor.visitTimeLiteral(this, context);
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

        TimeLiteral that = (TimeLiteral) o;

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
