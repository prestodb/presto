package com.facebook.presto.sql.tree;

import com.google.common.base.Function;
import com.google.common.base.Objects;

/**
 * Represents a reference to a field in a physical execution plan
 * <p/>
 * TODO: This class belongs to the execution engine and should not be in this package.
 */
public class Input
{
    private final int channel;
    private final int field;

    public Input(int channel, int field)
    {
        this.channel = channel;
        this.field = field;
    }

    public int getChannel()
    {
        return channel;
    }

    public int getField()
    {
        return field;
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(channel, field);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final Input other = (Input) obj;
        return Objects.equal(this.channel, other.channel) && Objects.equal(this.field, other.field);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("channel", channel)
                .add("field", field)
                .toString();
    }

    public static Function<Input, Integer> channelGetter()
    {
        return new Function<Input, Integer>()
        {
            @Override
            public Integer apply(Input input)
            {
                return input.getChannel();
            }
        };
    }

    public static Function<Input, Integer> fieldGetter()
    {
        return new Function<Input, Integer>()
        {
            @Override
            public Integer apply(Input input)
            {
                return input.getField();
            }
        };
    }
}
