package com.facebook.presto.operator;

import com.google.common.base.Objects;

/**
 * Represents a reference to a field in a physical execution plan
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
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("channel", channel)
                .add("field", field)
                .toString();
    }
}
