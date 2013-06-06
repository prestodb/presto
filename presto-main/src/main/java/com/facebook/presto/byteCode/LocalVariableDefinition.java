/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.byteCode;

import javax.annotation.concurrent.Immutable;

@Immutable
public class LocalVariableDefinition
{
    private final String name;
    private final int slot;
    private final ParameterizedType type;

    public LocalVariableDefinition(String name, int slot, ParameterizedType type)
    {
        this.name = name;
        this.slot = slot;
        this.type = type;
    }

    public String getName()
    {
        return name;
    }

    public int getSlot()
    {
        return slot;
    }

    public ParameterizedType getType()
    {
        return type;
    }

    @Override
    public String toString()
    {
        final StringBuilder sb = new StringBuilder();
        sb.append("LocalVariableDefinition");
        sb.append("{name='").append(name).append('\'');
        sb.append(", slot=").append(slot);
        sb.append('}');
        return sb.toString();
    }
}
