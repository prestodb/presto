package com.facebook.prism.namespaceservice;

import com.facebook.swift.codec.ThriftEnumValue;

public enum PrismErrorCode
{
    CONNECT__ERROR(1), LOAD__OBJECT__ERROR(2);

    private final int value;

    PrismErrorCode(int value)
    {
        this.value = value;
    }

    @ThriftEnumValue
    public int getValue()
    {
        return value;
    }
}
