package com.facebook.presto.spi.function;

public @interface TypeParameterBinding
{
    String parameter();
    String type();
}
