package com.facebook.presto.spi;

final class SchemaUtil
{
    private SchemaUtil()
    {
    }

    static String checkNotEmpty(String value, String name)
    {
        if (value == null) {
            throw new NullPointerException(name + " is null");
        }
        if (value.isEmpty()) {
            throw new IllegalArgumentException(name + " is empty");
        }
        return value;
    }
}
