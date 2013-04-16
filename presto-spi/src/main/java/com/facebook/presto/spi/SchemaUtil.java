package com.facebook.presto.spi;

final class SchemaUtil
{
    private SchemaUtil()
    {
    }

    static String checkLowerCase(String value, String name)
    {
        if (value == null) {
            throw new NullPointerException(name + " is null");
        }
        if (value.isEmpty()) {
            throw new IllegalArgumentException(name + " is empty");
        }
        if (!value.equals(value.toLowerCase())) {
            throw new IllegalArgumentException(name + " is not lowercase");
        }
        return value;
    }

}
