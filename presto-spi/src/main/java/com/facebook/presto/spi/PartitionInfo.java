package com.facebook.presto.spi;

import java.util.LinkedHashMap;
import java.util.Map;

import static java.util.Collections.unmodifiableMap;

public class PartitionInfo
{
    private final String name;
    private final Map<String, String> keys;

    public PartitionInfo(String name, Map<String, String> keys)
    {
        if (name == null) {
            throw new NullPointerException("name is null");
        }
        if (keys == null) {
            throw new NullPointerException("keys is null");
        }

        this.name = name;
        this.keys = unmodifiableMap(new LinkedHashMap<>(keys));
    }

    public String getName()
    {
        return name;
    }

    /**
     * Gets the values associated with each partition key for this partition
     */
    public Map<String, String> getKeyFields()
    {
        return keys;
    }

    @Override
    public String toString()
    {
        return "PartitionInfo{" +
                "name='" + name + '\'' +
                ", keys=" + keys +
                '}';
    }
}
