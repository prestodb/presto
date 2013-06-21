package com.facebook.presto.spi;


public interface PartitionKey
{
    public String getName();

    public ColumnType getType();

    public String getValue();
}
