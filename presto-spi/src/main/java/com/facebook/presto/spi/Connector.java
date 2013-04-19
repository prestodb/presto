package com.facebook.presto.spi;

public interface Connector
{
    <T> T getService(Class<T> type);
}
