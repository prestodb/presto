package com.facebook.presto.metadata;

public interface JsonTypeIdResolver<T>
{
    String getId(T value);

    Class<? extends T> getType(String id);
}
