package com.facebook.presto.spi.exchange;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class ExchangeId
{
    private final String id;

    public ExchangeId(String id)
    {
        requireNonNull(id, "id is null");
        if (id.isEmpty()) {
            throw new IllegalArgumentException("id is empty");
        }
        this.id = id;
    }

    public String getId()
    {
        return id;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ExchangeId that = (ExchangeId) o;
        return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(id);
    }

    @Override
    public String toString()
    {
        return id;
    }
}
