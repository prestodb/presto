package com.facebook.presto.spi.exchange;

import com.facebook.presto.spi.QueryId;

import static java.util.Objects.requireNonNull;

public class ExchangeContext
{
    private final QueryId queryId;
    private final ExchangeId exchangeId;

    public ExchangeContext(QueryId queryId, ExchangeId exchangeId)
    {
        this.queryId = requireNonNull(queryId, "queryId is null");
        this.exchangeId = requireNonNull(exchangeId, "exchangeId is null");
    }

    public QueryId getQueryId()
    {
        return queryId;
    }

    public ExchangeId getExchangeId()
    {
        return exchangeId;
    }

    @Override
    public String toString()
    {
        return "ExchangeContext{queryId=" + queryId + ", exchangeId=" + exchangeId + "}";
    }
}
