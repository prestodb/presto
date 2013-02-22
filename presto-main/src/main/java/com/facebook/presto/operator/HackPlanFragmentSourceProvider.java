/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.operator;

import com.facebook.presto.execution.ExchangePlanFragmentSource;
import com.facebook.presto.execution.QueryManagerConfig;
import com.facebook.presto.metadata.ColumnHandle;
import com.facebook.presto.split.DataStreamProvider;
import com.facebook.presto.sql.planner.PlanFragmentSource;
import com.facebook.presto.sql.planner.PlanFragmentSourceProvider;
import com.facebook.presto.sql.planner.TableScanPlanFragmentSource;
import com.google.common.base.Function;
import io.airlift.http.client.AsyncHttpClient;

import javax.annotation.concurrent.Immutable;
import javax.inject.Inject;
import java.net.URI;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.transform;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;

@Immutable
public class HackPlanFragmentSourceProvider
        implements PlanFragmentSourceProvider
{
    private final DataStreamProvider dataStreamProvider;
    private final AsyncHttpClient httpClient;
    private final int exchangeConcurrentRequestMultiplier;
    private final int exchangeMaxBufferedPages;
    private final int exchangeExpectedPagesPerRequest;

    @Inject
    public HackPlanFragmentSourceProvider(DataStreamProvider dataStreamProvider, @ForExchange AsyncHttpClient httpClient, QueryManagerConfig queryManagerConfig)
    {
        this.dataStreamProvider = checkNotNull(dataStreamProvider, "dataStreamProvider is null");
        this.httpClient = httpClient;
        this.exchangeMaxBufferedPages = queryManagerConfig.getExchangeMaxBufferedPages();
        this.exchangeExpectedPagesPerRequest = queryManagerConfig.getExchangeExpectedPagesPerRequest();
        this.exchangeConcurrentRequestMultiplier = queryManagerConfig.getExchangeConcurrentRequestMultiplier();
    }

    @Override
    public Operator createDataStream(PlanFragmentSource source, List<ColumnHandle> columns)
    {
        checkNotNull(source, "source is null");
        if (source instanceof ExchangePlanFragmentSource) {
            final ExchangePlanFragmentSource exchangeSource = (ExchangePlanFragmentSource) source;
            return new ExchangeOperator(httpClient,
                    exchangeSource.getTupleInfos(),
                    exchangeMaxBufferedPages,
                    exchangeExpectedPagesPerRequest,
                    exchangeConcurrentRequestMultiplier,
                    transform(exchangeSource.getSources().values(),
                            new Function<URI, URI>()
                            {
                                @Override
                                public URI apply(URI location)
                                {
                                    return uriBuilderFrom(location).appendPath("results").appendPath(exchangeSource.getOutputId()).build();
                                }
                            }));
        }
        else if (source instanceof TableScanPlanFragmentSource) {
            TableScanPlanFragmentSource tableScanSource = (TableScanPlanFragmentSource) source;
            return dataStreamProvider.createDataStream(tableScanSource.getSplit(), columns);
        }

        throw new IllegalArgumentException("Unsupported source type " + source.getClass().getName());
    }
}
