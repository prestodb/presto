/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.execution.ExchangePlanFragmentSource;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.metadata.ColumnHandle;
import com.facebook.presto.operator.ForExchange;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.split.DataStreamProvider;
import com.facebook.presto.sql.planner.PlanFragmentSource;
import com.facebook.presto.sql.planner.PlanFragmentSourceProvider;
import com.facebook.presto.sql.planner.TableScanPlanFragmentSource;
import com.google.common.base.Function;
import io.airlift.http.client.AsyncHttpClient;
import io.airlift.json.JsonCodec;

import javax.annotation.concurrent.Immutable;
import javax.inject.Inject;
import java.net.URI;
import java.util.List;
import java.util.Map.Entry;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.transform;

@Immutable
public class HackPlanFragmentSourceProvider
        implements PlanFragmentSourceProvider
{
    private final DataStreamProvider dataStreamProvider;
    private final JsonCodec<TaskInfo> taskInfoCodec;
    private final int pageBufferMax;

    private final AsyncHttpClient httpClient;

    @Inject
    public HackPlanFragmentSourceProvider(DataStreamProvider dataStreamProvider, @ForExchange AsyncHttpClient httpClient, JsonCodec<TaskInfo> taskInfoCodec)
    {
        this.dataStreamProvider = checkNotNull(dataStreamProvider, "dataStreamProvider is null");
        this.httpClient = httpClient;
        this.taskInfoCodec = checkNotNull(taskInfoCodec, "taskInfoCodec is null");

        this.pageBufferMax = 10;
    }

    @Override
    public Operator createDataStream(PlanFragmentSource source, List<ColumnHandle> columns)
    {
        checkNotNull(source, "source is null");
        if (source instanceof ExchangePlanFragmentSource) {
            final ExchangePlanFragmentSource exchangeSource = (ExchangePlanFragmentSource) source;
            return new QueryDriversOperator(pageBufferMax, exchangeSource.getTupleInfos(), transform(exchangeSource.getSources().entrySet(),
                    new Function<Entry<String, URI>, QueryDriverProvider>()
                    {
                        @Override
                        public QueryDriverProvider apply(Entry<String, URI> source)
                        {
                            return new HttpTaskClient(
                                    source.getKey(),
                                    source.getValue(),
                                    exchangeSource.getOutputId(),
                                    httpClient,
                                    taskInfoCodec
                            );
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
