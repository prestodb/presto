/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.operator;

import com.facebook.airlift.http.client.FullJsonResponseHandler;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.bloomfilter.BloomFilterForDynamicFilter;
import com.facebook.presto.bloomfilter.BloomFilterForDynamicFilterImpl;
import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.expressions.DynamicFilters;
import com.facebook.presto.expressions.LogicalRowExpressions;
import com.facebook.presto.metadata.CastType;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;

import javax.ws.rs.core.Response;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static io.airlift.slice.Slices.utf8Slice;
import static java.util.Objects.requireNonNull;

public class DynamicFilterCollect
{
    public static final int MAX_RETRIES = 100;
    private static final Logger log = Logger.get(DynamicFilterCollect.class);
    public static final int DYNAMIC_FILTER_RECEIVE_TIMEOUT_MILLIS = 10000;
    private final Metadata metadata;
    private final ConcurrentHashMap<DynamicFilters.DynamicFilterPlaceholder, DynamicProcessorState> dynamicFilterState = new ConcurrentHashMap<>();
    private final AtomicReference<BloomFilterForDynamicFilter> bloomFilterForDynamicFilter = new AtomicReference<>(null);
    private final long autograph;
    private final Optional<RowExpression> staticFilter;
    private final List<DynamicFilters.DynamicFilterPlaceholder> dynamicFilters;
    private final DynamicFilterClient client;
    private final QueryId queryId;
    private List<DynamicFilterCollectorCallback> listeners = new ArrayList();

    enum DynamicProcessorState
    {
        NOTFOUND,
        DONE,
        FAILED
    }

    protected DynamicProcessorState globalState = DynamicProcessorState.NOTFOUND;
    private ExecutorService executorService = Executors.newSingleThreadExecutor(new ThreadFactory() {
        public Thread newThread(Runnable r)
        {
            Thread thread = new Thread(r);
            return thread;
        }
    });

    public DynamicFilterCollect(Metadata metadata,
            List<DynamicFilters.DynamicFilterPlaceholder> dynamicFilters,
            Optional<RowExpression> staticFilter,
            DynamicFilterClient client,
            QueryId queryId)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.dynamicFilters = requireNonNull(dynamicFilters, "dynamicFilters is null");
        this.staticFilter = requireNonNull(staticFilter, "staticFilter is null");
        this.client = requireNonNull(client, "client is null");
        this.queryId = requireNonNull(queryId, "queryId is null");
        this.autograph = Long.parseLong(queryId.toString().split("_")[1] + queryId.toString().split("_")[2] + dynamicFilters.get(0).getSource());
        this.dynamicFilters.stream().forEach(d -> dynamicFilterState.put(d, DynamicProcessorState.NOTFOUND));
    }

    public interface DynamicFilterCollectorCallback
    {
        void onSuccess(RowExpression result);
    }

    private void fireDynamicFilterCollection(final DynamicFilters.DynamicFilterPlaceholder dynamicFilterPlaceholder)
    {
        try {
            final ListenableFuture result = client.getSummary(queryId.toString(), dynamicFilterPlaceholder.getSource());
            final Object response = result.get(DYNAMIC_FILTER_RECEIVE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
            final FullJsonResponseHandler.JsonResponse<DynamicFilterSummary> jsonResponse = (FullJsonResponseHandler.JsonResponse<DynamicFilterSummary>) response;
            if (jsonResponse.getStatusCode() == Response.Status.OK.getStatusCode()) {
                final DynamicFilterSummary summary = jsonResponse.getValue();
                if (bloomFilterForDynamicFilter.get() == null) {
                    bloomFilterForDynamicFilter.set(summary.getBloomFilterForDynamicFilter());
                }
                else {
                    bloomFilterForDynamicFilter.accumulateAndGet(summary.getBloomFilterForDynamicFilter(),
                            BloomFilterForDynamicFilter::merge);
                }
                dynamicFilterState.put(dynamicFilterPlaceholder, DynamicProcessorState.DONE);
            }
            else if (jsonResponse.getStatusCode() == Response.Status.NOT_FOUND.getStatusCode()) {
                dynamicFilterState.put(dynamicFilterPlaceholder, DynamicProcessorState.NOTFOUND);
            }
            else {
                dynamicFilterState.put(dynamicFilterPlaceholder, DynamicProcessorState.FAILED);
            }
        }
        catch (Exception exp) {
            exp.printStackTrace();
            dynamicFilterState.put(dynamicFilterPlaceholder, DynamicProcessorState.FAILED);
        }
    }

    public void collectDynamicSummaryAsync()
    {
        CompletableFuture.runAsync(this::collectDynamicSummary, executorService).whenComplete((value, exception) ->
        {
            this.cleanUp();
        });
    }

    private void cleanUp()
    {
        //TODO: RejectedExecutionException
        //es.shutdown();
    }

    private void collectDynamicSummary()
    {
        int retry = 0;
        while (retry < MAX_RETRIES) {
            if (globalState == DynamicProcessorState.DONE || globalState == DynamicProcessorState.FAILED) {
                break;
            }
            else {
                collectDynamicSummaryInternal();
            }
            retry++;
            try {
                Thread.sleep(1000);
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        if (retry == MAX_RETRIES) {
            afterDynamicFilterCollection();
        }
    }

    private void collectDynamicSummaryInternal()
    {
        boolean inProgress = false;
        for (ConcurrentHashMap.Entry<DynamicFilters.DynamicFilterPlaceholder, DynamicProcessorState> e : dynamicFilterState.entrySet()) {
            DynamicProcessorState state = e.getValue();
            if (state != DynamicProcessorState.DONE && state != DynamicProcessorState.FAILED) {
                inProgress = true;
                fireDynamicFilterCollection(e.getKey());
            }
        }
        if (!inProgress) {
            afterDynamicFilterCollection();
        }
    }

    private void afterDynamicFilterCollection()
    {
        if (globalState == DynamicProcessorState.DONE || globalState == DynamicProcessorState.FAILED) {
            return;
        }

        BloomFilterForDynamicFilter finalBloomFilter = bloomFilterForDynamicFilter.get();

        if (((BloomFilterForDynamicFilterImpl) finalBloomFilter).getBloomFilter() != null && !((BloomFilterForDynamicFilterImpl) finalBloomFilter).getBloomFilter().toString().equals("{}")) {
            try {
                ObjectMapper objectMapper = new ObjectMapper();
                String json = objectMapper.writeValueAsString(finalBloomFilter);

                FunctionAndTypeManager functionManager = metadata.getFunctionAndTypeManager();

                List<RowExpression> arguments = extractDynamicFilterArguments();
                ImmutableList.Builder<RowExpression> castBuilder = ImmutableList.builder();
                for (int i = 0; i < arguments.size(); i++) {
                    Type type = arguments.get(i).getType();
                    FunctionHandle castFunctionHandle = functionManager.lookupCast(CastType.TRY_CAST, type.getTypeSignature(), VARCHAR.getTypeSignature());
                    CallExpression callExpression = new CallExpression("cast", castFunctionHandle, VARCHAR, ImmutableList.of(arguments.get(i)));
                    castBuilder.add(callExpression);
                }
                List<RowExpression> castCallExpression = castBuilder.build();

                FunctionHandle arrayFunctionHandle = functionManager.lookupFunction("array_constructor", fromTypes(castCallExpression.stream().map(RowExpression::getType).collect(
                        Collectors.toList())));
                CallExpression arrayExpression = new CallExpression("array", arrayFunctionHandle, new ArrayType(VARCHAR), castCallExpression);

                FunctionHandle arrayJoinFunctionHandle = functionManager.lookupFunction("array_join", fromTypes(new ArrayType(VARCHAR), VARCHAR));
                CallExpression arrayJoinCallExpression = new CallExpression("join", arrayJoinFunctionHandle, VARCHAR, ImmutableList.of(arrayExpression, constant(utf8Slice(""), VARCHAR)));

                FunctionHandle inBloomfilterFunctionHandle = functionManager.lookupFunction("BLOOM_FILTER_CONTAINS", fromTypes(BIGINT, VARCHAR, VARCHAR));
                CallExpression inBloomfilterCallExpression = new CallExpression(
                        "bloom_filter_contains",
                        inBloomfilterFunctionHandle,
                        BOOLEAN,
                        ImmutableList.of(
                                constant(autograph, BIGINT),
                                constant(utf8Slice(json), VARCHAR),
                                arrayJoinCallExpression));
                final Optional<RowExpression> rowExpression = Optional.of(inBloomfilterCallExpression);

                // TODO
                Optional<RowExpression> andRowExpression;
                if (staticFilter.isPresent() && rowExpression.isPresent()) {
                    andRowExpression = Optional.of(
                            LogicalRowExpressions.and(staticFilter.get(), rowExpression.get()));
                }
                else if (staticFilter.isPresent()) {
                    andRowExpression = staticFilter;
                }
                else {
                    andRowExpression = rowExpression;
                }
                final RowExpression combinedExpression = call(metadata.getFunctionAndTypeManager(),
                        OperatorType.EQUAL.getFunctionName().getObjectName(),
                        BOOLEAN,
                        andRowExpression.get(),
                        LogicalRowExpressions.TRUE_CONSTANT);

                globalState = DynamicProcessorState.DONE;
                triggerCallback(combinedExpression);
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }
        else {
            log.debug("RF Processing: Bloom Filter is null");
            globalState = DynamicProcessorState.FAILED;
        }
    }

    private List<RowExpression> extractDynamicFilterArguments()
    {
        ImmutableList.Builder<RowExpression> resultBuilder = ImmutableList.builder();
        for (DynamicFilters.DynamicFilterPlaceholder dynamicFilter : dynamicFilters) {
            resultBuilder.add(dynamicFilter.getInput());
        }
        return resultBuilder.build();
    }

    public void addListener(DynamicFilterCollectorCallback callback)
    {
        listeners.add(callback);
    }

    private void triggerCallback(RowExpression result)
    {
        listeners.forEach(callback -> callback.onSuccess(result));
    }

    public boolean isFailed()
    {
        return globalState == DynamicProcessorState.FAILED;
    }

    public boolean isDone()
    {
        return globalState == DynamicProcessorState.DONE;
    }
}
