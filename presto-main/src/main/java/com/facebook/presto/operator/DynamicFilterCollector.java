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

import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.sql.planner.DomainTranslator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.relational.RowExpression;
import com.facebook.presto.sql.tree.DynamicFilterExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.SymbolReference;
import com.facebook.presto.util.RowExpressionConverter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.http.client.FullJsonResponseHandler;
import io.airlift.log.Logger;

import javax.ws.rs.core.Response;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.Signatures.logicalExpressionSignature;
import static java.util.Objects.requireNonNull;

public class DynamicFilterCollector
{
    public static final int MAX_RETRIES = 5;
    private static final Logger log = Logger.get(DynamicFilterCollector.class);
    public static final int DF_TIMEOUT_MILLIS = 100;
    private final ConcurrentHashMap<DynamicFilterExpression, DynamicProcessorState> dfState = new ConcurrentHashMap<>();
    private final AtomicReference<TupleDomain> dfTupleDomain = new AtomicReference<>(TupleDomain.all());
    private final Optional<RowExpression> staticFilter;
    private final RowExpressionConverter converter;
    private final Set<DynamicFilterExpression> dynamicFilters;
    private final DynamicFilterClient client;
    private final QueryId queryId;
    private Optional<RowExpression> resultExpression = Optional.empty();
    private List<DFCollectorCallback> listeners = new ArrayList();
    enum DynamicProcessorState {
        NOTFOUND,
        DONE,
        FAILED
    }

    protected DynamicProcessorState globalState = DynamicProcessorState.NOTFOUND;
    private ExecutorService es = Executors.newSingleThreadExecutor(new ThreadFactory() {
        public Thread newThread(Runnable r)
        {
            Thread thread = new Thread(r);
            return thread;
        }
    });

    public DynamicFilterCollector(Set<DynamicFilterExpression> dynamicFilters,
            Optional<RowExpression> staticFilter,
            DynamicFilterClient client,
            RowExpressionConverter converter,
            QueryId queryId)
    {
        this.dynamicFilters = requireNonNull(dynamicFilters, "dynamicFilters is null");
        this.staticFilter = requireNonNull(staticFilter, "staticFilter is null");
        this.client = requireNonNull(client, "client is null");
        this.converter = requireNonNull(converter, "converter is null");
        this.queryId = requireNonNull(queryId, "queryId is null");
        dynamicFilters.stream().forEach(d -> dfState.put(d, DynamicProcessorState.NOTFOUND));
    }

    private void fireDFCollection(final DynamicFilterExpression df)
    {
        try {
            final ListenableFuture result = client.getSummary(queryId.toString(), df.getSourceId());
            final Object response = result.get(DF_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
            final FullJsonResponseHandler.JsonResponse<DynamicFilterSummary> jsonResponse = (FullJsonResponseHandler.JsonResponse<DynamicFilterSummary>) response;
            if (jsonResponse.getStatusCode() == Response.Status.OK.getStatusCode()) {
                final DynamicFilterSummary summary = jsonResponse.getValue();
                final TupleDomain<Symbol> tupleDomain = translateSummaryIntoTupleDomain(summary, ImmutableSet.of(df));
                dfTupleDomain.accumulateAndGet(tupleDomain, TupleDomain::intersect);
                dfState.put(df, DynamicProcessorState.DONE);
            }
            else if (jsonResponse.getStatusCode() == Response.Status.NOT_FOUND.getStatusCode()) {
                dfState.put(df, DynamicProcessorState.NOTFOUND);
            }
            else {
                dfState.put(df, DynamicProcessorState.FAILED);
            }
        }
        catch (Exception exp) {
            dfState.put(df, DynamicProcessorState.FAILED);
        }
    }

    public void collectDynamicSummaryAsync()
    {
        CompletableFuture.runAsync(this::collectDynamicSummary, es).whenComplete((value, exception) ->
        {
            this.cleanUp();
        });
    }

    private void cleanUp()
    {
        es.shutdown();
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
        }
        if (retry == MAX_RETRIES) {
            afterDFcollection();
        }
    }

    private void collectDynamicSummaryInternal()
    {
        boolean inProgress = false;
        for (ConcurrentHashMap.Entry<DynamicFilterExpression, DynamicProcessorState> e : dfState.entrySet()) {
            DynamicProcessorState state = e.getValue();
            if (state != DynamicProcessorState.DONE && state != DynamicProcessorState.FAILED) {
                inProgress = true;
                fireDFCollection(e.getKey());
            }
        }
        if (!inProgress) {
            afterDFcollection();
        }
    }

    private void afterDFcollection()
    {
        if (globalState == DynamicProcessorState.DONE || globalState == DynamicProcessorState.FAILED) {
            return;
        }
        TupleDomain<Symbol> tupleDomain = dfTupleDomain.get();
        if (!tupleDomain.isAll()) {
            try {
                final Expression expr = DomainTranslator.toPredicate(tupleDomain);
                final Optional<RowExpression> rowExpression = converter.expressionToRowExpression(Optional.of(expr));
                final RowExpression combinedExpression = call(logicalExpressionSignature(LogicalBinaryExpression.Type.AND),
                        BOOLEAN,
                        rowExpression.get(),
                        staticFilter.get());
                resultExpression = Optional.of(combinedExpression);
                globalState = DynamicProcessorState.DONE;
                triggerCallback(combinedExpression);
            }
            catch (Exception e) {
                globalState = DynamicProcessorState.FAILED;
                log.warn(e, "Could not convert Tuple Domain from DynamicFilter to Expression");
                throw e;
            }
        }
        else {
            log.debug("DF Processing: Tuple Domain is all");
            globalState = DynamicProcessorState.FAILED;
        }
    }

    public Optional<RowExpression> getFinalExpression()
    {
        if (globalState != DynamicProcessorState.DONE) {
            return Optional.empty();
        }
        else {
            return resultExpression;
        }
    }

    private TupleDomain translateSummaryIntoTupleDomain(DynamicFilterSummary summary, Set<DynamicFilterExpression> dynamicFilters)
    {
        if (!summary.getTupleDomain().getDomains().isPresent()) {
            return TupleDomain.all();
        }

        Map<String, Symbol> sourceExpressionSymbols = extractSourceExpressionSymbols(dynamicFilters);
        ImmutableMap.Builder<Symbol, Domain> domainBuilder = ImmutableMap.builder();
        for (Map.Entry<String, Domain> entry : summary.getTupleDomain().getDomains().get().entrySet()) {
            Symbol actualSymbol = sourceExpressionSymbols.get(entry.getKey());
            if (actualSymbol == null) {
                continue;
            }
            domainBuilder.put(actualSymbol, entry.getValue());
        }

        final ImmutableMap<Symbol, Domain> domainMap = domainBuilder.build();
        return domainMap.isEmpty() ? TupleDomain.all() : TupleDomain.withColumnDomains(domainMap);
    }

    private Map<String, Symbol> extractSourceExpressionSymbols(Set<DynamicFilterExpression> dynamicFilters)
    {
        ImmutableMap.Builder<String, Symbol> resultBuilder = ImmutableMap.builder();
        for (DynamicFilterExpression dynamicFilter : dynamicFilters) {
            Expression expression = dynamicFilter.getProbeExpression();
            if (!(expression instanceof SymbolReference)) {
                continue;
            }
            resultBuilder.put(dynamicFilter.getDfSymbol(), Symbol.from(expression));
        }
        return resultBuilder.build();
    }

    public void addListener(DFCollectorCallback callback)
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

    public interface DFCollectorCallback
    {
        void onSuccess(RowExpression result);
    }
}
