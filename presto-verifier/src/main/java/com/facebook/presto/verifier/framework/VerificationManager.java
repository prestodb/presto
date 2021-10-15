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
package com.facebook.presto.verifier.framework;

import com.facebook.airlift.event.client.EventClient;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.sql.parser.ParsingException;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.AliasedRelation;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.CreateTable;
import com.facebook.presto.sql.tree.CreateTableAsSelect;
import com.facebook.presto.sql.tree.CreateView;
import com.facebook.presto.sql.tree.Except;
import com.facebook.presto.sql.tree.Insert;
import com.facebook.presto.sql.tree.Intersect;
import com.facebook.presto.sql.tree.Join;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.Relation;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.sql.tree.Table;
import com.facebook.presto.sql.tree.TableSubquery;
import com.facebook.presto.sql.tree.Union;
import com.facebook.presto.sql.tree.Unnest;
import com.facebook.presto.sql.tree.Values;
import com.facebook.presto.sql.tree.With;
import com.facebook.presto.sql.tree.WithQuery;
import com.facebook.presto.verifier.annotation.ForControl;
import com.facebook.presto.verifier.annotation.ForTest;
import com.facebook.presto.verifier.event.VerifierQueryEvent;
import com.facebook.presto.verifier.event.VerifierQueryEvent.EventStatus;
import com.facebook.presto.verifier.source.SourceQuerySupplier;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import static com.facebook.presto.verifier.event.VerifierQueryEvent.EventStatus.FAILED;
import static com.facebook.presto.verifier.event.VerifierQueryEvent.EventStatus.FAILED_RESOLVED;
import static com.facebook.presto.verifier.event.VerifierQueryEvent.EventStatus.SKIPPED;
import static com.facebook.presto.verifier.event.VerifierQueryEvent.EventStatus.SUCCEEDED;
import static com.facebook.presto.verifier.framework.ClusterType.CONTROL;
import static com.facebook.presto.verifier.framework.ClusterType.TEST;
import static com.facebook.presto.verifier.framework.QueryType.UNSUPPORTED;
import static com.facebook.presto.verifier.framework.SkippedReason.CUSTOM_FILTER;
import static com.facebook.presto.verifier.framework.SkippedReason.MISMATCHED_QUERY_TYPE;
import static com.facebook.presto.verifier.framework.SkippedReason.NON_DETERMINISTIC;
import static com.facebook.presto.verifier.framework.SkippedReason.SYNTAX_ERROR;
import static com.facebook.presto.verifier.framework.SkippedReason.UNSUPPORTED_QUERY_TYPE;
import static com.facebook.presto.verifier.framework.VerifierUtil.PARSING_OPTIONS;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.Thread.currentThread;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newFixedThreadPool;

public class VerificationManager
{
    private static final Logger log = Logger.get(VerificationManager.class);

    private final SourceQuerySupplier sourceQuerySupplier;
    private final VerificationFactory verificationFactory;
    private final SqlParser sqlParser;
    private final Set<EventClient> eventClients;
    private final List<Predicate<SourceQuery>> customQueryFilters;

    private final QueryConfigurationOverrides controlOverrides;
    private final QueryConfigurationOverrides testOverrides;

    private final String testId;

    private final Optional<Set<String>> whitelist;
    private final Optional<Set<String>> blacklist;
    private final int maxConcurrency;
    private final int suiteRepetitions;
    private final int queryRepetitions;
    private final int verificationResubmissionLimit;
    private final boolean explain;
    private final boolean skipControl;

    private final ExecutorService executor;
    private final CompletionService<VerificationResult> completionService;
    private final AtomicInteger queriesSubmitted = new AtomicInteger();

    @Inject
    public VerificationManager(
            SourceQuerySupplier sourceQuerySupplier,
            VerificationFactory verificationFactory,
            SqlParser sqlParser,
            Set<EventClient> eventClients,
            List<Predicate<SourceQuery>> customQueryFilters,
            @ForControl QueryConfigurationOverrides controlOverrides,
            @ForTest QueryConfigurationOverrides testOverrides,
            VerifierConfig config)
    {
        this.sourceQuerySupplier = requireNonNull(sourceQuerySupplier, "sourceQuerySupplier is null");
        this.verificationFactory = requireNonNull(verificationFactory, "verificationFactory is null");
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
        this.eventClients = ImmutableSet.copyOf(eventClients);
        this.customQueryFilters = requireNonNull(customQueryFilters, "customQueryFilters is null");

        this.controlOverrides = requireNonNull(controlOverrides, "controlOverrides is null");
        this.testOverrides = requireNonNull(testOverrides, "testOverride is null");

        this.testId = requireNonNull(config.getTestId(), "testId is null");

        this.whitelist = requireNonNull(config.getWhitelist(), "whitelist is null");
        this.blacklist = requireNonNull(config.getBlacklist(), "blacklist is null");
        this.maxConcurrency = config.getMaxConcurrency();
        this.suiteRepetitions = config.getSuiteRepetitions();
        this.queryRepetitions = config.getQueryRepetitions();
        this.verificationResubmissionLimit = config.getVerificationResubmissionLimit();
        this.skipControl = config.isSkipControl();
        this.explain = config.isExplain();

        this.executor = newFixedThreadPool(maxConcurrency);
        this.completionService = new ExecutorCompletionService<>(executor);
    }

    @PostConstruct
    public void start()
    {
        List<SourceQuery> sourceQueries = sourceQuerySupplier.get();
        log.info("Total Queries: %s", sourceQueries.size());
        sourceQueries = applyOverrides(sourceQueries);
        sourceQueries = applyWhitelist(sourceQueries);
        sourceQueries = applyBlacklist(sourceQueries);
        sourceQueries = filterQueryType(sourceQueries);
        sourceQueries = applyCustomFilters(sourceQueries);

        submit(sourceQueries);
        reportProgressUntilFinished();
    }

    @PreDestroy
    public void close()
    {
        for (EventClient eventClient : eventClients) {
            if (eventClient instanceof Closeable) {
                try {
                    ((Closeable) eventClient).close();
                }
                catch (IOException e) {
                    log.error(e);
                }
            }
        }
        executor.shutdownNow();
    }

    private void resubmit(Verification verification)
    {
        SourceQuery sourceQuery = verification.getSourceQuery();
        VerificationContext newContext = verification.getVerificationContext();
        Verification newVerification = verificationFactory.get(sourceQuery, Optional.of(newContext));
        completionService.submit(newVerification::run);
        queriesSubmitted.addAndGet(1);
        log.info("Verification %s failed, resubmitted for verification (%s/%s)", sourceQuery.getName(), newContext.getResubmissionCount(), verificationResubmissionLimit);
    }

    @VisibleForTesting
    AtomicInteger getQueriesSubmitted()
    {
        return queriesSubmitted;
    }

    private List<SourceQuery> applyOverrides(List<SourceQuery> sourceQueries)
    {
        return sourceQueries.stream()
                .map(sourceQuery -> new SourceQuery(
                        sourceQuery.getSuite(),
                        sourceQuery.getName(),
                        sourceQuery.getQuery(CONTROL),
                        sourceQuery.getQuery(TEST),
                        sourceQuery.getControlConfiguration().applyOverrides(controlOverrides),
                        sourceQuery.getTestConfiguration().applyOverrides(testOverrides)))
                .collect(toImmutableList());
    }

    private List<SourceQuery> applyWhitelist(List<SourceQuery> sourceQueries)
    {
        if (!whitelist.isPresent()) {
            return sourceQueries;
        }
        List<SourceQuery> selected = sourceQueries.stream()
                .filter(sourceQuery -> whitelist.get().contains(sourceQuery.getName()))
                .collect(toImmutableList());
        log.info("Applying whitelist... Remaining queries: %s", selected.size());
        return selected;
    }

    private List<SourceQuery> applyBlacklist(List<SourceQuery> sourceQueries)
    {
        if (!blacklist.isPresent()) {
            return sourceQueries;
        }
        List<SourceQuery> selected = sourceQueries.stream()
                .filter(sourceQuery -> !blacklist.get().contains(sourceQuery.getName()))
                .collect(toImmutableList());
        log.info("Applying blacklist... Remaining queries: %s", selected.size());
        return selected;
    }

    private List<SourceQuery> filterQueryType(List<SourceQuery> sourceQueries)
    {
        if (explain) {
            return sourceQueries;
        }

        ImmutableList.Builder<SourceQuery> selected = ImmutableList.builder();
        for (SourceQuery sourceQuery : sourceQueries) {
            try {
                Statement controlStatement = sqlParser.createStatement(sourceQuery.getQuery(CONTROL), PARSING_OPTIONS);
                QueryType controlQueryType = QueryType.of(controlStatement);
                QueryType testQueryType = QueryType.of(sqlParser.createStatement(sourceQuery.getQuery(TEST), PARSING_OPTIONS));

                if (controlQueryType == UNSUPPORTED || testQueryType == UNSUPPORTED) {
                    postEvent(VerifierQueryEvent.skipped(sourceQuery.getSuite(), testId, sourceQuery, UNSUPPORTED_QUERY_TYPE, skipControl));
                }
                else if (controlQueryType != testQueryType) {
                    postEvent(VerifierQueryEvent.skipped(sourceQuery.getSuite(), testId, sourceQuery, MISMATCHED_QUERY_TYPE, skipControl));
                }
                else if (isLimitWithoutOrderBy(controlStatement, sourceQuery.getName())) {
                    log.debug("LimitWithoutOrderByChecker Skipped %s", sourceQuery.getName());
                    postEvent(VerifierQueryEvent.skipped(sourceQuery.getSuite(), testId, sourceQuery, NON_DETERMINISTIC, skipControl));
                }
                else {
                    selected.add(sourceQuery);
                }
            }
            catch (ParsingException e) {
                log.warn("Failed to parse query: %s", sourceQuery.getName());
                postEvent(VerifierQueryEvent.skipped(sourceQuery.getSuite(), testId, sourceQuery, SYNTAX_ERROR, skipControl));
            }
        }
        List<SourceQuery> selectQueries = selected.build();
        log.info("Filtering query type... Remaining queries: %s", selectQueries.size());
        return selectQueries;
    }

    private Boolean isLimitWithoutOrderBy(Statement controlStatement, String queryId)
    {
        try {
            Boolean process = new LimitWithoutOrderByChecker().process(controlStatement, 0);
            //We want to continue processing without an NPE
            if (process == null) {
                log.warn("LimitWithoutOrderByChecker Check failed for %s ", queryId);
                return false;
            }
            return process;
        }
        catch (Exception e) {
            //We want to continue processing for the rest of the queries
            log.warn("LimitWithoutOrderByChecker Check failed for %s ", queryId);
            log.error(e);
            return false;
        }
    }

    private List<SourceQuery> applyCustomFilters(List<SourceQuery> sourceQueries)
    {
        if (customQueryFilters.isEmpty()) {
            return sourceQueries;
        }

        log.info("Applying custom query filters");
        sourceQueries = new ArrayList<>(sourceQueries);
        for (Predicate<SourceQuery> filter : customQueryFilters) {
            Iterator<SourceQuery> iterator = sourceQueries.iterator();
            while (iterator.hasNext()) {
                SourceQuery sourceQuery = iterator.next();
                if (!filter.test(sourceQuery)) {
                    iterator.remove();
                    postEvent(VerifierQueryEvent.skipped(sourceQuery.getSuite(), testId, sourceQuery, CUSTOM_FILTER, skipControl));
                }
            }
            log.info("Applying custom filter %s... Remaining queries: %s", filter.getClass().getSimpleName(), sourceQueries.size());
        }
        return sourceQueries;
    }

    private void submit(List<SourceQuery> sourceQueries)
    {
        for (int i = 0; i < suiteRepetitions; i++) {
            for (SourceQuery sourceQuery : sourceQueries) {
                for (int j = 0; j < queryRepetitions; j++) {
                    Verification verification = verificationFactory.get(sourceQuery, Optional.empty());
                    completionService.submit(verification::run);
                }
            }
        }
        int queriesSubmitted = sourceQueries.size() * suiteRepetitions * queryRepetitions;
        log.info("Queries submitted: %s", queriesSubmitted);
        this.queriesSubmitted.addAndGet(queriesSubmitted);
    }

    private void reportProgressUntilFinished()
    {
        int completed = 0;
        double lastProgress = 0;
        Map<EventStatus, Integer> statusCount = new EnumMap<>(EventStatus.class);

        while (completed < queriesSubmitted.get()) {
            try {
                VerificationResult result = completionService.take().get();
                Optional<VerifierQueryEvent> event = result.getEvent();
                completed++;
                if (!event.isPresent()) {
                    statusCount.compute(SKIPPED, (status, count) -> count == null ? 1 : count + 1);
                }
                else {
                    statusCount.compute(EventStatus.valueOf(event.get().getStatus()), (status, count) -> count == null ? 1 : count + 1);
                    postEvent(event.get());
                }

                if (result.shouldResubmit()) {
                    resubmit(result.getVerification());
                }

                double progress = ((double) completed) / queriesSubmitted.get() * 100;
                if (progress - lastProgress > 0.5 || completed == queriesSubmitted.get()) {
                    log.info(
                            "Progress: %s succeeded, %s skipped, %s resolved, %s failed, %.2f%% done",
                            statusCount.getOrDefault(SUCCEEDED, 0),
                            statusCount.getOrDefault(SKIPPED, 0),
                            statusCount.getOrDefault(FAILED_RESOLVED, 0),
                            statusCount.getOrDefault(FAILED, 0),
                            progress);
                    lastProgress = progress;
                }
            }
            catch (InterruptedException e) {
                currentThread().interrupt();
                throw new RuntimeException(e);
            }
            catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void postEvent(VerifierQueryEvent event)
    {
        for (EventClient eventClient : eventClients) {
            eventClient.post(event);
        }
    }

    private class LimitWithoutOrderByChecker
            extends AstVisitor<Boolean, Integer>
    {
        @Override
        protected Boolean visitQuerySpecification(QuerySpecification node, Integer level)
        {
            if (node.getFrom().isPresent()) {
                Relation relation = node.getFrom().get();
                if (process(relation, level)) {
                    return true;
                }
            }
            //Check if node is not at root level and limit clause is present without order by
            return level != 0 && node.getLimit().isPresent() && !node.getOrderBy().isPresent();
        }

        @Override
        protected Boolean visitQuery(Query node, Integer level)
        {
            if (node.getWith().isPresent()) {
                With with = node.getWith().get();
                for (WithQuery query : with.getQueries()) {
                    if (process(query.getQuery(), level + 1)) {
                        return true;
                    }
                }
            }

            return process(node.getQueryBody(), level);
        }

        @Override
        protected Boolean visitInsert(Insert node, Integer level)
        {
            return process(node.getQuery(), level);
        }

        @Override
        protected Boolean visitJoin(Join node, Integer level)
        {
            return false;
        }

        @Override
        protected Boolean visitUnion(Union node, Integer level)
        {
            for (Relation relation : node.getRelations()) {
                if (process(relation, level)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        protected Boolean visitIntersect(Intersect node, Integer level)
        {
            for (Relation relation : node.getRelations()) {
                if (process(relation, level)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        protected Boolean visitExcept(Except node, Integer level)
        {
            return process(node.getLeft(), level) || process(node.getRight(), level);
        }

        @Override
        protected Boolean visitAliasedRelation(AliasedRelation node, Integer level)
        {
            return process(node.getRelation(), level);
        }

        @Override
        protected Boolean visitUnnest(Unnest node, Integer level)
        {
            return false;
        }

        @Override
        protected Boolean visitTable(Table node, Integer level)
        {
            return false;
        }

        @Override
        protected Boolean visitTableSubquery(TableSubquery node, Integer level)
        {
            //Incrementing level when we reach a sub-query
            return process(node.getQuery(), level + 1);
        }

        @Override
        protected Boolean visitCreateTable(CreateTable node, Integer level)
        {
            return false;
        }

        @Override
        protected Boolean visitCreateTableAsSelect(CreateTableAsSelect node, Integer level)
        {
            return process(node.getQuery(), level);
        }

        @Override
        protected Boolean visitCreateView(CreateView node, Integer level)
        {
            return process(node.getQuery(), level);
        }

        @Override
        protected Boolean visitValues(Values node, Integer level)
        {
            return false;
        }
    }
}
