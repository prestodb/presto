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

import com.facebook.presto.sql.parser.ParsingException;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.verifier.event.VerifierQueryEvent;
import com.facebook.presto.verifier.event.VerifierQueryEvent.EventStatus;
import com.facebook.presto.verifier.source.SourceQuerySupplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.event.client.EventClient;
import io.airlift.log.Logger;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.io.Closeable;
import java.io.IOException;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.function.Predicate;

import static com.facebook.presto.verifier.event.VerifierQueryEvent.EventStatus.FAILED;
import static com.facebook.presto.verifier.event.VerifierQueryEvent.EventStatus.FAILED_RESOLVED;
import static com.facebook.presto.verifier.event.VerifierQueryEvent.EventStatus.SKIPPED;
import static com.facebook.presto.verifier.event.VerifierQueryEvent.EventStatus.SUCCEEDED;
import static com.facebook.presto.verifier.framework.JdbcDriverUtil.initializeDrivers;
import static com.facebook.presto.verifier.framework.QueryType.Category.DATA_PRODUCING;
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

    private final Optional<String> additionalJdbcDriverPath;
    private final Optional<String> controlJdbcDriverClass;
    private final Optional<String> testJdbcDriverClass;

    private final Optional<String> controlCatalogOverride;
    private final Optional<String> controlSchemaOverride;
    private final Optional<String> controlUsernameOverride;
    private final Optional<String> controlPasswordOverride;
    private final Optional<String> testCatalogOverride;
    private final Optional<String> testSchemaOverride;
    private final Optional<String> testUsernameOverride;
    private final Optional<String> testPasswordOverride;

    private final Optional<Set<String>> whitelist;
    private final Optional<Set<String>> blacklist;
    private final int maxConcurrency;
    private final int suiteRepetitions;
    private final int queryRepetitions;

    @Inject
    public VerificationManager(
            SourceQuerySupplier sourceQuerySupplier,
            VerificationFactory verificationFactory,
            SqlParser sqlParser,
            Set<EventClient> eventClients,
            List<Predicate<SourceQuery>> customQueryFilters,
            VerifierConfig config)
    {
        this.sourceQuerySupplier = requireNonNull(sourceQuerySupplier, "sourceQuerySupplier is null");
        this.verificationFactory = requireNonNull(verificationFactory, "verificationFactory is null");
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
        this.eventClients = ImmutableSet.copyOf(eventClients);
        this.customQueryFilters = requireNonNull(customQueryFilters, "customQueryFilters is null");

        this.additionalJdbcDriverPath = requireNonNull(config.getAdditionalJdbcDriverPath(), "additionalJdbcDriverPath is null");
        this.controlJdbcDriverClass = requireNonNull(config.getControlJdbcDriverClass(), "controlJdbcDriverClass is null");
        this.testJdbcDriverClass = requireNonNull(config.getTestJdbcDriverClass(), "testJdbcDriverClass is null");

        this.controlCatalogOverride = requireNonNull(config.getControlCatalogOverride(), "controlCatalogOverride is null");
        this.controlSchemaOverride = requireNonNull(config.getControlSchemaOverride(), "controlSchemaOverride is null");
        this.controlUsernameOverride = requireNonNull(config.getControlUsernameOverride(), "controlUsernameOverride is null");
        this.controlPasswordOverride = requireNonNull(config.getControlPasswordOverride(), "controlPasswordOverride is null");
        this.testCatalogOverride = requireNonNull(config.getTestCatalogOverride(), "testCatalogOverride is null");
        this.testSchemaOverride = requireNonNull(config.getTestSchemaOverride(), "testSchemaOverride is null");
        this.testUsernameOverride = requireNonNull(config.getTestUsernameOverride(), "testUsernameOverride is null");
        this.testPasswordOverride = requireNonNull(config.getTestPasswordOverride(), "testPasswordOverride is null");

        this.whitelist = requireNonNull(config.getWhitelist(), "whitelist is null");
        this.blacklist = requireNonNull(config.getBlacklist(), "blacklist is null");
        this.maxConcurrency = config.getMaxConcurrency();
        this.suiteRepetitions = config.getSuiteRepetitions();
        this.queryRepetitions = config.getQueryRepetitions();
    }

    public void start()
    {
        initializeDrivers(additionalJdbcDriverPath, controlJdbcDriverClass, testJdbcDriverClass);

        List<SourceQuery> sourceQueries = sourceQuerySupplier.get();
        log.info("Total Queries: %s", sourceQueries.size());
        sourceQueries = applyOverrides(sourceQueries);
        sourceQueries = applyWhitelist(sourceQueries);
        sourceQueries = applyBlacklist(sourceQueries);
        sourceQueries = filterQueryType(sourceQueries);
        sourceQueries = applyCustomFilters(sourceQueries);

        CompletionService<Optional<VerifierQueryEvent>> completionService = submitSourceQueriesForVerification(sourceQueries);
        int queriesSubmitted = sourceQueries.size() * suiteRepetitions * queryRepetitions;
        log.info("Queries submitted: %s", queriesSubmitted);

        reportProgressUntilFinished(completionService, queriesSubmitted);
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
    }

    private List<SourceQuery> applyOverrides(List<SourceQuery> sourceQueries)
    {
        return sourceQueries.stream()
                .map(sourceQuery -> new SourceQuery(
                        sourceQuery.getSuite(),
                        sourceQuery.getName(),
                        sourceQuery.getControlQuery(),
                        sourceQuery.getTestQuery(),
                        new QueryConfiguration(
                                testCatalogOverride.orElse(sourceQuery.getTestConfiguration().getCatalog()),
                                testSchemaOverride.orElse(sourceQuery.getTestConfiguration().getSchema()),
                                testUsernameOverride.orElse(sourceQuery.getTestConfiguration().getUsername()),
                                Optional.ofNullable(testPasswordOverride.orElse(sourceQuery.getTestConfiguration().getPassword().orElse(null))),
                                sourceQuery.getTestConfiguration().getSessionProperties()),
                        new QueryConfiguration(
                                controlCatalogOverride.orElse(sourceQuery.getControlConfiguration().getCatalog()),
                                controlSchemaOverride.orElse(sourceQuery.getControlConfiguration().getSchema()),
                                controlUsernameOverride.orElse(sourceQuery.getControlConfiguration().getUsername()),
                                Optional.ofNullable(controlPasswordOverride.orElse(sourceQuery.getControlConfiguration().getPassword().orElse(null))),
                                sourceQuery.getControlConfiguration().getSessionProperties())))
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
        ImmutableList.Builder<SourceQuery> selected = ImmutableList.builder();
        for (SourceQuery sourceQuery : sourceQueries) {
            try {
                QueryType controlQueryType = QueryType.of(sqlParser.createStatement(sourceQuery.getControlQuery(), PARSING_OPTIONS));
                QueryType testQueryType = QueryType.of(sqlParser.createStatement(sourceQuery.getTestQuery(), PARSING_OPTIONS));
                if (controlQueryType == testQueryType && controlQueryType.getCategory() == DATA_PRODUCING) {
                    selected.add(sourceQuery);
                }
            }
            catch (ParsingException e) {
                log.warn("Failed to parse query: %s", sourceQuery.getName());
            }
            catch (UnsupportedQueryTypeException ignored) {
            }
        }
        List<SourceQuery> selectQueries = selected.build();
        log.info("Filtering query type... Remaining queries: %s", selectQueries.size());
        return selectQueries;
    }

    private List<SourceQuery> applyCustomFilters(List<SourceQuery> sourceQueries)
    {
        if (customQueryFilters.isEmpty()) {
            return sourceQueries;
        }

        log.info("Applying custom query filters");
        for (Predicate<SourceQuery> filter : customQueryFilters) {
            sourceQueries = sourceQueries.stream()
                    .filter(filter::test)
                    .collect(toImmutableList());
            log.info("Applying custom filter %s... Remaining queries: %s", filter.getClass().getSimpleName(), sourceQueries.size());
        }
        return sourceQueries;
    }

    private CompletionService<Optional<VerifierQueryEvent>> submitSourceQueriesForVerification(List<SourceQuery> sourceQueries)
    {
        ExecutorService executor = newFixedThreadPool(maxConcurrency);
        CompletionService<Optional<VerifierQueryEvent>> completionService = new ExecutorCompletionService<>(executor);
        for (int i = 0; i < suiteRepetitions; i++) {
            for (SourceQuery sourceQuery : sourceQueries) {
                for (int j = 0; j < queryRepetitions; j++) {
                    Verification verification = verificationFactory.get(sourceQuery);
                    completionService.submit(verification::run);
                }
            }
        }
        executor.shutdown();
        return completionService;
    }

    private void reportProgressUntilFinished(CompletionService<Optional<VerifierQueryEvent>> completionService, int queriesSubmitted)
    {
        int completed = 0;
        double lastProgress = 0;
        Map<EventStatus, Integer> statusCount = new EnumMap<>(EventStatus.class);

        while (completed < queriesSubmitted) {
            try {
                Optional<VerifierQueryEvent> event = completionService.take().get();
                completed++;
                if (!event.isPresent()) {
                    statusCount.compute(SKIPPED, (status, count) -> count == null ? 1 : count + 1);
                }
                else {
                    statusCount.compute(EventStatus.valueOf(event.get().getStatus()), (status, count) -> count == null ? 1 : count + 1);
                    for (EventClient eventClient : eventClients) {
                        eventClient.post(event.get());
                    }
                }

                double progress = ((double) completed) / queriesSubmitted * 100;
                if (progress - lastProgress > 0.5 || completed == queriesSubmitted) {
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
}
