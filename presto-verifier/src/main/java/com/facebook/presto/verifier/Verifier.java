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
package com.facebook.presto.verifier;

import com.facebook.presto.spi.ErrorCode;
import com.facebook.presto.spi.PrestoException;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import io.airlift.event.client.EventClient;
import io.airlift.log.Logger;
import io.airlift.units.Duration;

import java.io.Closeable;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.regex.Pattern;

import static com.facebook.presto.spi.StandardErrorCode.PAGE_TRANSPORT_TIMEOUT;
import static com.facebook.presto.spi.StandardErrorCode.REMOTE_TASK_MISMATCH;
import static com.facebook.presto.spi.StandardErrorCode.TOO_MANY_REQUESTS_FAILED;
import static com.facebook.presto.verifier.QueryResult.State.SUCCESS;
import static com.google.common.base.Throwables.getStackTraceAsString;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;

public class Verifier
{
    private static final Logger log = Logger.get(Verifier.class);

    private static final Set<ErrorCode> EXPECTED_ERRORS = ImmutableSet.<ErrorCode>builder()
            .add(REMOTE_TASK_MISMATCH.toErrorCode())
            .add(TOO_MANY_REQUESTS_FAILED.toErrorCode())
            .add(PAGE_TRANSPORT_TIMEOUT.toErrorCode())
            .build();

    private final VerifierConfig config;
    private final Set<EventClient> eventClients;
    private final int threadCount;
    private final Set<String> whitelist;
    private final Set<String> blacklist;
    private final int precision;

    public Verifier(PrintStream out, VerifierConfig config, Set<EventClient> eventClients)
    {
        requireNonNull(out, "out is null");
        this.config = requireNonNull(config, "config is null");
        this.eventClients = requireNonNull(eventClients, "eventClients is null");
        this.whitelist = requireNonNull(config.getWhitelist(), "whitelist is null");
        this.blacklist = requireNonNull(config.getBlacklist(), "blacklist is null");
        this.threadCount = config.getThreadCount();
        this.precision = config.getDoublePrecision();
    }

    // Returns number of failed queries
    public int run(List<QueryPair> queries)
            throws InterruptedException
    {
        ExecutorService executor = newFixedThreadPool(threadCount);
        CompletionService<Validator> completionService = new ExecutorCompletionService<>(executor);

        int totalQueries = queries.size() * config.getSuiteRepetitions();
        log.info("Total Queries:     %d", totalQueries);

        log.info("Whitelisted Queries: %s", Joiner.on(',').join(whitelist));

        int queriesSubmitted = 0;
        for (int i = 0; i < config.getSuiteRepetitions(); i++) {
            for (QueryPair query : queries) {
                for (int j = 0; j < config.getQueryRepetitions(); j++) {
                    // If a whitelist exists, only run the tests on the whitelist
                    if (!whitelist.isEmpty() && !whitelist.contains(query.getName())) {
                        log.debug("Query %s is not whitelisted", query.getName());
                        continue;
                    }
                    if (blacklist.contains(query.getName())) {
                        log.debug("Query %s is blacklisted", query.getName());
                        continue;
                    }
                    Validator validator = new Validator(
                            config.getControlGateway(),
                            config.getTestGateway(),
                            config.getControlTimeout(),
                            config.getTestTimeout(),
                            config.getMaxRowCount(),
                            config.isExplainOnly(),
                            config.getDoublePrecision(),
                            isCheckCorrectness(query),
                            true,
                            config.isVerboseResultsComparison(),
                            config.getControlTeardownRetries(),
                            config.getTestTeardownRetries(),
                            query);
                    completionService.submit(validator::valid, validator);
                    queriesSubmitted++;
                }
            }
        }

        log.info("Allowed Queries:     %d", queriesSubmitted);
        log.info("Skipped Queries:     %d", (totalQueries - queriesSubmitted));
        log.info("---------------------");

        executor.shutdown();

        int total = 0;
        int valid = 0;
        int failed = 0;
        int skipped = 0;
        double lastProgress = 0;

        while (total < queriesSubmitted) {
            total++;

            Validator validator = takeUnchecked(completionService);

            if (validator.isSkipped()) {
                if (!config.isQuiet()) {
                    log.warn("%s", validator.getSkippedMessage());
                }

                skipped++;
                continue;
            }

            if (validator.valid()) {
                valid++;
            }
            else {
                failed++;
            }

            for (EventClient eventClient : eventClients) {
                eventClient.post(buildEvent(validator));
            }

            double progress = (((double) total) / totalQueries) * 100;
            if (!config.isQuiet() || (progress - lastProgress) > 1) {
                log.info("Progress: %s valid, %s failed, %s skipped, %.2f%% done", valid, failed, skipped, progress);
                lastProgress = progress;
            }
        }

        log.info("Results: %s / %s (%s skipped)", valid, failed, skipped);
        log.info("");

        for (EventClient eventClient : eventClients) {
            if (eventClient instanceof Closeable) {
                try {
                    ((Closeable) eventClient).close();
                }
                catch (IOException ignored) { }
                log.info("");
            }
        }

        return failed;
    }

    private boolean isCheckCorrectness(QueryPair query)
    {
        // Check if either the control query or the test query matches the regex
        if (Pattern.matches(config.getSkipCorrectnessRegex(), query.getTest().getQuery()) ||
                Pattern.matches(config.getSkipCorrectnessRegex(), query.getControl().getQuery())) {
            // If so disable correctness checking
            return false;
        }
        else {
            return config.isCheckCorrectnessEnabled();
        }
    }

    private VerifierQueryEvent buildEvent(Validator validator)
    {
        String errorMessage = null;
        QueryPair queryPair = validator.getQueryPair();
        QueryResult control = validator.getControlResult();
        QueryResult test = validator.getTestResult();

        if (!validator.valid()) {
            errorMessage = format("Test state %s, Control state %s\n", test.getState(), control.getState());
            Exception e = test.getException();
            if (e != null && shouldAddStackTrace(e)) {
                errorMessage += getStackTraceAsString(e);
            }
            if (control.getState() == SUCCESS && test.getState() == SUCCESS) {
                errorMessage += validator.getResultsComparison(precision).trim();
            }
        }

        return new VerifierQueryEvent(
                queryPair.getSuite(),
                config.getRunId(),
                config.getSource(),
                queryPair.getName(),
                !validator.valid(),
                queryPair.getTest().getCatalog(),
                queryPair.getTest().getSchema(),
                queryPair.getTest().getPreQueries(),
                queryPair.getTest().getQuery(),
                queryPair.getTest().getPostQueries(),
                test.getQueryId(),
                optionalDurationToSeconds(test.getCpuTime()),
                optionalDurationToSeconds(test.getWallTime()),
                queryPair.getControl().getCatalog(),
                queryPair.getControl().getSchema(),
                queryPair.getControl().getPreQueries(),
                queryPair.getControl().getQuery(),
                queryPair.getControl().getPostQueries(),
                control.getQueryId(),
                optionalDurationToSeconds(control.getCpuTime()),
                optionalDurationToSeconds(control.getWallTime()),
                errorMessage);
    }

    private static Double optionalDurationToSeconds(Duration duration)
    {
        return duration != null ? duration.convertTo(SECONDS).getValue() : null;
    }

    private static <T> T takeUnchecked(CompletionService<T> completionService)
            throws InterruptedException
    {
        try {
            return completionService.take().get();
        }
        catch (ExecutionException e) {
            throw Throwables.propagate(e);
        }
    }

    private static boolean shouldAddStackTrace(Exception e)
    {
        if (e instanceof PrestoException) {
            ErrorCode errorCode = ((PrestoException) e).getErrorCode();
            if (EXPECTED_ERRORS.contains(errorCode)) {
                return false;
            }
        }
        return true;
    }
}
