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

import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import io.airlift.event.client.EventClient;
import io.airlift.log.Logger;

import java.io.PrintStream;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Throwables.getStackTraceAsString;
import static java.lang.String.format;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;

public class Verifier
{
    private static final Logger log = Logger.get(Verifier.class);

    private final VerifierConfig config;
    private final EventClient eventClient;
    private final int threadCount;
    private final Set<String> whitelist;
    private final Set<String> blacklist;

    public Verifier(PrintStream out, VerifierConfig config, EventClient eventClient)
    {
        checkNotNull(out, "out is null");
        this.config = checkNotNull(config, "config is null");
        this.eventClient = checkNotNull(eventClient, "eventClient is null");
        this.whitelist = checkNotNull(config.getWhitelist(), "whitelist is null");
        this.blacklist = checkNotNull(config.getBlacklist(), "blacklist is null");
        this.threadCount = config.getThreadCount();
    }

    public void run(List<QueryPair> queries)
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
                    Validator validator = new Validator(config, query);
                    completionService.submit(validateTask(validator), validator);
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

            eventClient.post(buildEvent(validator));

            double progress = (((double) total) / totalQueries) * 100;
            if (!config.isQuiet() || (progress - lastProgress) > 1) {
                log.info("Progress: %s valid, %s failed, %s skipped, %.2f%% done", valid, failed, skipped, progress);
                lastProgress = progress;
            }
        }

        log.info("Results: %s / %s (%s skipped)", valid, failed, skipped);
    }

    private VerifierQueryEvent buildEvent(Validator validator)
    {
        String errorMessage = null;
        QueryPair queryPair = validator.getQueryPair();
        QueryResult control = validator.getControlResult();
        QueryResult test = validator.getTestResult();

        if (!validator.valid()) {
            errorMessage = format("Test state %s, Control state %s\n", test.getState(), control.getState());
            if (test.getException() != null) {
                errorMessage += getStackTraceAsString(test.getException());
            }
            else {
                errorMessage += validator.getResultsComparison().trim();
            }
        }

        // TODO implement cpu time tracking
        return new VerifierQueryEvent(
                config.getSuite(),
                config.getRunId(),
                config.getSource(),
                queryPair.getName(),
                !validator.valid(),
                queryPair.getTest().getCatalog(),
                queryPair.getTest().getSchema(),
                queryPair.getTest().getQuery(),
                null,
                optionalDurationToSeconds(test),
                queryPair.getControl().getCatalog(),
                queryPair.getControl().getSchema(),
                queryPair.getControl().getQuery(),
                null,
                optionalDurationToSeconds(control),
                errorMessage);
    }

    private static Double optionalDurationToSeconds(QueryResult test)
    {
        return test.getDuration() != null ? test.getDuration().convertTo(SECONDS).getValue() : null;
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

    private static Runnable validateTask(final Validator validator)
    {
        return new Runnable()
        {
            @Override
            public void run()
            {
                validator.valid();
            }
        };
    }
}
