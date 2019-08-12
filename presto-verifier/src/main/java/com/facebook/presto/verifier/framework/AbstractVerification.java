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

import com.facebook.presto.jdbc.QueryStats;
import com.facebook.presto.sql.SqlFormatter;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.verifier.checksum.ChecksumResult;
import com.facebook.presto.verifier.event.QueryInfo;
import com.facebook.presto.verifier.event.VerifierQueryEvent;
import com.facebook.presto.verifier.event.VerifierQueryEvent.EventStatus;
import com.facebook.presto.verifier.framework.MatchResult.MatchType;
import com.facebook.presto.verifier.resolver.FailureResolver;
import io.airlift.log.Logger;
import io.airlift.units.Duration;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.spi.StandardErrorCode.EXCEEDED_TIME_LIMIT;
import static com.facebook.presto.verifier.event.VerifierQueryEvent.EventStatus.FAILED;
import static com.facebook.presto.verifier.event.VerifierQueryEvent.EventStatus.FAILED_RESOLVED;
import static com.facebook.presto.verifier.event.VerifierQueryEvent.EventStatus.SKIPPED;
import static com.facebook.presto.verifier.event.VerifierQueryEvent.EventStatus.SUCCEEDED;
import static com.facebook.presto.verifier.framework.ClusterType.CONTROL;
import static com.facebook.presto.verifier.framework.ClusterType.TEST;
import static com.facebook.presto.verifier.framework.QueryStage.CONTROL_MAIN;
import static com.facebook.presto.verifier.framework.QueryStage.DETERMINISM_ANALYSIS;
import static com.facebook.presto.verifier.framework.QueryStage.TEST_MAIN;
import static com.facebook.presto.verifier.framework.QueryStage.forMain;
import static com.facebook.presto.verifier.framework.QueryStage.forSetup;
import static com.facebook.presto.verifier.framework.QueryStage.forTeardown;
import static com.facebook.presto.verifier.framework.SkippedReason.CONTROL_QUERY_FAILED;
import static com.facebook.presto.verifier.framework.SkippedReason.CONTROL_QUERY_TIMED_OUT;
import static com.facebook.presto.verifier.framework.SkippedReason.CONTROL_SETUP_QUERY_FAILED;
import static com.facebook.presto.verifier.framework.SkippedReason.FAILED_BEFORE_CONTROL_QUERY;
import static com.facebook.presto.verifier.framework.SkippedReason.NON_DETERMINISTIC;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.getStackTraceAsString;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public abstract class AbstractVerification
        implements Verification
{
    private static final Logger log = Logger.get(AbstractVerification.class);

    private final VerificationResubmitter verificationResubmitter;
    private final PrestoAction prestoAction;
    private final SourceQuery sourceQuery;
    private final QueryRewriter queryRewriter;
    private final List<FailureResolver> failureResolvers;
    private final VerificationContext verificationContext;

    private final String testId;
    private final boolean runTearDownOnResultMismatch;
    private final boolean failureResolverEnabled;

    public AbstractVerification(
            VerificationResubmitter verificationResubmitter,
            PrestoAction prestoAction,
            SourceQuery sourceQuery,
            QueryRewriter queryRewriter,
            List<FailureResolver> failureResolvers,
            VerificationContext verificationContext,
            VerifierConfig verifierConfig)
    {
        this.verificationResubmitter = requireNonNull(verificationResubmitter, "verificationResubmitter is null");
        this.prestoAction = requireNonNull(prestoAction, "prestoAction is null");
        this.sourceQuery = requireNonNull(sourceQuery, "sourceQuery is null");
        this.queryRewriter = requireNonNull(queryRewriter, "queryRewriter is null");
        this.failureResolvers = requireNonNull(failureResolvers, "failureResolvers is null");
        this.verificationContext = requireNonNull(verificationContext, "verificationContext is null");

        this.testId = requireNonNull(verifierConfig.getTestId(), "testId is null");
        this.runTearDownOnResultMismatch = verifierConfig.isRunTearDownOnResultMismatch();
        this.failureResolverEnabled = verifierConfig.isFailureResolverEnabled();
    }

    protected abstract VerificationResult verify(QueryBundle control, QueryBundle test);

    protected abstract Optional<Boolean> isDeterministic(QueryBundle control, ChecksumResult firstChecksum);

    @Override
    public SourceQuery getSourceQuery()
    {
        return sourceQuery;
    }

    @Override
    public Optional<VerifierQueryEvent> run()
    {
        boolean resultMismatched = false;
        QueryBundle control = null;
        QueryBundle test = null;
        VerificationResult verificationResult = null;
        Optional<Boolean> deterministic = Optional.empty();

        QueryStats controlQueryStats = null;
        QueryStats testQueryStats = null;

        try {
            control = queryRewriter.rewriteQuery(sourceQuery.getControlQuery(), CONTROL);
            test = queryRewriter.rewriteQuery(sourceQuery.getTestQuery(), TEST);
            controlQueryStats = setupAndRun(control, false);
            testQueryStats = setupAndRun(test, false);
            verificationResult = verify(control, test);

            deterministic = verificationResult.getMatchResult().isMismatchPossiblyCausedByNonDeterminism() ?
                    isDeterministic(control, verificationResult.getMatchResult().getControlChecksum()) :
                    Optional.empty();
            resultMismatched = deterministic.orElse(true) && !verificationResult.getMatchResult().isMatched();

            return Optional.of(buildEvent(
                    Optional.of(control),
                    Optional.of(test),
                    Optional.ofNullable(controlQueryStats),
                    Optional.ofNullable(testQueryStats),
                    Optional.empty(),
                    Optional.of(verificationResult),
                    deterministic));
        }
        catch (QueryException e) {
            if (verificationResubmitter.resubmit(this, e)) {
                return Optional.empty();
            }
            return Optional.of(buildEvent(
                    Optional.ofNullable(control),
                    Optional.ofNullable(test),
                    Optional.ofNullable(controlQueryStats),
                    Optional.ofNullable(testQueryStats),
                    Optional.of(e),
                    Optional.ofNullable(verificationResult),
                    deterministic));
        }
        catch (Throwable t) {
            log.error(t);
            return Optional.empty();
        }
        finally {
            if (!resultMismatched || runTearDownOnResultMismatch) {
                teardownSafely(control);
                teardownSafely(test);
            }
        }
    }

    protected PrestoAction getPrestoAction()
    {
        return prestoAction;
    }

    protected QueryRewriter getQueryRewriter()
    {
        return queryRewriter;
    }

    protected QueryStats setupAndRun(QueryBundle bundle, boolean determinismAnalysis)
    {
        checkState(!determinismAnalysis || bundle.getCluster() == CONTROL, "Determinism analysis can only be run on control cluster");
        QueryStage setupStage = determinismAnalysis ? DETERMINISM_ANALYSIS : forSetup(bundle.getCluster());
        QueryStage mainStage = determinismAnalysis ? DETERMINISM_ANALYSIS : forMain(bundle.getCluster());

        for (Statement setupQuery : bundle.getSetupQueries()) {
            prestoAction.execute(setupQuery, setupStage);
        }
        return getPrestoAction().execute(bundle.getQuery(), mainStage);
    }

    protected void teardownSafely(@Nullable QueryBundle bundle)
    {
        if (bundle == null) {
            return;
        }

        for (Statement teardownQuery : bundle.getTeardownQueries()) {
            try {
                prestoAction.execute(teardownQuery, forTeardown(bundle.getCluster()));
            }
            catch (Throwable t) {
                log.warn("Failed to teardown %s: %s", bundle.getCluster().name().toLowerCase(ENGLISH), formatSql(teardownQuery));
            }
        }
    }

    private VerifierQueryEvent buildEvent(
            Optional<QueryBundle> control,
            Optional<QueryBundle> test,
            Optional<QueryStats> controlStats,
            Optional<QueryStats> testStats,
            Optional<QueryException> queryException,
            Optional<VerificationResult> verificationResult,
            Optional<Boolean> deterministic)
    {
        boolean succeeded = verificationResult.isPresent() && verificationResult.get().getMatchResult().isMatched();

        QueryState controlState = getQueryState(controlStats, queryException, CONTROL);
        QueryState testState = getQueryState(testStats, queryException, TEST);
        String errorMessage = null;
        if (!succeeded) {
            errorMessage = format("Test state %s, Control state %s\n", testState.name(), controlState.name());

            if (queryException.isPresent()) {
                errorMessage += getStackTraceAsString(queryException.get().getCause());
            }
            if (verificationResult.isPresent()) {
                errorMessage += verificationResult.get().getMatchResult().getResultsComparison();
            }
        }

        EventStatus status;
        Optional<SkippedReason> skippedReason = getSkippedReason(controlState, deterministic);
        Optional<String> resolveMessage = Optional.empty();
        if (succeeded) {
            status = SUCCEEDED;
        }
        else if (skippedReason.isPresent()) {
            status = SKIPPED;
        }
        else {
            if (controlState == QueryState.SUCCEEDED && queryException.isPresent()) {
                checkState(controlStats.isPresent(), "control succeeded but control stats is missing");
                resolveMessage = resolveFailure(controlStats.get(), queryException.get());
            }
            status = resolveMessage.isPresent() ? FAILED_RESOLVED : FAILED;
        }

        controlStats = queryException.isPresent() && queryException.get().getQueryStage() == CONTROL_MAIN ?
                queryException.get().getQueryStats() :
                controlStats;
        testStats = queryException.isPresent() && queryException.get().getQueryStage() == TEST_MAIN ?
                queryException.get().getQueryStats() :
                testStats;

        Optional<String> errorCode = Optional.empty();
        if (!succeeded) {
            errorCode = Optional.ofNullable(queryException.map(QueryException::getErrorCode).orElse(
                    verificationResult.map(VerificationResult::getMatchResult).map(MatchResult::getMatchType).map(MatchType::name).orElse(null)));
        }

        return new VerifierQueryEvent(
                sourceQuery.getSuite(),
                testId,
                sourceQuery.getName(),
                status,
                skippedReason,
                deterministic,
                resolveMessage,
                buildQueryInfo(
                        sourceQuery.getControlConfiguration(),
                        sourceQuery.getControlQuery(),
                        verificationResult.map(VerificationResult::getControlChecksumQueryId),
                        verificationResult.map(VerificationResult::getControlChecksumQuery),
                        control,
                        controlStats),
                buildQueryInfo(
                        sourceQuery.getTestConfiguration(),
                        sourceQuery.getTestQuery(),
                        verificationResult.map(VerificationResult::getTestChecksumQueryId),
                        verificationResult.map(VerificationResult::getTestChecksumQuery),
                        test,
                        testStats),
                errorCode,
                Optional.ofNullable(errorMessage),
                queryException.map(QueryException::toQueryFailure),
                verificationContext.getQueryFailures());
    }

    private Optional<String> resolveFailure(QueryStats controlStats, QueryException queryException)
    {
        if (!failureResolverEnabled) {
            return Optional.empty();
        }
        for (FailureResolver failureResolver : failureResolvers) {
            Optional<String> resolveMessage = failureResolver.resolve(controlStats, queryException);
            if (resolveMessage.isPresent()) {
                return resolveMessage;
            }
        }
        return Optional.empty();
    }

    private static QueryInfo buildQueryInfo(
            QueryConfiguration configuration,
            String originalQuery,
            Optional<String> checksumQueryId,
            Optional<String> checksumQuery,
            Optional<QueryBundle> queryBundle,
            Optional<QueryStats> queryStats)
    {
        return new QueryInfo(
                configuration.getCatalog(),
                configuration.getSchema(),
                originalQuery,
                queryStats.map(QueryStats::getQueryId),
                checksumQueryId,
                queryBundle.map(QueryBundle::getQuery).map(AbstractVerification::formatSql),
                queryBundle.map(QueryBundle::getSetupQueries).map(AbstractVerification::formatSqls),
                queryBundle.map(QueryBundle::getTeardownQueries).map(AbstractVerification::formatSqls),
                checksumQuery,
                millisToSeconds(queryStats.map(QueryStats::getCpuTimeMillis)),
                millisToSeconds(queryStats.map(QueryStats::getWallTimeMillis)));
    }

    protected static String formatSql(Statement statement)
    {
        return SqlFormatter.formatSql(statement, Optional.empty());
    }

    protected static List<String> formatSqls(List<Statement> statements)
    {
        return statements.stream()
                .map(AbstractVerification::formatSql)
                .collect(toImmutableList());
    }

    private static Optional<SkippedReason> getSkippedReason(QueryState controlState, Optional<Boolean> deterministic)
    {
        switch (controlState) {
            case FAILED:
                return Optional.of(CONTROL_QUERY_FAILED);
            case FAILED_TO_SETUP:
                return Optional.of(CONTROL_SETUP_QUERY_FAILED);
            case TIMED_OUT:
                return Optional.of(CONTROL_QUERY_TIMED_OUT);
            case NOT_RUN:
                return Optional.of(FAILED_BEFORE_CONTROL_QUERY);
        }
        if (!deterministic.orElse(true)) {
            return Optional.of(NON_DETERMINISTIC);
        }
        return Optional.empty();
    }

    private static Optional<Double> millisToSeconds(Optional<Long> millis)
    {
        return millis.map(value -> new Duration(value, MILLISECONDS).getValue(SECONDS));
    }

    private static QueryState getQueryState(Optional<QueryStats> statsFromResult, Optional<QueryException> queryException, ClusterType cluster)
    {
        if (statsFromResult.isPresent()) {
            return QueryState.SUCCEEDED;
        }
        if (!queryException.isPresent() || queryException.get().getQueryStage().getTargetCluster() != cluster) {
            return QueryState.NOT_RUN;
        }
        if (queryException.get().getQueryStage().isSetup()) {
            return QueryState.FAILED_TO_SETUP;
        }
        if (queryException.get().getQueryStage().isMain()) {
            return queryException.get().getPrestoErrorCode().map(errorCode -> errorCode == EXCEEDED_TIME_LIMIT).orElse(false) ?
                    QueryState.TIMED_OUT :
                    QueryState.FAILED;
        }
        if (queryException.get().getQueryStage().isTeardown()) {
            return QueryState.FAILED_TO_TEARDOWN;
        }
        return QueryState.NOT_RUN;
    }

    private enum QueryState
    {
        SUCCEEDED,
        FAILED,
        TIMED_OUT,
        FAILED_TO_SETUP,
        FAILED_TO_TEARDOWN,
        NOT_RUN
    }
}
