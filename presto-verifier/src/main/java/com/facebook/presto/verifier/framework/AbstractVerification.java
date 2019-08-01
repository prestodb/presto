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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.jdbc.QueryStats;
import com.facebook.presto.sql.SqlFormatter;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.verifier.checksum.ChecksumResult;
import com.facebook.presto.verifier.event.DeterminismAnalysisDetails;
import com.facebook.presto.verifier.event.QueryInfo;
import com.facebook.presto.verifier.event.VerifierQueryEvent;
import com.facebook.presto.verifier.event.VerifierQueryEvent.EventStatus;
import com.facebook.presto.verifier.framework.MatchResult.MatchType;
import com.facebook.presto.verifier.prestoaction.PrestoAction;
import com.facebook.presto.verifier.resolver.FailureResolverManager;
import com.facebook.presto.verifier.rewrite.QueryRewriter;
import io.airlift.units.Duration;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.spi.StandardErrorCode.COMPILER_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.EXCEEDED_TIME_LIMIT;
import static com.facebook.presto.verifier.event.VerifierQueryEvent.EventStatus.FAILED;
import static com.facebook.presto.verifier.event.VerifierQueryEvent.EventStatus.FAILED_RESOLVED;
import static com.facebook.presto.verifier.event.VerifierQueryEvent.EventStatus.SKIPPED;
import static com.facebook.presto.verifier.event.VerifierQueryEvent.EventStatus.SUCCEEDED;
import static com.facebook.presto.verifier.framework.ClusterType.CONTROL;
import static com.facebook.presto.verifier.framework.ClusterType.TEST;
import static com.facebook.presto.verifier.framework.QueryStage.CHECKSUM;
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
import static com.facebook.presto.verifier.framework.SkippedReason.VERIFIER_LIMITATION;
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
    private final FailureResolverManager failureResolverManager;
    private final VerificationContext verificationContext;

    private final String testId;
    private final boolean runTearDownOnResultMismatch;

    public AbstractVerification(
            VerificationResubmitter verificationResubmitter,
            PrestoAction prestoAction,
            SourceQuery sourceQuery,
            QueryRewriter queryRewriter,
            FailureResolverManager failureResolverManager,
            VerificationContext verificationContext,
            VerifierConfig verifierConfig)
    {
        this.verificationResubmitter = requireNonNull(verificationResubmitter, "verificationResubmitter is null");
        this.prestoAction = requireNonNull(prestoAction, "prestoAction is null");
        this.sourceQuery = requireNonNull(sourceQuery, "sourceQuery is null");
        this.queryRewriter = requireNonNull(queryRewriter, "queryRewriter is null");
        this.failureResolverManager = requireNonNull(failureResolverManager, "failureResolverManager is null");
        this.verificationContext = requireNonNull(verificationContext, "verificationContext is null");

        this.testId = requireNonNull(verifierConfig.getTestId(), "testId is null");
        this.runTearDownOnResultMismatch = verifierConfig.isRunTeardownOnResultMismatch();
    }

    protected abstract MatchResult verify(QueryBundle control, QueryBundle test);

    protected abstract DeterminismAnalysis analyzeDeterminism(QueryBundle control, ChecksumResult firstChecksum);

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
        MatchResult matchResult = null;
        Optional<DeterminismAnalysis> determinismAnalysis = Optional.empty();

        QueryStats controlQueryStats = null;
        QueryStats testQueryStats = null;

        try {
            control = queryRewriter.rewriteQuery(sourceQuery.getControlQuery(), CONTROL);
            test = queryRewriter.rewriteQuery(sourceQuery.getTestQuery(), TEST);
            controlQueryStats = setupAndRun(control, false);
            testQueryStats = setupAndRun(test, false);
            matchResult = verify(control, test);

            if (matchResult.isMismatchPossiblyCausedByNonDeterminism()) {
                determinismAnalysis = Optional.of(analyzeDeterminism(control, matchResult.getControlChecksum()));
            }
            boolean maybeDeterministic = !determinismAnalysis.isPresent() ||
                    determinismAnalysis.get().isDeterministic() ||
                    determinismAnalysis.get().isUnknown();
            resultMismatched = maybeDeterministic && !matchResult.isMatched();

            return Optional.of(buildEvent(
                    Optional.of(control),
                    Optional.of(test),
                    Optional.ofNullable(controlQueryStats),
                    Optional.ofNullable(testQueryStats),
                    Optional.empty(),
                    Optional.of(matchResult),
                    determinismAnalysis));
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
                    Optional.ofNullable(matchResult),
                    determinismAnalysis));
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

    protected VerificationContext getVerificationContext()
    {
        return verificationContext;
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
            Optional<MatchResult> matchResult,
            Optional<DeterminismAnalysis> determinismAnalysis)
    {
        boolean succeeded = matchResult.isPresent() && matchResult.get().isMatched();

        QueryState controlState = getQueryState(controlStats, queryException, CONTROL);
        QueryState testState = getQueryState(testStats, queryException, TEST);
        String errorMessage = null;
        if (!succeeded) {
            errorMessage = format("Test state %s, Control state %s.\n\n", testState.name(), controlState.name());

            if (queryException.isPresent()) {
                errorMessage += format(
                        "%s query failed on %s cluster:\n%s",
                        queryException.get().getQueryStage().name().replace("_", " "),
                        queryException.get().getQueryStage().getTargetCluster(),
                        getStackTraceAsString(queryException.get().getCause()));
            }
            if (matchResult.isPresent()) {
                errorMessage += matchResult.get().getResultsComparison();
            }
        }

        EventStatus status;
        Optional<SkippedReason> skippedReason = getSkippedReason(controlState, determinismAnalysis, queryException);
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
                resolveMessage = failureResolverManager.resolve(controlStats.get(), queryException.get(), test);
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
                    matchResult.map(MatchResult::getMatchType).map(MatchType::name).orElse(null)));
        }

        return new VerifierQueryEvent(
                sourceQuery.getSuite(),
                testId,
                sourceQuery.getName(),
                status,
                skippedReason,
                determinismAnalysis,
                determinismAnalysis.isPresent() ?
                        Optional.of(new DeterminismAnalysisDetails(
                                verificationContext.getDeterminismAnalysisRuns(),
                                verificationContext.getLimitQueryAnalysis(),
                                verificationContext.getLimitQueryAnalysisQueryId())) :
                        Optional.empty(),
                resolveMessage,
                buildQueryInfo(
                        sourceQuery.getControlConfiguration(),
                        sourceQuery.getControlQuery(),
                        verificationContext.getControlChecksumQueryId(),
                        verificationContext.getControlChecksumQuery(),
                        control,
                        controlStats),
                buildQueryInfo(
                        sourceQuery.getTestConfiguration(),
                        sourceQuery.getTestQuery(),
                        verificationContext.getTestChecksumQueryId(),
                        verificationContext.getTestChecksumQuery(),
                        test,
                        testStats),
                errorCode,
                Optional.ofNullable(errorMessage),
                queryException.map(QueryException::toQueryFailure),
                verificationContext.getQueryFailures());
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

    private static Optional<SkippedReason> getSkippedReason(
            QueryState controlState,
            Optional<DeterminismAnalysis> determinismAnalysis,
            Optional<QueryException> queryException)
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
        if (determinismAnalysis.isPresent() && determinismAnalysis.get().isNonDeterministic()) {
            return Optional.of(NON_DETERMINISTIC);
        }
        if (queryException.isPresent() &&
                queryException.get().getQueryStage().equals(CHECKSUM) &&
                queryException.get().getPrestoErrorCode().isPresent() &&
                queryException.get().getPrestoErrorCode().get().equals(COMPILER_ERROR)) {
            return Optional.of(VERIFIER_LIMITATION);
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
