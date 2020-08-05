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
import com.facebook.presto.verifier.event.DeterminismAnalysisDetails;
import com.facebook.presto.verifier.event.QueryInfo;
import com.facebook.presto.verifier.event.VerifierQueryEvent;
import com.facebook.presto.verifier.event.VerifierQueryEvent.EventStatus;
import com.facebook.presto.verifier.framework.MatchResult.MatchType;
import com.facebook.presto.verifier.prestoaction.PrestoAction;
import com.facebook.presto.verifier.prestoaction.QueryAction;
import com.facebook.presto.verifier.prestoaction.QueryActionStats;
import com.facebook.presto.verifier.prestoaction.QueryActions;
import com.facebook.presto.verifier.prestoaction.SqlExceptionClassifier;
import com.facebook.presto.verifier.resolver.FailureResolverManager;
import com.facebook.presto.verifier.rewrite.QueryRewriter;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.spi.StandardErrorCode.EXCEEDED_TIME_LIMIT;
import static com.facebook.presto.verifier.event.VerifierQueryEvent.EventStatus.FAILED;
import static com.facebook.presto.verifier.event.VerifierQueryEvent.EventStatus.FAILED_RESOLVED;
import static com.facebook.presto.verifier.event.VerifierQueryEvent.EventStatus.SKIPPED;
import static com.facebook.presto.verifier.event.VerifierQueryEvent.EventStatus.SUCCEEDED;
import static com.facebook.presto.verifier.framework.ClusterType.CONTROL;
import static com.facebook.presto.verifier.framework.ClusterType.TEST;
import static com.facebook.presto.verifier.framework.DataVerificationUtil.teardownSafely;
import static com.facebook.presto.verifier.framework.QueryStage.CONTROL_MAIN;
import static com.facebook.presto.verifier.framework.QueryStage.CONTROL_SETUP;
import static com.facebook.presto.verifier.framework.QueryStage.TEST_MAIN;
import static com.facebook.presto.verifier.framework.QueryStage.TEST_SETUP;
import static com.facebook.presto.verifier.framework.QueryState.FAILED_TO_SETUP;
import static com.facebook.presto.verifier.framework.QueryState.NOT_RUN;
import static com.facebook.presto.verifier.framework.QueryState.TIMED_OUT;
import static com.facebook.presto.verifier.framework.SkippedReason.CONTROL_QUERY_FAILED;
import static com.facebook.presto.verifier.framework.SkippedReason.CONTROL_QUERY_TIMED_OUT;
import static com.facebook.presto.verifier.framework.SkippedReason.CONTROL_SETUP_QUERY_FAILED;
import static com.facebook.presto.verifier.framework.SkippedReason.FAILED_BEFORE_CONTROL_QUERY;
import static com.facebook.presto.verifier.framework.SkippedReason.NON_DETERMINISTIC;
import static com.facebook.presto.verifier.framework.SkippedReason.VERIFIER_INTERNAL_ERROR;
import static com.facebook.presto.verifier.framework.VerifierUtil.runAndConsume;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.base.Throwables.getStackTraceAsString;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public abstract class AbstractVerification
        implements Verification
{
    private static final String INTERNAL_ERROR = "VERIFIER_INTERNAL_ERROR";

    private final QueryActions queryActions;
    private final SourceQuery sourceQuery;
    private final QueryRewriter queryRewriter;
    private final DeterminismAnalyzer determinismAnalyzer;
    private final FailureResolverManager failureResolverManager;
    private final SqlExceptionClassifier exceptionClassifier;
    private final VerificationContext verificationContext;

    private final String testId;
    private final boolean smartTeardown;
    private final int verificationResubmissionLimit;

    private final boolean setupOnMainClusters;
    private final boolean teardownOnMainClusters;
    private final boolean skipControl;

    public AbstractVerification(
            QueryActions queryActions,
            SourceQuery sourceQuery,
            QueryRewriter queryRewriter,
            DeterminismAnalyzer determinismAnalyzer,
            FailureResolverManager failureResolverManager,
            SqlExceptionClassifier exceptionClassifier,
            VerificationContext verificationContext,
            VerifierConfig verifierConfig)
    {
        this.queryActions = requireNonNull(queryActions, "queryActions is null");
        this.sourceQuery = requireNonNull(sourceQuery, "sourceQuery is null");
        this.queryRewriter = requireNonNull(queryRewriter, "queryRewriter is null");
        this.determinismAnalyzer = requireNonNull(determinismAnalyzer, "determinismAnalyzer is null");
        this.failureResolverManager = requireNonNull(failureResolverManager, "failureResolverManager is null");
        this.exceptionClassifier = requireNonNull(exceptionClassifier, "exceptionClassifier is null");
        this.verificationContext = requireNonNull(verificationContext, "verificationContext is null");

        this.testId = requireNonNull(verifierConfig.getTestId(), "testId is null");
        this.smartTeardown = verifierConfig.isSmartTeardown();
        this.verificationResubmissionLimit = verifierConfig.getVerificationResubmissionLimit();
        this.setupOnMainClusters = verifierConfig.isSetupOnMainClusters();
        this.teardownOnMainClusters = verifierConfig.isTeardownOnMainClusters();
        this.skipControl = verifierConfig.isSkipControl();
    }

    protected abstract MatchResult verify(QueryBundle control, QueryBundle test, ChecksumQueryContext controlContext, ChecksumQueryContext testContext);

    protected PrestoAction getHelperAction()
    {
        return queryActions.getHelperAction();
    }

    @Override
    public SourceQuery getSourceQuery()
    {
        return sourceQuery;
    }

    @Override
    public VerificationContext getVerificationContext()
    {
        return verificationContext;
    }

    @Override
    public VerificationResult run()
    {
        Optional<QueryBundle> control = Optional.empty();
        Optional<QueryBundle> test = Optional.empty();
        QueryContext controlQueryContext = new QueryContext();
        QueryContext testQueryContext = new QueryContext();
        ChecksumQueryContext controlChecksumQueryContext = new ChecksumQueryContext();
        ChecksumQueryContext testChecksumQueryContext = new ChecksumQueryContext();
        Optional<MatchResult> matchResult = Optional.empty();
        Optional<DeterminismAnalysis> determinismAnalysis = Optional.empty();
        DeterminismAnalysisDetails.Builder determinismAnalysisDetails = DeterminismAnalysisDetails.builder();

        Optional<PartialVerificationResult> partialResult = Optional.empty();
        Optional<Throwable> throwable = Optional.empty();

        try {
            // Rewrite queries
            if (!skipControl) {
                control = Optional.of(queryRewriter.rewriteQuery(sourceQuery.getControlQuery(), CONTROL));
            }
            test = Optional.of(queryRewriter.rewriteQuery(sourceQuery.getTestQuery(), TEST));

            // Run control queries
            if (skipControl) {
                controlQueryContext.setState(NOT_RUN);
            }
            else {
                QueryBundle controlQueryBundle = control.get();
                QueryAction controlSetupAction = setupOnMainClusters ? queryActions.getControlAction() : queryActions.getHelperAction();
                controlQueryBundle.getSetupQueries().forEach(query -> runAndConsume(
                        () -> controlSetupAction.execute(query, CONTROL_SETUP),
                        controlQueryContext::addSetupQuery,
                        controlQueryContext::setException));
                runAndConsume(
                        () -> queryActions.getControlAction().execute(controlQueryBundle.getQuery(), CONTROL_MAIN),
                        controlQueryContext::setMainQueryStats,
                        controlQueryContext::setException);
                controlQueryContext.setState(QueryState.SUCCEEDED);
            }

            // Run test queries
            QueryBundle testQueryBundle = test.get();
            QueryAction testSetupAction = setupOnMainClusters ? queryActions.getTestAction() : queryActions.getHelperAction();
            testQueryBundle.getSetupQueries().forEach(query -> runAndConsume(
                    () -> testSetupAction.execute(query, TEST_SETUP),
                    testQueryContext::addSetupQuery,
                    testQueryContext::setException));
            runAndConsume(
                    () -> queryActions.getTestAction().execute(testQueryBundle.getQuery(), TEST_MAIN),
                    testQueryContext::setMainQueryStats,
                    testQueryContext::setException);
            testQueryContext.setState(QueryState.SUCCEEDED);

            // Verify results
            if (!skipControl) {
                matchResult = Optional.of(verify(control.get(), test.get(), controlChecksumQueryContext, testChecksumQueryContext));

                // Determinism analysis
                if (matchResult.get().isMismatchPossiblyCausedByNonDeterminism()) {
                    determinismAnalysis = Optional.of(determinismAnalyzer.analyze(control.get(), matchResult.get().getControlChecksum(), determinismAnalysisDetails));
                }
            }

            partialResult = Optional.of(concludeVerificationPartial(control, test, controlQueryContext, testQueryContext, matchResult, determinismAnalysis, Optional.empty()));
        }
        catch (Throwable t) {
            if (exceptionClassifier.shouldResubmit(t)
                    && verificationContext.getResubmissionCount() < verificationResubmissionLimit) {
                return new VerificationResult(this, true, Optional.empty());
            }
            throwable = Optional.of(t);
            partialResult = Optional.of(concludeVerificationPartial(control, test, controlQueryContext, testQueryContext, matchResult, determinismAnalysis, Optional.of(t)));
        }
        finally {
            if (!smartTeardown
                    || testQueryContext.getState() != QueryState.SUCCEEDED
                    || (partialResult.isPresent() && partialResult.get().getStatus().equals(SUCCEEDED))) {
                QueryAction controlTeardownAction = teardownOnMainClusters ? queryActions.getControlAction() : queryActions.getHelperAction();
                QueryAction testTeardownAction = teardownOnMainClusters ? queryActions.getTestAction() : queryActions.getHelperAction();
                teardownSafely(controlTeardownAction, control, controlQueryContext::addTeardownQuery);
                teardownSafely(testTeardownAction, test, testQueryContext::addTeardownQuery);
            }
        }

        return

                concludeVerification(
                        partialResult.get(),
                        control,
                        test,
                        controlQueryContext,
                        testQueryContext,
                        matchResult,
                        determinismAnalysis,
                        controlChecksumQueryContext,
                        testChecksumQueryContext,
                        determinismAnalysisDetails.build(),
                        throwable);
    }

    private Optional<String> resolveFailure(
            Optional<QueryBundle> control,
            Optional<QueryBundle> test,
            QueryContext controlQueryContext,
            Optional<MatchResult> matchResult,
            Optional<Throwable> throwable)
    {
        if (matchResult.isPresent() && !matchResult.get().isMatched()) {
            checkState(control.isPresent(), "control is missing");
            return failureResolverManager.resolveResultMismatch(matchResult.get(), control.get());
        }
        if (throwable.isPresent() && controlQueryContext.getState() == QueryState.SUCCEEDED) {
            checkState(controlQueryContext.getMainQueryStats().isPresent(), "controlQueryStats is missing");
            return failureResolverManager.resolveException(controlQueryContext.getMainQueryStats().get(), throwable.get(), test);
        }
        return Optional.empty();
    }

    private EventStatus getEventStatus(
            Optional<SkippedReason> skippedReason,
            Optional<String> resolveMessage,
            Optional<MatchResult> matchResult,
            QueryContext testQueryContext)
    {
        if (skippedReason.isPresent()) {
            return SKIPPED;
        }
        if (resolveMessage.isPresent()) {
            return FAILED_RESOLVED;
        }
        if ((skipControl && testQueryContext.getState() == QueryState.SUCCEEDED)
                || (!skipControl && matchResult.isPresent() && matchResult.get().isMatched())) {
            return SUCCEEDED;
        }
        return FAILED;
    }

    private PartialVerificationResult concludeVerificationPartial(
            Optional<QueryBundle> control,
            Optional<QueryBundle> test,
            QueryContext controlQueryContext,
            QueryContext testQueryContext,
            Optional<MatchResult> matchResult,
            Optional<DeterminismAnalysis> determinismAnalysis,
            Optional<Throwable> throwable)
    {
        Optional<SkippedReason> skippedReason = getSkippedReason(throwable, controlQueryContext.getState(), determinismAnalysis);
        Optional<String> resolveMessage = resolveFailure(control, test, controlQueryContext, matchResult, throwable);
        EventStatus status = getEventStatus(skippedReason, resolveMessage, matchResult, testQueryContext);
        return new PartialVerificationResult(skippedReason, resolveMessage, status);
    }

    private VerificationResult concludeVerification(
            PartialVerificationResult partialResult,
            Optional<QueryBundle> control,
            Optional<QueryBundle> test,
            QueryContext controlQueryContext,
            QueryContext testQueryContext,
            Optional<MatchResult> matchResult,
            Optional<DeterminismAnalysis> determinismAnalysis,
            ChecksumQueryContext controlChecksumQueryContext,
            ChecksumQueryContext testChecksumQueryContext,
            DeterminismAnalysisDetails determinismAnalysisDetails,
            Optional<Throwable> throwable)
    {
        Optional<String> errorCode = Optional.empty();
        Optional<String> errorMessage = Optional.empty();
        if (partialResult.getStatus() != SUCCEEDED) {
            errorCode = Optional.ofNullable(throwable.map(t -> t instanceof QueryException ? ((QueryException) t).getErrorCodeName() : INTERNAL_ERROR)
                    .orElse(matchResult.map(MatchResult::getMatchType)
                            .map(MatchType::name)
                            .orElse(null)));
            errorMessage = Optional.of(constructErrorMessage(throwable, matchResult, controlQueryContext.getState(), testQueryContext.getState()));
        }

        VerifierQueryEvent event = new VerifierQueryEvent(
                sourceQuery.getSuite(),
                testId,
                sourceQuery.getName(),
                partialResult.getStatus(),
                partialResult.getSkippedReason(),
                determinismAnalysis,
                determinismAnalysis.isPresent() ?
                        Optional.of(determinismAnalysisDetails) :
                        Optional.empty(),
                partialResult.getResolveMessage(),
                skipControl ?
                        Optional.empty() :
                        Optional.of(buildQueryInfo(
                                sourceQuery.getControlConfiguration(),
                                sourceQuery.getControlQuery(),
                                controlChecksumQueryContext,
                                control,
                                controlQueryContext)),
                buildQueryInfo(
                        sourceQuery.getTestConfiguration(),
                        sourceQuery.getTestQuery(),
                        testChecksumQueryContext,
                        test,
                        testQueryContext),
                errorCode,
                errorMessage,
                throwable.filter(QueryException.class::isInstance)
                        .map(QueryException.class::cast)
                        .map(QueryException::toQueryFailure),
                verificationContext.getQueryFailures(),
                verificationContext.getResubmissionCount());
        return new VerificationResult(this, false, Optional.of(event));
    }

    private static QueryInfo buildQueryInfo(
            QueryConfiguration configuration,
            String originalQuery,
            ChecksumQueryContext checksumQueryContext,
            Optional<QueryBundle> queryBundle,
            QueryContext queryContext)
    {
        return new QueryInfo(
                configuration.getCatalog(),
                configuration.getSchema(),
                originalQuery,
                queryContext.getSetupQueryIds(),
                queryContext.getTeardownQueryIds(),
                checksumQueryContext.getChecksumQueryId(),
                queryBundle.map(QueryBundle::getQuery).map(AbstractVerification::formatSql),
                queryBundle.map(QueryBundle::getSetupQueries).map(AbstractVerification::formatSqls),
                queryBundle.map(QueryBundle::getTeardownQueries).map(AbstractVerification::formatSqls),
                checksumQueryContext.getChecksumQuery(),
                queryContext.getMainQueryStats());
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

    private Optional<SkippedReason> getSkippedReason(Optional<Throwable> throwable, QueryState controlState, Optional<DeterminismAnalysis> determinismAnalysis)
    {
        if (throwable.isPresent() && !(throwable.get() instanceof QueryException)) {
            return Optional.of(VERIFIER_INTERNAL_ERROR);
        }
        if (skipControl) {
            return Optional.empty();
        }
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
        return Optional.empty();
    }

    private static QueryState getFailingQueryState(QueryException queryException)
    {
        QueryStage queryStage = queryException.getQueryStage();
        checkArgument(
                queryStage.isSetup() || queryStage.isMain(),
                "Expect QueryStage SETUP or MAIN: %s",
                queryStage);

        if (queryStage.isSetup()) {
            return FAILED_TO_SETUP;
        }
        return queryException instanceof PrestoQueryException
                && ((PrestoQueryException) queryException).getErrorCode().equals(Optional.of(EXCEEDED_TIME_LIMIT)) ?
                TIMED_OUT :
                QueryState.FAILED;
    }

    private String constructErrorMessage(
            Optional<Throwable> throwable,
            Optional<MatchResult> matchResult,
            QueryState controlState,
            QueryState testState)
    {
        StringBuilder message = new StringBuilder(format("Test state %s, Control state %s.\n\n", testState, controlState));
        if (throwable.isPresent()) {
            if (throwable.get() instanceof PrestoQueryException) {
                PrestoQueryException exception = (PrestoQueryException) throwable.get();
                message.append(exception.getQueryStage().name().replace("_", " "))
                        .append(" query failed on ")
                        .append(exception.getQueryStage().getTargetCluster())
                        .append(" cluster:\n")
                        .append(exception.getCause() == null ? nullToEmpty(exception.getMessage()) : getStackTraceAsString(exception.getCause()));
            }
            else {
                message.append(getStackTraceAsString(throwable.get()));
            }
        }
        matchResult.ifPresent(result -> message.append(result.getResultsComparison()));
        return message.toString();
    }

    private static class QueryContext
    {
        private Optional<QueryActionStats> mainQueryStats = Optional.empty();
        private Optional<QueryState> state = Optional.empty();
        private ImmutableList.Builder<String> setupQueryIds = ImmutableList.builder();
        private ImmutableList.Builder<String> teardownQueryIds = ImmutableList.builder();

        public Optional<QueryActionStats> getMainQueryStats()
        {
            return mainQueryStats;
        }

        public void setMainQueryStats(QueryActionStats mainQueryStats)
        {
            checkState(!this.mainQueryStats.isPresent(), "mainQueryStats is already set", mainQueryStats);
            this.mainQueryStats = Optional.of(mainQueryStats);
        }

        public QueryState getState()
        {
            return state.orElse(NOT_RUN);
        }

        public void setState(QueryState state)
        {
            checkState(!this.state.isPresent(), "state is already set", state);
            this.state = Optional.of(state);
        }

        public void setException(QueryException e)
        {
            setState(getFailingQueryState(e));
        }

        public List<String> getSetupQueryIds()
        {
            return setupQueryIds.build();
        }

        public void addSetupQuery(QueryActionStats queryActionStats)
        {
            queryActionStats.getQueryStats().map(QueryStats::getQueryId).ifPresent(setupQueryIds::add);
        }

        public List<String> getTeardownQueryIds()
        {
            return teardownQueryIds.build();
        }

        public void addTeardownQuery(QueryActionStats queryActionStats)
        {
            queryActionStats.getQueryStats().map(QueryStats::getQueryId).ifPresent(teardownQueryIds::add);
        }
    }

    private static class PartialVerificationResult
    {
        private final Optional<SkippedReason> skippedReason;
        private final Optional<String> resolveMessage;
        private final EventStatus status;

        public PartialVerificationResult(Optional<SkippedReason> skippedReason, Optional<String> resolveMessage, EventStatus status)
        {
            this.skippedReason = requireNonNull(skippedReason, "skippedReason is null");
            this.resolveMessage = requireNonNull(resolveMessage, "resolveMessage is null");
            this.status = requireNonNull(status, "status is null");
        }

        public Optional<SkippedReason> getSkippedReason()
        {
            return skippedReason;
        }

        public Optional<String> getResolveMessage()
        {
            return resolveMessage;
        }

        public EventStatus getStatus()
        {
            return status;
        }
    }
}
