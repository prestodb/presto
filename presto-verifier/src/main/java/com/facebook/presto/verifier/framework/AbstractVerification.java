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
import com.facebook.presto.verifier.prestoaction.PrestoAction;
import com.facebook.presto.verifier.prestoaction.PrestoAction.ResultSetConverter;
import com.facebook.presto.verifier.prestoaction.QueryAction;
import com.facebook.presto.verifier.prestoaction.QueryActionStats;
import com.facebook.presto.verifier.prestoaction.QueryActions;
import com.facebook.presto.verifier.prestoaction.SqlExceptionClassifier;
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
import static com.facebook.presto.verifier.framework.VerifierUtil.callAndConsume;
import static com.facebook.presto.verifier.framework.VerifierUtil.runAndConsume;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.base.Throwables.getStackTraceAsString;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public abstract class AbstractVerification<B extends QueryBundle, R extends MatchResult, V>
        implements Verification
{
    private static final String INTERNAL_ERROR = "VERIFIER_INTERNAL_ERROR";

    private final QueryActions queryActions;
    private final SourceQuery sourceQuery;
    private final SqlExceptionClassifier exceptionClassifier;
    private final VerificationContext verificationContext;
    private final Optional<ResultSetConverter<V>> mainQueryResultSetConverter;

    private final String testId;
    private final boolean smartTeardown;
    private final int verificationResubmissionLimit;

    private final boolean setupOnMainClusters;
    private final boolean teardownOnMainClusters;
    private final boolean skipControl;

    public AbstractVerification(
            QueryActions queryActions,
            SourceQuery sourceQuery,
            SqlExceptionClassifier exceptionClassifier,
            VerificationContext verificationContext,
            Optional<ResultSetConverter<V>> mainQueryResultSetConverter,
            VerifierConfig verifierConfig)
    {
        this.queryActions = requireNonNull(queryActions, "queryActions is null");
        this.sourceQuery = requireNonNull(sourceQuery, "sourceQuery is null");
        this.exceptionClassifier = requireNonNull(exceptionClassifier, "exceptionClassifier is null");
        this.verificationContext = requireNonNull(verificationContext, "verificationContext is null");
        this.mainQueryResultSetConverter = requireNonNull(mainQueryResultSetConverter, "mainQueryResultSetConverter is null");

        this.testId = requireNonNull(verifierConfig.getTestId(), "testId is null");
        this.smartTeardown = verifierConfig.isSmartTeardown();
        this.verificationResubmissionLimit = verifierConfig.getVerificationResubmissionLimit();
        this.setupOnMainClusters = verifierConfig.isSetupOnMainClusters();
        this.teardownOnMainClusters = verifierConfig.isTeardownOnMainClusters();
        this.skipControl = verifierConfig.isSkipControl();
    }

    protected abstract B getQueryRewrite(ClusterType clusterType);

    protected abstract R verify(
            B control,
            B test,
            Optional<QueryResult<V>> controlQueryResult,
            Optional<QueryResult<V>> testQueryResult,
            ChecksumQueryContext controlContext,
            ChecksumQueryContext testContext);

    protected abstract DeterminismAnalysisDetails analyzeDeterminism(B control, R matchResult);

    protected abstract Optional<String> resolveFailure(
            Optional<B> control,
            Optional<B> test,
            QueryContext controlQueryContext,
            Optional<R> matchResult,
            Optional<Throwable> throwable);

    protected void updateQueryInfo(QueryInfo.Builder queryInfo, Optional<QueryResult<V>> queryResult) {}

    protected void updateQueryInfoWithQueryBundle(QueryInfo.Builder queryInfo, Optional<B> queryBundle)
    {
        queryInfo.setQuery(queryBundle.map(B::getQuery).map(AbstractVerification::formatSql))
                .setSetupQueries(queryBundle.map(B::getSetupQueries).map(AbstractVerification::formatSqls))
                .setTeardownQueries(queryBundle.map(B::getTeardownQueries).map(AbstractVerification::formatSqls));
    }

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
        Optional<B> control = Optional.empty();
        Optional<B> test = Optional.empty();
        Optional<QueryResult<V>> controlQueryResult = Optional.empty();
        Optional<QueryResult<V>> testQueryResult = Optional.empty();
        QueryContext controlQueryContext = new QueryContext();
        QueryContext testQueryContext = new QueryContext();
        ChecksumQueryContext controlChecksumQueryContext = new ChecksumQueryContext();
        ChecksumQueryContext testChecksumQueryContext = new ChecksumQueryContext();
        Optional<R> matchResult = Optional.empty();
        Optional<DeterminismAnalysisDetails> determinismAnalysisDetails = Optional.empty();

        Optional<PartialVerificationResult> partialResult = Optional.empty();
        Optional<Throwable> throwable = Optional.empty();

        try {
            // Rewrite queries
            if (!skipControl) {
                control = Optional.of(getQueryRewrite(CONTROL));
            }
            test = Optional.of(getQueryRewrite(TEST));

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
                controlQueryResult = runMainQuery(controlQueryBundle.getQuery(), CONTROL, controlQueryContext);
                controlQueryContext.setState(QueryState.SUCCEEDED);
            }

            // Run test queries
            QueryBundle testQueryBundle = test.get();
            QueryAction testSetupAction = setupOnMainClusters ? queryActions.getTestAction() : queryActions.getHelperAction();
            testQueryBundle.getSetupQueries().forEach(query -> runAndConsume(
                    () -> testSetupAction.execute(query, TEST_SETUP),
                    testQueryContext::addSetupQuery,
                    testQueryContext::setException));
            testQueryResult = runMainQuery(testQueryBundle.getQuery(), TEST, testQueryContext);
            testQueryContext.setState(QueryState.SUCCEEDED);

            // Verify results
            if (!skipControl) {
                matchResult = Optional.of(verify(control.get(), test.get(), controlQueryResult, testQueryResult, controlChecksumQueryContext, testChecksumQueryContext));

                // Determinism analysis
                if (matchResult.get().isMismatchPossiblyCausedByNonDeterminism()) {
                    determinismAnalysisDetails = Optional.of(analyzeDeterminism(control.get(), matchResult.get()));
                }
            }

            partialResult = Optional.of(concludeVerificationPartial(control, test, controlQueryContext, testQueryContext, matchResult, determinismAnalysisDetails, Optional.empty()));
        }
        catch (Throwable t) {
            if (exceptionClassifier.shouldResubmit(t)
                    && verificationContext.getResubmissionCount() < verificationResubmissionLimit) {
                return new VerificationResult(this, true, Optional.empty());
            }
            throwable = Optional.of(t);
            partialResult = Optional.of(concludeVerificationPartial(control, test, controlQueryContext, testQueryContext, matchResult, determinismAnalysisDetails, Optional.of(t)));
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

        return concludeVerification(
                partialResult.get(),
                control,
                test,
                controlQueryResult,
                testQueryResult,
                controlQueryContext,
                testQueryContext,
                matchResult,
                controlChecksumQueryContext,
                testChecksumQueryContext,
                determinismAnalysisDetails,
                throwable);
    }

    private Optional<QueryResult<V>> runMainQuery(Statement statement, ClusterType clusterType, QueryContext queryContext)
    {
        checkArgument(clusterType == CONTROL || clusterType == TEST, "Invalid ClusterType: %s", clusterType);

        if (mainQueryResultSetConverter.isPresent()) {
            return Optional.of(callAndConsume(
                    () -> ((PrestoAction) queryActions.getQueryAction(clusterType)).execute(
                            statement,
                            clusterType == CONTROL ? CONTROL_MAIN : TEST_MAIN,
                            mainQueryResultSetConverter.get()),
                    queryContext::setMainQueryStats,
                    queryContext::setException));
        }

        runAndConsume(
                () -> queryActions.getQueryAction(clusterType).execute(statement, clusterType == CONTROL ? CONTROL_MAIN : TEST_MAIN),
                queryContext::setMainQueryStats,
                queryContext::setException);
        return Optional.empty();
    }

    private EventStatus getEventStatus(
            Optional<SkippedReason> skippedReason,
            Optional<String> resolveMessage,
            Optional<R> matchResult,
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
            Optional<B> control,
            Optional<B> test,
            QueryContext controlQueryContext,
            QueryContext testQueryContext,
            Optional<R> matchResult,
            Optional<DeterminismAnalysisDetails> determinismAnalysisDetails,
            Optional<Throwable> throwable)
    {
        Optional<SkippedReason> skippedReason = getSkippedReason(throwable, controlQueryContext.getState(), determinismAnalysisDetails.map(DeterminismAnalysisDetails::getDeterminismAnalysis));
        Optional<String> resolveMessage = resolveFailure(control, test, controlQueryContext, matchResult, throwable);
        EventStatus status = getEventStatus(skippedReason, resolveMessage, matchResult, testQueryContext);
        return new PartialVerificationResult(skippedReason, resolveMessage, status);
    }

    private VerificationResult concludeVerification(
            PartialVerificationResult partialResult,
            Optional<B> control,
            Optional<B> test,
            Optional<QueryResult<V>> controlQueryResult,
            Optional<QueryResult<V>> testQueryResult,
            QueryContext controlQueryContext,
            QueryContext testQueryContext,
            Optional<R> matchResult,
            ChecksumQueryContext controlChecksumQueryContext,
            ChecksumQueryContext testChecksumQueryContext,
            Optional<DeterminismAnalysisDetails> determinismAnalysisDetails,
            Optional<Throwable> throwable)
    {
        Optional<String> errorCode = Optional.empty();
        Optional<String> errorMessage = Optional.empty();
        if (partialResult.getStatus() != SUCCEEDED) {
            errorCode = Optional.ofNullable(throwable.map(t -> t instanceof QueryException ? ((QueryException) t).getErrorCodeName() : INTERNAL_ERROR)
                    .orElse(matchResult.map(MatchResult::getMatchTypeName).orElse(null)));
            errorMessage = Optional.of(constructErrorMessage(throwable, matchResult, controlQueryContext.getState(), testQueryContext.getState()));
        }

        VerifierQueryEvent event = new VerifierQueryEvent(
                sourceQuery.getSuite(),
                testId,
                sourceQuery.getName(),
                partialResult.getStatus(),
                matchResult.map(MatchResult::getMatchTypeName),
                partialResult.getSkippedReason(),
                determinismAnalysisDetails,
                partialResult.getResolveMessage(),
                skipControl ?
                        Optional.empty() :
                        Optional.of(buildQueryInfo(
                                sourceQuery.getControlConfiguration(),
                                sourceQuery.getQuery(CONTROL),
                                controlChecksumQueryContext,
                                control,
                                controlQueryContext,
                                controlQueryResult)),
                buildQueryInfo(
                        sourceQuery.getTestConfiguration(),
                        sourceQuery.getQuery(TEST),
                        testChecksumQueryContext,
                        test,
                        testQueryContext,
                        testQueryResult),
                errorCode,
                errorMessage,
                throwable.filter(QueryException.class::isInstance)
                        .map(QueryException.class::cast)
                        .map(QueryException::toQueryFailure),
                verificationContext.getQueryFailures(),
                verificationContext.getResubmissionCount());
        return new VerificationResult(this, false, Optional.of(event));
    }

    private QueryInfo buildQueryInfo(
            QueryConfiguration configuration,
            String originalQuery,
            ChecksumQueryContext checksumQueryContext,
            Optional<B> queryBundle,
            QueryContext queryContext,
            Optional<QueryResult<V>> queryResult)
    {
        QueryInfo.Builder queryInfo = QueryInfo.builder(configuration.getCatalog(), configuration.getSchema(), originalQuery, configuration.getSessionProperties())
                .setSetupQueryIds(queryContext.getSetupQueryIds())
                .setTeardownQueryIds(queryContext.getTeardownQueryIds())
                .setChecksumQueryId(checksumQueryContext.getChecksumQueryId())
                .setChecksumQuery(checksumQueryContext.getChecksumQuery())
                .setQueryActionStats(queryContext.getMainQueryStats());
        updateQueryInfoWithQueryBundle(queryInfo, queryBundle);
        updateQueryInfo(queryInfo, queryResult);
        return queryInfo.build();
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
            Optional<R> matchResult,
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
        matchResult.ifPresent(result -> message.append(result.getReport()));
        return message.toString();
    }

    protected static class QueryContext
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
