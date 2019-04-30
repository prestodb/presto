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
import com.facebook.presto.verifier.event.FailureInfo;
import com.facebook.presto.verifier.event.QueryInfo;
import com.facebook.presto.verifier.event.VerifierQueryEvent;
import com.facebook.presto.verifier.event.VerifierQueryEvent.EventStatus;
import com.facebook.presto.verifier.framework.MatchResult.MatchType;
import com.facebook.presto.verifier.framework.QueryOrigin.TargetCluster;
import com.facebook.presto.verifier.resolver.FailureResolver;
import io.airlift.log.Logger;
import io.airlift.units.Duration;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.spi.StandardErrorCode.EXCEEDED_TIME_LIMIT;
import static com.facebook.presto.verifier.event.VerifierQueryEvent.EventStatus.FAILED;
import static com.facebook.presto.verifier.event.VerifierQueryEvent.EventStatus.FAILED_RESOLVED;
import static com.facebook.presto.verifier.event.VerifierQueryEvent.EventStatus.SKIPPED;
import static com.facebook.presto.verifier.event.VerifierQueryEvent.EventStatus.SUCCEEDED;
import static com.facebook.presto.verifier.framework.QueryOrigin.QueryStage.MAIN;
import static com.facebook.presto.verifier.framework.QueryOrigin.QueryStage.SETUP;
import static com.facebook.presto.verifier.framework.QueryOrigin.QueryStage.TEARDOWN;
import static com.facebook.presto.verifier.framework.QueryOrigin.TargetCluster.CONTROL;
import static com.facebook.presto.verifier.framework.QueryOrigin.TargetCluster.TEST;
import static com.facebook.presto.verifier.framework.QueryOrigin.forMain;
import static com.facebook.presto.verifier.framework.QueryOrigin.forSetup;
import static com.facebook.presto.verifier.framework.QueryOrigin.forTeardown;
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

    private final PrestoAction prestoAction;
    private final SourceQuery sourceQuery;
    private final QueryRewriter queryRewriter;
    private final List<FailureResolver> failureResolvers;

    private final String testId;
    private final boolean runTearDownOnResultMismatch;
    private final boolean failureResolverEnabled;

    private final VerificationContext verificationContext = new VerificationContext();

    private Map<TargetCluster, QueryStats> queryStats = new EnumMap<>(TargetCluster.class);

    public AbstractVerification(
            PrestoAction prestoAction,
            SourceQuery sourceQuery,
            QueryRewriter queryRewriter,
            List<FailureResolver> failureResolvers,
            VerifierConfig config)
    {
        this.prestoAction = requireNonNull(prestoAction, "prestoAction is null");
        this.sourceQuery = requireNonNull(sourceQuery, "sourceQuery is null");
        this.queryRewriter = requireNonNull(queryRewriter, "queryRewriter is null");
        this.failureResolvers = requireNonNull(failureResolvers, "failureResolvers is null");

        this.testId = requireNonNull(config.getTestId(), "testId is null");
        this.runTearDownOnResultMismatch = config.isRunTearDownOnResultMismatch();
        this.failureResolverEnabled = config.isFailureResolverEnabled();
    }

    protected abstract VerificationResult verify(QueryBundle control, QueryBundle test);

    protected abstract Optional<Boolean> isDeterministic(QueryBundle control, ChecksumResult firstChecksum);

    protected VerificationContext getVerificationContext()
    {
        return verificationContext;
    }

    protected void setQueryStats(QueryStats queryStats, TargetCluster cluster)
    {
        checkState(!this.queryStats.containsKey(cluster), "%sQueryStats has already been set", cluster.name().toLowerCase(ENGLISH));
        this.queryStats.put(cluster, queryStats);
    }

    @Override
    public Optional<VerifierQueryEvent> run()
    {
        boolean resultMismatched = false;
        QueryBundle control = null;
        QueryBundle test = null;
        VerificationResult verificationResult = null;
        Optional<Boolean> deterministic = Optional.empty();

        try {
            control = queryRewriter.rewriteQuery(sourceQuery.getControlQuery(), CONTROL, getConfiguration(CONTROL), getVerificationContext());
            test = queryRewriter.rewriteQuery(sourceQuery.getTestQuery(), TEST, getConfiguration(TEST), getVerificationContext());
            verificationResult = verify(control, test);

            deterministic = verificationResult.getMatchResult().isMismatchPossiblyCausedByNonDeterminism() ?
                    isDeterministic(control, verificationResult.getMatchResult().getControlChecksum()) :
                    Optional.empty();
            resultMismatched = deterministic.orElse(true) && !verificationResult.getMatchResult().isMatched();

            return Optional.of(buildEvent(
                    Optional.of(control),
                    Optional.of(test),
                    Optional.of(queryStats.get(CONTROL)),
                    Optional.of(queryStats.get(TEST)),
                    Optional.empty(),
                    Optional.of(verificationResult),
                    deterministic));
        }
        catch (QueryException e) {
            return Optional.of(buildEvent(
                    Optional.ofNullable(control),
                    Optional.ofNullable(test),
                    Optional.ofNullable(queryStats.get(CONTROL)),
                    Optional.ofNullable(queryStats.get(TEST)),
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
                if (control != null) {
                    teardownSafely(control, CONTROL);
                }
                if (test != null) {
                    teardownSafely(test, TEST);
                }
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

    protected SourceQuery getSourceQuery()
    {
        return sourceQuery;
    }

    protected QueryConfiguration getConfiguration(TargetCluster cluster)
    {
        checkState(cluster == CONTROL || cluster == TEST, "Unexpected TargetCluster %s", cluster);
        return cluster == CONTROL ? sourceQuery.getControlConfiguration() : sourceQuery.getTestConfiguration();
    }

    protected void setup(QueryBundle control, TargetCluster cluster)
    {
        for (Statement setupQuery : control.getSetupQueries()) {
            prestoAction.execute(setupQuery, getConfiguration(cluster), forSetup(cluster), getVerificationContext());
        }
    }

    protected void teardownSafely(QueryBundle control, TargetCluster cluster)
    {
        for (Statement teardownQuery : control.getTeardownQueries()) {
            try {
                prestoAction.execute(teardownQuery, getConfiguration(cluster), forTeardown(cluster), getVerificationContext());
            }
            catch (Throwable t) {
                log.warn("Failed to teardown %s: %s", cluster.name().toLowerCase(ENGLISH), formatSql(teardownQuery));
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
        Optional<String> resolveMessage = Optional.empty();
        if (succeeded) {
            status = SUCCEEDED;
        }
        else if (isSkipped(controlState, deterministic)) {
            status = SKIPPED;
        }
        else {
            if (controlState == QueryState.SUCCEEDED && queryException.isPresent()) {
                checkState(controlStats.isPresent(), "control succeeded but control stats is missing");
                resolveMessage = resolveFailure(controlStats.get(), queryException.get());
            }
            status = resolveMessage.isPresent() ? FAILED_RESOLVED : FAILED;
        }

        controlStats = queryException.isPresent() && queryException.get().getQueryOrigin().equals(forMain(CONTROL)) ?
                queryException.get().getQueryStats() :
                controlStats;
        testStats = queryException.isPresent() && queryException.get().getQueryOrigin().equals(forMain(TEST)) ?
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
                deterministic,
                resolveMessage,
                buildQueryInfo(
                        sourceQuery.getControlConfiguration(),
                        sourceQuery.getControlQuery(),
                        verificationResult.map(VerificationResult::getControlChecksumQueryId),
                        verificationResult.map(VerificationResult::getControlChecksumQuery),
                        control,
                        controlStats,
                        verificationContext.getAllFailures(CONTROL)),
                buildQueryInfo(
                        sourceQuery.getTestConfiguration(),
                        sourceQuery.getTestQuery(),
                        verificationResult.map(VerificationResult::getTestChecksumQueryId),
                        verificationResult.map(VerificationResult::getTestChecksumQuery),
                        test,
                        testStats,
                        verificationContext.getAllFailures(TEST)),
                errorCode,
                Optional.ofNullable(errorMessage));
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
            Optional<QueryStats> queryStats,
            List<FailureInfo> allFailures)
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
                millisToSeconds(queryStats.map(QueryStats::getWallTimeMillis)),
                allFailures);
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

    private static boolean isSkipped(QueryState controlState, Optional<Boolean> deterministic)
    {
        if (controlState == QueryState.FAILED ||
                controlState == QueryState.FAILED_TO_SETUP ||
                controlState == QueryState.TIMED_OUT ||
                controlState == QueryState.NOT_RUN) {
            return true;
        }
        if (!deterministic.orElse(true)) {
            return true;
        }
        return false;
    }

    private static Optional<Double> millisToSeconds(Optional<Long> millis)
    {
        return millis.map(value -> new Duration(value, MILLISECONDS).getValue(SECONDS));
    }

    private static QueryState getQueryState(Optional<QueryStats> statsFromResult, Optional<QueryException> queryException, TargetCluster cluster)
    {
        if (statsFromResult.isPresent()) {
            return QueryState.SUCCEEDED;
        }
        if (!queryException.isPresent() || queryException.get().getQueryOrigin().getCluster() != cluster) {
            return QueryState.NOT_RUN;
        }
        if (queryException.get().getQueryOrigin().getStage() == SETUP) {
            return QueryState.FAILED_TO_SETUP;
        }
        if (queryException.get().getQueryOrigin().getStage() == MAIN) {
            return queryException.get().getPrestoErrorCode().map(errorCode -> errorCode == EXCEEDED_TIME_LIMIT).orElse(false) ?
                    QueryState.TIMED_OUT :
                    QueryState.FAILED;
        }
        if (queryException.get().getQueryOrigin().getStage() == TEARDOWN) {
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
