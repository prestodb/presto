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

import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.jdbc.QueryStats;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.verifier.checksum.ChecksumResult;
import com.facebook.presto.verifier.checksum.ChecksumValidator;
import com.facebook.presto.verifier.event.DeterminismAnalysisDetails;
import com.facebook.presto.verifier.event.QueryInfo;
import com.facebook.presto.verifier.prestoaction.QueryActions;
import com.facebook.presto.verifier.prestoaction.SqlExceptionClassifier;
import com.facebook.presto.verifier.resolver.FailureResolverManager;
import com.facebook.presto.verifier.rewrite.QueryRewriter;
import com.facebook.presto.verifier.source.SnapshotQueryConsumer;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListeningExecutorService;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static com.facebook.presto.verifier.framework.DataMatchResult.DataType.DATA;
import static com.facebook.presto.verifier.framework.DataMatchResult.MatchType.MATCH;
import static com.facebook.presto.verifier.framework.DataMatchResult.MatchType.SNAPSHOT_DOES_NOT_EXIST;
import static com.facebook.presto.verifier.framework.DataVerificationUtil.getColumns;
import static com.facebook.presto.verifier.framework.DataVerificationUtil.match;
import static com.facebook.presto.verifier.framework.QueryStage.CONTROL_CHECKSUM;
import static com.facebook.presto.verifier.framework.QueryStage.TEST_CHECKSUM;
import static com.facebook.presto.verifier.framework.VerifierConfig.QUERY_BANK_MODE;
import static com.facebook.presto.verifier.framework.VerifierUtil.callAndConsume;
import static com.facebook.presto.verifier.source.AbstractJdbiSnapshotQuerySupplier.VERIFIER_SNAPSHOT_KEY_PATTERN;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class DataVerification
        extends AbstractVerification<QueryObjectBundle, DataMatchResult, Void>
{
    protected final QueryRewriter queryRewriter;
    protected final DeterminismAnalyzer determinismAnalyzer;
    protected final FailureResolverManager failureResolverManager;
    protected final TypeManager typeManager;
    protected final ChecksumValidator checksumValidator;

    public DataVerification(
            QueryActions queryActions,
            SourceQuery sourceQuery,
            QueryRewriter queryRewriter,
            DeterminismAnalyzer determinismAnalyzer,
            FailureResolverManager failureResolverManager,
            SqlExceptionClassifier exceptionClassifier,
            VerificationContext verificationContext,
            VerifierConfig verifierConfig,
            TypeManager typeManager,
            ChecksumValidator checksumValidator,
            ListeningExecutorService executor,
            SnapshotQueryConsumer snapshotQueryConsumer,
            Map<String, SnapshotQuery> snapshotQueries)
    {
        super(queryActions, sourceQuery, exceptionClassifier, verificationContext, Optional.empty(), verifierConfig, executor, snapshotQueryConsumer, snapshotQueries);
        this.queryRewriter = requireNonNull(queryRewriter, "queryRewriter is null");
        this.determinismAnalyzer = requireNonNull(determinismAnalyzer, "determinismAnalyzer is null");
        this.failureResolverManager = requireNonNull(failureResolverManager, "failureResolverManager is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.checksumValidator = requireNonNull(checksumValidator, "checksumValidator is null");
    }

    @Override
    protected QueryObjectBundle getQueryRewrite(ClusterType clusterType)
    {
        return queryRewriter.rewriteQuery(getSourceQuery().getQuery(clusterType), getSourceQuery().getQueryConfiguration(clusterType), clusterType,
                getVerificationContext().getResubmissionCount() == 0);
    }

    @Override
    protected void updateQueryInfoWithQueryBundle(QueryInfo.Builder queryInfo, Optional<QueryObjectBundle> queryBundle)
    {
        super.updateQueryInfoWithQueryBundle(queryInfo, queryBundle);
        queryInfo.setQuery(queryBundle.map(bundle -> formatSql(bundle.getQuery(), bundle.getRewrittenFunctionCalls())))
                .setOutputTableName(queryBundle.map(QueryObjectBundle::getObjectName).map(QualifiedName::toString))
                .setIsReuseTable(queryBundle.map(QueryObjectBundle::isReuseTable).orElse(false));
    }

    @Override
    public DataMatchResult verify(
            QueryObjectBundle control,
            QueryObjectBundle test,
            Optional<QueryResult<Void>> controlQueryResult,
            Optional<QueryResult<Void>> testQueryResult,
            ChecksumQueryContext controlChecksumQueryContext,
            ChecksumQueryContext testChecksumQueryContext)
    {
        List<Column> testColumns = getColumns(getHelperAction(), typeManager, test.getObjectName());
        Query testChecksumQuery = checksumValidator.generateChecksumQuery(test.getObjectName(), testColumns);
        testChecksumQueryContext.setChecksumQuery(formatSql(testChecksumQuery));

        List<Column> controlColumns = null;
        ChecksumResult controlChecksumResult = null;

        if (isControlEnabled()) {
            controlColumns = getColumns(getHelperAction(), typeManager, control.getObjectName());
            Query controlChecksumQuery = checksumValidator.generateChecksumQuery(control.getObjectName(), controlColumns);
            controlChecksumQueryContext.setChecksumQuery(formatSql(controlChecksumQuery));

            QueryResult<ChecksumResult> controlChecksum = callAndConsume(
                    () -> getHelperAction().execute(controlChecksumQuery, CONTROL_CHECKSUM, ChecksumResult::fromResultSet),
                    stats -> stats.getQueryStats().map(QueryStats::getQueryId).ifPresent(controlChecksumQueryContext::setChecksumQueryId));
            controlChecksumResult = getOnlyElement(controlChecksum.getResults());

            if (saveSnapshot) {
                String snapshot = ChecksumResult.toJson(controlChecksumResult);

                snapshotQueryConsumer.accept(new SnapshotQuery(getSourceQuery().getSuite(), getSourceQuery().getName(), isExplain, snapshot));
                return new DataMatchResult(
                        DATA,
                        MATCH,
                        Optional.empty(),
                        OptionalLong.empty(),
                        OptionalLong.empty(),
                        ImmutableList.of());
            }
        }
        else if (QUERY_BANK_MODE.equals(runningMode)) {
            controlColumns = testColumns;
            String key = format(VERIFIER_SNAPSHOT_KEY_PATTERN, getSourceQuery().getSuite(), getSourceQuery().getName(), isExplain);
            SnapshotQuery snapshotQuery = snapshotQueries.get(key);
            if (snapshotQuery != null) {
                String snapshotJson = snapshotQuery.getSnapshot();
                controlChecksumResult = ChecksumResult.fromJson(snapshotJson);
            }
            else {
                return new DataMatchResult(DATA, SNAPSHOT_DOES_NOT_EXIST, Optional.empty(), OptionalLong.empty(), OptionalLong.empty(), Collections.emptyList());
            }
        }

        QueryResult<ChecksumResult> testChecksum = callAndConsume(
                () -> getHelperAction().execute(testChecksumQuery, TEST_CHECKSUM, ChecksumResult::fromResultSet),
                stats -> stats.getQueryStats().map(QueryStats::getQueryId).ifPresent(testChecksumQueryContext::setChecksumQueryId));
        ChecksumResult testChecksumResult = getOnlyElement(testChecksum.getResults());

        return match(DATA, checksumValidator, controlColumns, testColumns, controlChecksumResult, testChecksumResult);
    }

    @Override
    protected DeterminismAnalysisDetails analyzeDeterminism(QueryObjectBundle control, DataMatchResult matchResult)
    {
        return determinismAnalyzer.analyze(control, matchResult.getControlChecksum());
    }

    @Override
    protected Optional<String> resolveFailure(
            Optional<QueryObjectBundle> control,
            Optional<QueryObjectBundle> test,
            QueryContext controlQueryContext,
            Optional<DataMatchResult> matchResult,
            Optional<Throwable> throwable)
    {
        if (matchResult.isPresent() && !matchResult.get().isMatched()) {
            checkState(control.isPresent(), "control is missing");
            return failureResolverManager.resolveResultMismatch((DataMatchResult) matchResult.get(), control.get());
        }
        if (throwable.isPresent() && ImmutableList.of(QueryState.SUCCEEDED, QueryState.REUSE).contains(controlQueryContext.getState())) {
            checkState(controlQueryContext.getMainQueryStats().isPresent(), "controlQueryStats is missing");
            return failureResolverManager.resolveException(controlQueryContext.getMainQueryStats().get(), throwable.get(), test);
        }
        return Optional.empty();
    }
}
