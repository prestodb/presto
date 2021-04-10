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

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.verifier.framework.DataVerificationUtil.getColumns;
import static com.facebook.presto.verifier.framework.DataVerificationUtil.match;
import static com.facebook.presto.verifier.framework.QueryStage.CONTROL_CHECKSUM;
import static com.facebook.presto.verifier.framework.QueryStage.TEST_CHECKSUM;
import static com.facebook.presto.verifier.framework.VerifierUtil.callAndConsume;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;

public class DataVerification
        extends AbstractVerification<QueryObjectBundle, DataMatchResult, Void>
{
    private final QueryRewriter queryRewriter;
    private final DeterminismAnalyzer determinismAnalyzer;
    private final FailureResolverManager failureResolverManager;
    private final TypeManager typeManager;
    private final ChecksumValidator checksumValidator;

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
            ChecksumValidator checksumValidator)
    {
        super(queryActions, sourceQuery, exceptionClassifier, verificationContext, Optional.empty(), verifierConfig);
        this.queryRewriter = requireNonNull(queryRewriter, "queryRewriter is null");
        this.determinismAnalyzer = requireNonNull(determinismAnalyzer, "determinismAnalyzer is null");
        this.failureResolverManager = requireNonNull(failureResolverManager, "failureResolverManager is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.checksumValidator = requireNonNull(checksumValidator, "checksumValidator is null");
    }

    @Override
    protected QueryObjectBundle getQueryRewrite(ClusterType clusterType)
    {
        return queryRewriter.rewriteQuery(getSourceQuery().getQuery(clusterType), clusterType);
    }

    @Override
    protected void updateQueryInfoWithQueryBundle(QueryInfo.Builder queryInfo, Optional<QueryObjectBundle> queryBundle)
    {
        super.updateQueryInfoWithQueryBundle(queryInfo, queryBundle);
        queryInfo.setOutputTableName(queryBundle.map(QueryObjectBundle::getObjectName).map(QualifiedName::toString));
    }

    @Override
    public DataMatchResult verify(
            QueryObjectBundle control,
            QueryObjectBundle test,
            Optional<QueryResult<Void>> controlQueryResult,
            Optional<QueryResult<Void>> testQueryResult,
            ChecksumQueryContext controlContext,
            ChecksumQueryContext testContext)
    {
        List<Column> controlColumns = getColumns(getHelperAction(), typeManager, control.getObjectName());
        List<Column> testColumns = getColumns(getHelperAction(), typeManager, test.getObjectName());

        Query controlChecksumQuery = checksumValidator.generateChecksumQuery(control.getObjectName(), controlColumns);
        Query testChecksumQuery = checksumValidator.generateChecksumQuery(test.getObjectName(), testColumns);

        controlContext.setChecksumQuery(formatSql(controlChecksumQuery));
        testContext.setChecksumQuery(formatSql(testChecksumQuery));

        QueryResult<ChecksumResult> controlChecksum = callAndConsume(
                () -> getHelperAction().execute(controlChecksumQuery, CONTROL_CHECKSUM, ChecksumResult::fromResultSet),
                stats -> stats.getQueryStats().map(QueryStats::getQueryId).ifPresent(controlContext::setChecksumQueryId));
        QueryResult<ChecksumResult> testChecksum = callAndConsume(
                () -> getHelperAction().execute(testChecksumQuery, TEST_CHECKSUM, ChecksumResult::fromResultSet),
                stats -> stats.getQueryStats().map(QueryStats::getQueryId).ifPresent(testContext::setChecksumQueryId));

        return match(checksumValidator, controlColumns, testColumns, getOnlyElement(controlChecksum.getResults()), getOnlyElement(testChecksum.getResults()));
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
        if (throwable.isPresent() && controlQueryContext.getState() == QueryState.SUCCEEDED) {
            checkState(controlQueryContext.getMainQueryStats().isPresent(), "controlQueryStats is missing");
            return failureResolverManager.resolveException(controlQueryContext.getMainQueryStats().get(), throwable.get(), test);
        }
        return Optional.empty();
    }
}
