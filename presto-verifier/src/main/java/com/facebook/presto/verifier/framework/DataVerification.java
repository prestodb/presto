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

import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.ShowColumns;
import com.facebook.presto.verifier.checksum.ChecksumResult;
import com.facebook.presto.verifier.checksum.ChecksumValidator;
import com.facebook.presto.verifier.checksum.ColumnMatchResult;
import com.facebook.presto.verifier.framework.MatchResult.MatchType;
import com.facebook.presto.verifier.prestoaction.PrestoAction;
import com.facebook.presto.verifier.resolver.FailureResolverManager;
import com.facebook.presto.verifier.rewrite.QueryRewriter;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static com.facebook.presto.verifier.framework.ClusterType.CONTROL;
import static com.facebook.presto.verifier.framework.DeterminismAnalysis.ANALYSIS_FAILED;
import static com.facebook.presto.verifier.framework.DeterminismAnalysis.ANALYSIS_FAILED_DATA_CHANGED;
import static com.facebook.presto.verifier.framework.DeterminismAnalysis.ANALYSIS_FAILED_INCONSISTENT_SCHEMA;
import static com.facebook.presto.verifier.framework.DeterminismAnalysis.ANALYSIS_FAILED_QUERY_FAILURE;
import static com.facebook.presto.verifier.framework.DeterminismAnalysis.DETERMINISTIC;
import static com.facebook.presto.verifier.framework.DeterminismAnalysis.NON_DETERMINISTIC_COLUMNS;
import static com.facebook.presto.verifier.framework.DeterminismAnalysis.NON_DETERMINISTIC_LIMIT_CLAUSE;
import static com.facebook.presto.verifier.framework.DeterminismAnalysis.NON_DETERMINISTIC_ROW_COUNT;
import static com.facebook.presto.verifier.framework.MatchResult.MatchType.COLUMN_MISMATCH;
import static com.facebook.presto.verifier.framework.MatchResult.MatchType.MATCH;
import static com.facebook.presto.verifier.framework.MatchResult.MatchType.ROW_COUNT_MISMATCH;
import static com.facebook.presto.verifier.framework.MatchResult.MatchType.SCHEMA_MISMATCH;
import static com.facebook.presto.verifier.framework.QueryStage.CHECKSUM;
import static com.facebook.presto.verifier.framework.QueryStage.DESCRIBE;
import static com.facebook.presto.verifier.framework.VerifierUtil.callWithQueryStatsConsumer;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class DataVerification
        extends AbstractVerification
{
    private final TypeManager typeManager;
    private final ChecksumValidator checksumValidator;
    private final LimitQueryDeterminismAnalyzer limitQueryDeterminismAnalyzer;

    public DataVerification(
            VerificationResubmitter verificationResubmitter,
            PrestoAction prestoAction,
            SourceQuery sourceQuery,
            QueryRewriter queryRewriter,
            FailureResolverManager failureResolverManager,
            VerificationContext verificationContext,
            VerifierConfig verifierConfig,
            TypeManager typeManager,
            ChecksumValidator checksumValidator,
            LimitQueryDeterminismAnalyzer limitQueryDeterminismAnalyzer)
    {
        super(verificationResubmitter, prestoAction, sourceQuery, queryRewriter, failureResolverManager, verificationContext, verifierConfig);
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.checksumValidator = requireNonNull(checksumValidator, "checksumValidator is null");
        this.limitQueryDeterminismAnalyzer = requireNonNull(limitQueryDeterminismAnalyzer, "limitQueryDeterminismAnalyzer is null");
    }

    @Override
    public MatchResult verify(QueryBundle control, QueryBundle test)
    {
        List<Column> controlColumns = getColumns(control.getTableName());
        List<Column> testColumns = getColumns(test.getTableName());

        Query controlChecksumQuery = checksumValidator.generateChecksumQuery(control.getTableName(), controlColumns);
        Query testChecksumQuery = checksumValidator.generateChecksumQuery(test.getTableName(), testColumns);

        getVerificationContext().setControlChecksumQuery(formatSql(controlChecksumQuery));
        getVerificationContext().setTestChecksumQuery(formatSql(testChecksumQuery));

        QueryResult<ChecksumResult> controlChecksum = callWithQueryStatsConsumer(
                () -> executeChecksumQuery(controlChecksumQuery),
                stats -> getVerificationContext().setControlChecksumQueryId(stats.getQueryId()));
        QueryResult<ChecksumResult> testChecksum = callWithQueryStatsConsumer(
                () -> executeChecksumQuery(testChecksumQuery),
                stats -> getVerificationContext().setTestChecksumQueryId(stats.getQueryId()));

        return match(controlColumns, testColumns, getOnlyElement(controlChecksum.getResults()), getOnlyElement(testChecksum.getResults()));
    }

    @Override
    protected DeterminismAnalysis analyzeDeterminism(QueryBundle control, ChecksumResult firstChecksum)
    {
        List<Column> columns = getColumns(control.getTableName());

        QueryBundle secondRun = null;
        QueryBundle thirdRun = null;
        try {
            secondRun = getQueryRewriter().rewriteQuery(getSourceQuery().getControlQuery(), CONTROL);
            setupAndRun(secondRun, true);
            DeterminismAnalysis determinismAnalysis = matchResultToDeterminism(match(columns, columns, firstChecksum, computeChecksum(secondRun, columns).getResult()));
            if (determinismAnalysis != DETERMINISTIC) {
                return determinismAnalysis;
            }

            thirdRun = getQueryRewriter().rewriteQuery(getSourceQuery().getControlQuery(), CONTROL);
            setupAndRun(thirdRun, true);
            determinismAnalysis = matchResultToDeterminism(match(columns, columns, firstChecksum, computeChecksum(thirdRun, columns).getResult()));
            if (determinismAnalysis != DETERMINISTIC) {
                return determinismAnalysis;
            }

            LimitQueryDeterminismAnalysis analysis = limitQueryDeterminismAnalyzer.analyze(control, firstChecksum.getRowCount());
            switch (analysis) {
                case NON_DETERMINISTIC:
                    return NON_DETERMINISTIC_LIMIT_CLAUSE;
                case NOT_RUN:
                case DETERMINISTIC:
                    return DETERMINISTIC;
                case FAILED_DATA_CHANGED:
                    return ANALYSIS_FAILED_DATA_CHANGED;
                default:
                    throw new IllegalArgumentException(format("Invalid analysis: %s", analysis));
            }
        }
        catch (QueryException qe) {
            return ANALYSIS_FAILED_QUERY_FAILURE;
        }
        catch (Throwable t) {
            return ANALYSIS_FAILED;
        }
        finally {
            teardownSafely(secondRun);
            teardownSafely(thirdRun);
        }
    }

    private MatchResult match(
            List<Column> controlColumns,
            List<Column> testColumns,
            ChecksumResult controlChecksum,
            ChecksumResult testChecksum)
    {
        if (!controlColumns.equals(testColumns)) {
            return new MatchResult(
                    SCHEMA_MISMATCH,
                    Optional.empty(),
                    OptionalLong.empty(),
                    OptionalLong.empty(),
                    ImmutableMap.of());
        }

        OptionalLong controlRowCount = OptionalLong.of(controlChecksum.getRowCount());
        OptionalLong testRowCount = OptionalLong.of(testChecksum.getRowCount());

        MatchType matchType;
        Map<Column, ColumnMatchResult> mismatchedColumns;
        if (controlChecksum.getRowCount() != testChecksum.getRowCount()) {
            mismatchedColumns = ImmutableMap.of();
            matchType = ROW_COUNT_MISMATCH;
        }
        else {
            mismatchedColumns = checksumValidator.getMismatchedColumns(controlColumns, controlChecksum, testChecksum);
            matchType = mismatchedColumns.isEmpty() ? MATCH : COLUMN_MISMATCH;
        }
        return new MatchResult(
                matchType,
                Optional.of(controlChecksum),
                controlRowCount,
                testRowCount,
                mismatchedColumns);
    }

    private DeterminismAnalysis matchResultToDeterminism(MatchResult matchResult)
    {
        switch (matchResult.getMatchType()) {
            case MATCH:
                return DETERMINISTIC;
            case SCHEMA_MISMATCH:
                return ANALYSIS_FAILED_INCONSISTENT_SCHEMA;
            case ROW_COUNT_MISMATCH:
                return NON_DETERMINISTIC_ROW_COUNT;
            case COLUMN_MISMATCH:
                return NON_DETERMINISTIC_COLUMNS;
            default:
                throw new IllegalArgumentException(format("Invalid MatchResult: %s", matchResult));
        }
    }

    private List<Column> getColumns(QualifiedName tableName)
    {
        return getPrestoAction()
                .execute(new ShowColumns(tableName), DESCRIBE, resultSet -> Column.fromResultSet(typeManager, resultSet))
                .getResults();
    }

    private QueryResult<ChecksumResult> executeChecksumQuery(Query query)
    {
        return getPrestoAction().execute(query, CHECKSUM, ChecksumResult::fromResultSet);
    }

    private ChecksumQueryAndResult computeChecksum(QueryBundle bundle, List<Column> columns)
    {
        Query checksumQuery = checksumValidator.generateChecksumQuery(bundle.getTableName(), columns);
        QueryResult<ChecksumResult> queryResult = getPrestoAction().execute(
                checksumQuery,
                CHECKSUM,
                ChecksumResult::fromResultSet);
        return new ChecksumQueryAndResult(
                queryResult.getQueryStats().getQueryId(),
                checksumQuery,
                getOnlyElement(queryResult.getResults()));
    }

    private class ChecksumQueryAndResult
    {
        private final String queryId;
        private final Query query;
        private final ChecksumResult result;

        public ChecksumQueryAndResult(String queryId, Query query, ChecksumResult result)
        {
            this.queryId = requireNonNull(queryId, "queryId is null");
            this.query = requireNonNull(query, "query is null");
            this.result = requireNonNull(result, "result is null");
        }

        public String getQueryId()
        {
            return queryId;
        }

        public Query getQuery()
        {
            return query;
        }

        public ChecksumResult getResult()
        {
            return result;
        }
    }
}
