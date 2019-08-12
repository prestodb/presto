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

import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.ShowColumns;
import com.facebook.presto.verifier.checksum.ChecksumResult;
import com.facebook.presto.verifier.checksum.ChecksumValidator;
import com.facebook.presto.verifier.checksum.ColumnMatchResult;
import com.facebook.presto.verifier.framework.MatchResult.MatchType;
import com.facebook.presto.verifier.resolver.FailureResolver;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static com.facebook.presto.verifier.framework.ClusterType.CONTROL;
import static com.facebook.presto.verifier.framework.MatchResult.MatchType.COLUMN_MISMATCH;
import static com.facebook.presto.verifier.framework.MatchResult.MatchType.MATCH;
import static com.facebook.presto.verifier.framework.MatchResult.MatchType.ROW_COUNT_MISMATCH;
import static com.facebook.presto.verifier.framework.MatchResult.MatchType.SCHEMA_MISMATCH;
import static com.facebook.presto.verifier.framework.QueryStage.CHECKSUM;
import static com.facebook.presto.verifier.framework.QueryStage.DESCRIBE;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;

public class DataVerification
        extends AbstractVerification
{
    private final ChecksumValidator checksumValidator;

    public DataVerification(
            VerificationResubmitter verificationResubmitter,
            PrestoAction prestoAction,
            SourceQuery sourceQuery,
            QueryRewriter queryRewriter,
            List<FailureResolver> failureResolvers,
            VerificationContext verificationContext,
            VerifierConfig verifierConfig,
            ChecksumValidator checksumValidator)
    {
        super(verificationResubmitter, prestoAction, sourceQuery, queryRewriter, failureResolvers, verificationContext, verifierConfig);
        this.checksumValidator = requireNonNull(checksumValidator, "checksumValidator is null");
    }

    @Override
    public VerificationResult verify(QueryBundle control, QueryBundle test)
    {
        List<Column> controlColumns = getColumns(control.getTableName());
        List<Column> testColumns = getColumns(test.getTableName());
        ChecksumQueryAndResult controlChecksum = computeChecksum(control, controlColumns);
        ChecksumQueryAndResult testChecksum = computeChecksum(test, testColumns);
        return new VerificationResult(
                controlChecksum.getQueryId(),
                testChecksum.getQueryId(),
                formatSql(controlChecksum.getQuery()),
                formatSql(testChecksum.getQuery()),
                match(
                        controlColumns,
                        testColumns,
                        controlChecksum.getResult(),
                        testChecksum.getResult()));
    }

    @Override
    protected Optional<Boolean> isDeterministic(QueryBundle control, ChecksumResult firstChecksum)
    {
        List<Column> columns = getColumns(control.getTableName());

        QueryBundle secondRun = null;
        QueryBundle thirdRun = null;
        try {
            secondRun = getQueryRewriter().rewriteQuery(getSourceQuery().getControlQuery(), CONTROL);
            setupAndRun(secondRun, true);
            if (!match(columns, columns, firstChecksum, computeChecksum(secondRun, columns).getResult()).isMatched()) {
                return Optional.of(false);
            }

            thirdRun = getQueryRewriter().rewriteQuery(getSourceQuery().getControlQuery(), CONTROL);
            setupAndRun(thirdRun, true);
            if (!match(columns, columns, firstChecksum, computeChecksum(thirdRun, columns).getResult()).isMatched()) {
                return Optional.of(false);
            }

            return Optional.of(true);
        }
        catch (Throwable t) {
            return Optional.empty();
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

    private List<Column> getColumns(QualifiedName tableName)
    {
        return getPrestoAction()
                .execute(new ShowColumns(tableName), DESCRIBE, Column::fromResultSet)
                .getResults();
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
