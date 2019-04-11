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
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.ShowColumns;
import com.facebook.presto.verifier.checksum.ChecksumResult;
import com.facebook.presto.verifier.checksum.ChecksumValidator;
import com.facebook.presto.verifier.checksum.ColumnMatchResult;
import com.facebook.presto.verifier.framework.MatchResult.MatchType;
import com.facebook.presto.verifier.framework.QueryOrigin.TargetCluster;
import com.facebook.presto.verifier.resolver.FailureResolver;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static com.facebook.presto.verifier.framework.MatchResult.MatchType.COLUMN_MISMATCH;
import static com.facebook.presto.verifier.framework.MatchResult.MatchType.MATCH;
import static com.facebook.presto.verifier.framework.MatchResult.MatchType.ROW_COUNT_MISMATCH;
import static com.facebook.presto.verifier.framework.MatchResult.MatchType.SCHEMA_MISMATCH;
import static com.facebook.presto.verifier.framework.QueryOrigin.TargetCluster.CONTROL;
import static com.facebook.presto.verifier.framework.QueryOrigin.TargetCluster.TEST;
import static com.facebook.presto.verifier.framework.QueryOrigin.forChecksum;
import static com.facebook.presto.verifier.framework.QueryOrigin.forDescribe;
import static com.facebook.presto.verifier.framework.QueryOrigin.forMain;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;

public class DataVerification
        extends AbstractVerification
{
    private final ChecksumValidator checksumValidator;

    public DataVerification(
            PrestoAction prestoAction,
            SourceQuery sourceQuery,
            QueryRewriter queryRewriter,
            List<FailureResolver> failureResolvers,
            VerifierConfig config,
            ChecksumValidator checksumValidator)
    {
        super(prestoAction, sourceQuery, queryRewriter, failureResolvers, config);
        this.checksumValidator = requireNonNull(checksumValidator, "checksumValidator is null");
    }

    @Override
    public VerificationResult verify(QueryBundle control, QueryBundle test)
    {
        setQueryStats(setupAndRun(control, CONTROL), CONTROL);
        setQueryStats(setupAndRun(test, TEST), TEST);
        List<Column> controlColumns = getColumns(control.getTableName(), CONTROL);
        List<Column> testColumns = getColumns(test.getTableName(), TEST);
        ChecksumQueryAndResult controlChecksum = computeChecksum(control, controlColumns, CONTROL);
        ChecksumQueryAndResult testChecksum = computeChecksum(test, testColumns, TEST);
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
        List<Column> columns = getColumns(control.getTableName(), CONTROL);

        QueryBundle secondRun = null;
        QueryBundle thirdRun = null;
        try {
            secondRun = getQueryRewriter().rewriteQuery(getSourceQuery().getControlQuery(), CONTROL, getConfiguration(CONTROL), getVerificationContext());
            setupAndRun(secondRun, CONTROL);
            if (!match(columns, columns, firstChecksum, computeChecksum(secondRun, columns, CONTROL).getResult()).isMatched()) {
                return Optional.of(false);
            }

            thirdRun = getQueryRewriter().rewriteQuery(getSourceQuery().getControlQuery(), CONTROL, getConfiguration(CONTROL), getVerificationContext());
            setupAndRun(thirdRun, CONTROL);
            if (!match(columns, columns, firstChecksum, computeChecksum(thirdRun, columns, CONTROL).getResult()).isMatched()) {
                return Optional.of(false);
            }

            return Optional.of(true);
        }
        catch (Throwable t) {
            return Optional.empty();
        }
        finally {
            if (secondRun != null) {
                teardownSafely(secondRun, CONTROL);
            }
            if (thirdRun != null) {
                teardownSafely(thirdRun, CONTROL);
            }
        }
    }

    private QueryStats setupAndRun(QueryBundle control, TargetCluster cluster)
    {
        setup(control, cluster);
        return getPrestoAction().execute(control.getQuery(), getConfiguration(cluster), forMain(cluster), getVerificationContext());
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

    private List<Column> getColumns(QualifiedName tableName, TargetCluster cluster)
    {
        return getPrestoAction()
                .execute(
                        new ShowColumns(tableName),
                        getConfiguration(cluster),
                        forDescribe(),
                        getVerificationContext(),
                        Column::fromResultSet)
                .getResults();
    }

    private ChecksumQueryAndResult computeChecksum(QueryBundle bundle, List<Column> columns, TargetCluster cluster)
    {
        Query checksumQuery = checksumValidator.generateChecksumQuery(bundle.getTableName(), columns);
        QueryResult<ChecksumResult> queryResult = getPrestoAction().execute(
                checksumQuery,
                getConfiguration(cluster),
                forChecksum(),
                getVerificationContext(),
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
