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
import com.facebook.presto.verifier.framework.QueryOrigin.QueryGroup;
import com.facebook.presto.verifier.framework.VerificationResult.MatchType;
import com.facebook.presto.verifier.resolver.FailureResolver;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static com.facebook.presto.verifier.framework.QueryOrigin.QueryGroup.CONTROL;
import static com.facebook.presto.verifier.framework.QueryOrigin.QueryGroup.TEST;
import static com.facebook.presto.verifier.framework.QueryOrigin.QueryStage.CHECKSUM;
import static com.facebook.presto.verifier.framework.QueryOrigin.QueryStage.DESCRIBE;
import static com.facebook.presto.verifier.framework.QueryOrigin.QueryStage.MAIN;
import static com.facebook.presto.verifier.framework.VerificationResult.MatchType.COLUMN_MISMATCH;
import static com.facebook.presto.verifier.framework.VerificationResult.MatchType.MATCH;
import static com.facebook.presto.verifier.framework.VerificationResult.MatchType.ROW_COUNT_MISMATCH;
import static com.facebook.presto.verifier.framework.VerificationResult.MatchType.SCHEMA_MISMATCH;
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
        return produceResult(
                controlColumns,
                testColumns,
                computeChecksum(control, controlColumns, CONTROL),
                computeChecksum(test, testColumns, TEST));
    }

    @Override
    protected Optional<Boolean> isDeterministic(QueryBundle control, ChecksumResult firstChecksumResult)
    {
        List<Column> columns = getColumns(control.getTableName(), CONTROL);
        ChecksumQueryAndResult firstChecksum = new ChecksumQueryAndResult(
                checksumValidator.generateChecksumQuery(control.getTableName(), columns),
                firstChecksumResult);

        QueryBundle secondRun = null;
        QueryBundle thirdRun = null;
        try {
            secondRun = getQueryRewriter().rewriteQuery(getSourceQuery().getControlQuery(), CONTROL, getConfiguration(CONTROL), getVerificationContext());
            setupAndRun(secondRun, CONTROL);
            if (!produceResult(columns, columns, firstChecksum, computeChecksum(secondRun, columns, CONTROL)).isMatched()) {
                return Optional.of(false);
            }

            thirdRun = getQueryRewriter().rewriteQuery(getSourceQuery().getControlQuery(), CONTROL, getConfiguration(CONTROL), getVerificationContext());
            setupAndRun(thirdRun, CONTROL);
            if (!produceResult(columns, columns, firstChecksum, computeChecksum(thirdRun, columns, CONTROL)).isMatched()) {
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

    private QueryStats setupAndRun(QueryBundle control, QueryGroup group)
    {
        setup(control, group);
        return getPrestoAction().execute(control.getQuery(), getConfiguration(group), new QueryOrigin(group, MAIN), getVerificationContext());
    }

    private VerificationResult produceResult(
            List<Column> controlColumns,
            List<Column> testColumns,
            ChecksumQueryAndResult controlChecksum,
            ChecksumQueryAndResult testChecksum)
    {
        if (!controlColumns.equals(testColumns)) {
            return new VerificationResult(
                    SCHEMA_MISMATCH,
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    OptionalLong.empty(),
                    OptionalLong.empty(),
                    ImmutableMap.of());
        }

        Optional<String> controlChecksumQuery = Optional.of(formatSql(controlChecksum.getQuery()));
        Optional<String> testChecksumQuery = Optional.of(formatSql(controlChecksum.getQuery()));
        OptionalLong controlRowCount = OptionalLong.of(controlChecksum.getResult().getRowCount());
        OptionalLong testRowCount = OptionalLong.of(testChecksum.getResult().getRowCount());

        MatchType matchType;
        Map<Column, ColumnMatchResult> mismatchedColumns;
        if (controlChecksum.getResult().getRowCount() != testChecksum.getResult().getRowCount()) {
            mismatchedColumns = ImmutableMap.of();
            matchType = ROW_COUNT_MISMATCH;
        }
        else {
            mismatchedColumns = checksumValidator.getMismatchedColumns(controlColumns, controlChecksum.getResult(), testChecksum.getResult());
            matchType = mismatchedColumns.isEmpty() ? MATCH : COLUMN_MISMATCH;
        }
        return new VerificationResult(
                matchType,
                controlChecksumQuery,
                testChecksumQuery,
                Optional.of(controlChecksum.getResult()),
                controlRowCount,
                testRowCount,
                mismatchedColumns);
    }

    private List<Column> getColumns(QualifiedName tableName, QueryGroup group)
    {
        return getPrestoAction()
                .execute(
                        new ShowColumns(tableName),
                        getConfiguration(group),
                        new QueryOrigin(group, DESCRIBE),
                        getVerificationContext(),
                        Column::fromResultSet)
                .getResults();
    }

    private ChecksumQueryAndResult computeChecksum(QueryBundle bundle, List<Column> columns, QueryGroup group)
    {
        Query checksumQuery = checksumValidator.generateChecksumQuery(bundle.getTableName(), columns);
        return new ChecksumQueryAndResult(
                checksumQuery,
                getOnlyElement(getPrestoAction()
                        .execute(
                                checksumQuery,
                                getConfiguration(group),
                                new QueryOrigin(group, CHECKSUM),
                                getVerificationContext(),
                                ChecksumResult::fromResultSet)
                        .getResults()));
    }

    private class ChecksumQueryAndResult
    {
        private final Query query;
        private final ChecksumResult result;

        public ChecksumQueryAndResult(Query query, ChecksumResult result)
        {
            this.query = requireNonNull(query, "query is null");
            this.result = requireNonNull(result, "result is null");
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
