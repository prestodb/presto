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
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.verifier.checksum.ChecksumResult;
import com.facebook.presto.verifier.checksum.ChecksumValidator;
import com.facebook.presto.verifier.prestoaction.QueryActions;
import com.facebook.presto.verifier.prestoaction.SqlExceptionClassifier;
import com.facebook.presto.verifier.resolver.FailureResolverManager;
import com.facebook.presto.verifier.rewrite.QueryRewriter;

import java.util.List;

import static com.facebook.presto.verifier.framework.DataVerificationUtil.getColumns;
import static com.facebook.presto.verifier.framework.DataVerificationUtil.match;
import static com.facebook.presto.verifier.framework.QueryStage.CONTROL_CHECKSUM;
import static com.facebook.presto.verifier.framework.QueryStage.TEST_CHECKSUM;
import static com.facebook.presto.verifier.framework.VerifierUtil.callAndConsume;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;

public class DataVerification
        extends AbstractVerification
{
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
        super(queryActions, sourceQuery, queryRewriter, determinismAnalyzer, failureResolverManager, exceptionClassifier, verificationContext, verifierConfig);
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.checksumValidator = requireNonNull(checksumValidator, "checksumValidator is null");
    }

    @Override
    public MatchResult verify(QueryBundle control, QueryBundle test, ChecksumQueryContext controlContext, ChecksumQueryContext testContext)
    {
        List<Column> controlColumns = getColumns(getHelperAction(), typeManager, control.getTableName());
        List<Column> testColumns = getColumns(getHelperAction(), typeManager, test.getTableName());

        Query controlChecksumQuery = checksumValidator.generateChecksumQuery(control.getTableName(), controlColumns);
        Query testChecksumQuery = checksumValidator.generateChecksumQuery(test.getTableName(), testColumns);

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
}
