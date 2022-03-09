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
import com.facebook.presto.sql.parser.ParsingException;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.verifier.event.DeterminismAnalysisDetails;
import com.facebook.presto.verifier.prestoaction.PrestoAction.ResultSetConverter;
import com.facebook.presto.verifier.prestoaction.QueryActions;
import com.facebook.presto.verifier.prestoaction.SqlExceptionClassifier;

import java.util.Optional;

import static com.facebook.presto.verifier.framework.DdlMatchResult.MatchType.CONTROL_NOT_PARSABLE;
import static com.facebook.presto.verifier.framework.DdlMatchResult.MatchType.MATCH;
import static com.facebook.presto.verifier.framework.DdlMatchResult.MatchType.MISMATCH;
import static com.facebook.presto.verifier.framework.DdlMatchResult.MatchType.TEST_NOT_PARSABLE;
import static com.facebook.presto.verifier.framework.QueryStage.CONTROL_CHECKSUM;
import static com.facebook.presto.verifier.framework.QueryStage.TEST_CHECKSUM;
import static com.facebook.presto.verifier.framework.VerifierUtil.PARSING_OPTIONS;
import static com.facebook.presto.verifier.framework.VerifierUtil.callAndConsume;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;

public abstract class DdlVerification<S extends Statement>
        extends AbstractVerification<QueryObjectBundle, DdlMatchResult, Void>
{
    private final SqlParser sqlParser;
    private final ResultSetConverter<String> checksumConverter;

    public DdlVerification(
            SqlParser sqlParser,
            QueryActions queryActions,
            SourceQuery sourceQuery,
            SqlExceptionClassifier exceptionClassifier,
            VerificationContext verificationContext,
            VerifierConfig verifierConfig,
            ResultSetConverter<String> checksumConverter)
    {
        super(queryActions, sourceQuery, exceptionClassifier, verificationContext, Optional.empty(), verifierConfig);
        this.sqlParser = requireNonNull(sqlParser, "sqlParser");
        this.checksumConverter = requireNonNull(checksumConverter, "checksumConverter is null");
    }

    protected abstract Statement getChecksumQuery(QueryObjectBundle queryBundle);

    protected abstract boolean match(S controlObject, S testObject, QueryObjectBundle control, QueryObjectBundle test);

    @Override
    @SuppressWarnings("unchecked")
    protected DdlMatchResult verify(
            QueryObjectBundle control,
            QueryObjectBundle test,
            Optional<QueryResult<Void>> controlQueryResult,
            Optional<QueryResult<Void>> testQueryResult,
            ChecksumQueryContext controlChecksumQueryContext,
            ChecksumQueryContext testChecksumQueryContext)
    {
        Statement controlChecksumQuery = getChecksumQuery(control);
        Statement testChecksumQuery = getChecksumQuery(test);

        controlChecksumQueryContext.setChecksumQuery(formatSql(controlChecksumQuery));
        testChecksumQueryContext.setChecksumQuery(formatSql(testChecksumQuery));

        String controlChecksum = getOnlyElement(callAndConsume(
                () -> getHelperAction().execute(controlChecksumQuery, CONTROL_CHECKSUM, checksumConverter),
                stats -> stats.getQueryStats().map(QueryStats::getQueryId).ifPresent(controlChecksumQueryContext::setChecksumQueryId)).getResults());
        String testChecksum = getOnlyElement(callAndConsume(
                () -> getHelperAction().execute(testChecksumQuery, TEST_CHECKSUM, checksumConverter),
                stats -> stats.getQueryStats().map(QueryStats::getQueryId).ifPresent(testChecksumQueryContext::setChecksumQueryId)).getResults());

        S controlObject;
        S testObject;

        try {
            controlObject = (S) sqlParser.createStatement(controlChecksum, PARSING_OPTIONS);
        }
        catch (ParsingException e) {
            return new DdlMatchResult(CONTROL_NOT_PARSABLE, Optional.of(e), controlChecksum, testChecksum);
        }

        try {
            testObject = (S) sqlParser.createStatement(testChecksum, PARSING_OPTIONS);
        }
        catch (ParsingException e) {
            return new DdlMatchResult(TEST_NOT_PARSABLE, Optional.of(e), controlChecksum, testChecksum);
        }

        return new DdlMatchResult(
                match(controlObject, testObject, control, test) ? MATCH : MISMATCH,
                Optional.empty(),
                controlChecksum,
                testChecksum);
    }

    @Override
    protected DeterminismAnalysisDetails analyzeDeterminism(QueryObjectBundle control, DdlMatchResult matchResult)
    {
        throw new UnsupportedOperationException("analyzeDeterminism is not supported for DdlVerification");
    }

    @Override
    protected Optional<String> resolveFailure(Optional<QueryObjectBundle> control, Optional<QueryObjectBundle> test, QueryContext controlQueryContext, Optional<DdlMatchResult> matchResult, Optional<Throwable> throwable)
    {
        return Optional.empty();
    }
}
