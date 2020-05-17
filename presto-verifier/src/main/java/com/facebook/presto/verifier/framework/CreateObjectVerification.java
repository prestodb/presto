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
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.verifier.checksum.ChecksumResult;
import com.facebook.presto.verifier.checksum.ChecksumValidator;
import com.facebook.presto.verifier.prestoaction.PrestoAction;
import com.facebook.presto.verifier.resolver.FailureResolverManager;
import com.facebook.presto.verifier.rewrite.QueryRewriter;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

import static com.facebook.presto.verifier.framework.MatchResult.MatchType.SCHEMA_MISMATCH;
import static com.facebook.presto.verifier.framework.QueryStage.CONTROL_CHECKSUM;
import static com.facebook.presto.verifier.framework.QueryStage.TEST_CHECKSUM;
import static com.facebook.presto.verifier.framework.VerifierUtil.callAndConsume;
import static com.facebook.presto.verifier.framework.VerifierUtil.getColumns;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;

public abstract class CreateObjectVerification
        extends AbstractVerification
{
    private final TypeManager typeManager;
    private final ChecksumValidator checksumValidator;

    public CreateObjectVerification(
            PrestoAction prestoAction,
            SourceQuery sourceQuery,
            QueryRewriter queryRewriter,
            DeterminismAnalyzer determinismAnalyzer,
            FailureResolverManager failureResolverManager,
            VerificationContext verificationContext,
            VerifierConfig verifierConfig,
            TypeManager typeManager,
            ChecksumValidator checksumValidator)
    {
        super(prestoAction, sourceQuery, queryRewriter, determinismAnalyzer, failureResolverManager, verificationContext, verifierConfig);
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.checksumValidator = requireNonNull(checksumValidator, "checksumValidator is null");
    }

    @Override
    protected MatchResult verify(QueryBundle control, QueryBundle test, ChecksumQueryContext controlContext, ChecksumQueryContext testContext)
    {
        Statement controlChecksumQuery = getShowObjectQuery(control);
        Statement testChecksumQuery = getShowObjectQuery(test);

        controlContext.setChecksumQuery(formatSql(controlChecksumQuery));
        testContext.setChecksumQuery(formatSql(testChecksumQuery));

        QueryResult<ChecksumResult> controlResults = callAndConsume(
                () -> getPrestoAction().execute(controlChecksumQuery, CONTROL_CHECKSUM, ChecksumResult::fromResultSet),
                stats -> controlContext.setChecksumQueryId(stats.getQueryId()));
        QueryResult<ChecksumResult> testResults = callAndConsume(
                () -> getPrestoAction().execute(testChecksumQuery, TEST_CHECKSUM, ChecksumResult::fromResultSet),
                stats -> testContext.setChecksumQueryId(stats.getQueryId()));


    }

    protected abstract Statement getShowObjectQuery(QueryBundle bundle);

    private String
}
