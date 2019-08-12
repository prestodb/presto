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

import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.verifier.checksum.ChecksumValidator;
import com.facebook.presto.verifier.resolver.FailureResolver;

import javax.inject.Inject;

import java.util.List;

import static com.facebook.presto.verifier.framework.VerifierUtil.PARSING_OPTIONS;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class VerificationFactory
{
    private final SqlParser sqlParser;
    private final PrestoActionFactory prestoActionFactory;
    private final QueryRewriterFactory queryRewriterFactory;
    private final ChecksumValidator checksumValidator;
    private final List<FailureResolver> failureResolvers;
    private final VerifierConfig verifierConfig;

    @Inject
    public VerificationFactory(
            SqlParser sqlParser,
            PrestoActionFactory prestoActionFactory,
            QueryRewriterFactory queryRewriterFactory,
            ChecksumValidator checksumValidator,
            List<FailureResolver> failureResolvers,
            VerifierConfig verifierConfig)
    {
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
        this.prestoActionFactory = requireNonNull(prestoActionFactory, "prestoActionFactory is null");
        this.queryRewriterFactory = requireNonNull(queryRewriterFactory, "queryRewriterFactory is null");
        this.checksumValidator = requireNonNull(checksumValidator, "checksumValidator is null");
        this.failureResolvers = requireNonNull(failureResolvers, "failureResolvers is null");
        this.verifierConfig = requireNonNull(verifierConfig, "config is null");
    }

    public Verification get(VerificationResubmitter verificationResubmitter, SourceQuery sourceQuery)
    {
        QueryType queryType = QueryType.of(sqlParser.createStatement(sourceQuery.getControlQuery(), PARSING_OPTIONS));
        switch (queryType.getCategory()) {
            case DATA_PRODUCING:
                VerificationContext verificationContext = new VerificationContext();
                PrestoAction prestoAction = prestoActionFactory.create(
                        sourceQuery.getControlConfiguration(),
                        sourceQuery.getTestConfiguration(),
                        verificationContext);
                QueryRewriter queryRewriter = queryRewriterFactory.create(prestoAction);
                return new DataVerification(
                        verificationResubmitter,
                        prestoAction,
                        sourceQuery,
                        queryRewriter,
                        failureResolvers,
                        verificationContext,
                        verifierConfig,
                        checksumValidator);
            default:
                throw new IllegalStateException(format("Unsupported query type: %s", queryType));
        }
    }
}
