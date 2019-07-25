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
    private final PrestoAction prestoAction;
    private final QueryRewriter queryRewriter;
    private final ChecksumValidator checksumValidator;
    private final List<FailureResolver> failureResolvers;
    private final VerifierConfig config;

    @Inject
    public VerificationFactory(
            SqlParser sqlParser,
            PrestoAction prestoAction,
            QueryRewriter queryRewriter,
            ChecksumValidator checksumValidator,
            List<FailureResolver> failureResolvers,
            VerifierConfig config)
    {
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
        this.prestoAction = requireNonNull(prestoAction, "prestoAction is null");
        this.queryRewriter = requireNonNull(queryRewriter, "queryRewriter is null");
        this.checksumValidator = requireNonNull(checksumValidator, "checksumValidator is null");
        this.failureResolvers = requireNonNull(failureResolvers, "failureResolvers is null");
        this.config = requireNonNull(config, "config is null");
    }

    public Verification get(SourceQuery sourceQuery)
    {
        QueryType queryType = QueryType.of(sqlParser.createStatement(sourceQuery.getControlQuery(), PARSING_OPTIONS));
        switch (queryType.getCategory()) {
            case DATA_PRODUCING:
                return new DataVerification(prestoAction, sourceQuery, queryRewriter, failureResolvers, config, checksumValidator);
            default:
                throw new IllegalStateException(format("Unsupported query type: %s", queryType));
        }
    }
}
