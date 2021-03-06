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
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.verifier.checksum.ChecksumValidator;
import com.facebook.presto.verifier.prestoaction.QueryActions;
import com.facebook.presto.verifier.prestoaction.QueryActionsFactory;
import com.facebook.presto.verifier.prestoaction.SqlExceptionClassifier;
import com.facebook.presto.verifier.resolver.FailureResolverFactoryContext;
import com.facebook.presto.verifier.resolver.FailureResolverManager;
import com.facebook.presto.verifier.resolver.FailureResolverManagerFactory;
import com.facebook.presto.verifier.rewrite.QueryRewriter;
import com.facebook.presto.verifier.rewrite.QueryRewriterFactory;

import javax.inject.Inject;

import java.util.Optional;

import static com.facebook.presto.verifier.framework.ClusterType.CONTROL;
import static com.facebook.presto.verifier.framework.VerifierUtil.PARSING_OPTIONS;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class VerificationFactory
{
    private final SqlParser sqlParser;
    private final QueryActionsFactory queryActionsFactory;
    private final QueryRewriterFactory queryRewriterFactory;
    private final FailureResolverManagerFactory failureResolverManagerFactory;
    private final ChecksumValidator checksumValidator;
    private final SqlExceptionClassifier exceptionClassifier;
    private final VerifierConfig verifierConfig;
    private final TypeManager typeManager;
    private final DeterminismAnalyzerConfig determinismAnalyzerConfig;

    @Inject
    public VerificationFactory(
            SqlParser sqlParser,
            QueryActionsFactory queryActionsFactory,
            QueryRewriterFactory queryRewriterFactory,
            FailureResolverManagerFactory failureResolverManagerFactory,
            ChecksumValidator checksumValidator,
            SqlExceptionClassifier exceptionClassifier,
            VerifierConfig verifierConfig,
            TypeManager typeManager,
            DeterminismAnalyzerConfig determinismAnalyzerConfig)
    {
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
        this.queryActionsFactory = requireNonNull(queryActionsFactory, "queryActionsFactory is null");
        this.queryRewriterFactory = requireNonNull(queryRewriterFactory, "queryRewriterFactory is null");
        this.failureResolverManagerFactory = requireNonNull(failureResolverManagerFactory, "failureResolverManagerFactory is null");
        this.checksumValidator = requireNonNull(checksumValidator, "checksumValidator is null");
        this.exceptionClassifier = requireNonNull(exceptionClassifier, "exceptionClassifier is null");
        this.verifierConfig = requireNonNull(verifierConfig, "config is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.determinismAnalyzerConfig = requireNonNull(determinismAnalyzerConfig, "determinismAnalyzerConfig is null");
    }

    public Verification get(SourceQuery sourceQuery, Optional<VerificationContext> existingContext)
    {
        QueryType queryType = QueryType.of(sqlParser.createStatement(sourceQuery.getQuery(CONTROL), PARSING_OPTIONS));
        VerificationContext verificationContext = existingContext.map(VerificationContext::createForResubmission)
                .orElseGet(() -> VerificationContext.create(sourceQuery.getName(), sourceQuery.getSuite()));
        QueryActions queryActions = queryActionsFactory.create(sourceQuery, verificationContext);

        if (verifierConfig.isExplain()) {
            return new ExplainVerification(
                    queryActions,
                    sourceQuery,
                    exceptionClassifier,
                    verificationContext,
                    verifierConfig,
                    sqlParser);
        }

        QueryRewriter queryRewriter = queryRewriterFactory.create(queryActions.getHelperAction());
        switch (queryType) {
            case CREATE_TABLE_AS_SELECT:
            case INSERT:
            case QUERY:
                DeterminismAnalyzer determinismAnalyzer = new DeterminismAnalyzer(
                        sourceQuery,
                        queryActions.getHelperAction(),
                        queryRewriter,
                        checksumValidator,
                        typeManager,
                        determinismAnalyzerConfig);
                FailureResolverManager failureResolverManager = failureResolverManagerFactory.create(new FailureResolverFactoryContext(sqlParser, queryActions.getHelperAction()));
                return new DataVerification(
                        queryActions,
                        sourceQuery,
                        queryRewriter,
                        determinismAnalyzer,
                        failureResolverManager,
                        exceptionClassifier,
                        verificationContext,
                        verifierConfig,
                        typeManager,
                        checksumValidator);
            case CREATE_VIEW:
                return new CreateViewVerification(
                        sqlParser,
                        queryActions,
                        sourceQuery,
                        queryRewriter,
                        exceptionClassifier,
                        verificationContext,
                        verifierConfig);
            case CREATE_TABLE:
                return new CreateTableVerification(
                        sqlParser,
                        queryActions,
                        sourceQuery,
                        queryRewriter,
                        exceptionClassifier,
                        verificationContext,
                        verifierConfig);
            default:
                throw new IllegalStateException(format("Unsupported query type: %s", queryType));
        }
    }
}
