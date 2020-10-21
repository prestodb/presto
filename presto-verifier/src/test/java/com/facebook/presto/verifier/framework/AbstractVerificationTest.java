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

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.airlift.bootstrap.LifeCycleManager;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.parser.SqlParserOptions;
import com.facebook.presto.tests.StandaloneQueryRunner;
import com.facebook.presto.verifier.event.VerifierQueryEvent;
import com.facebook.presto.verifier.prestoaction.JdbcPrestoAction;
import com.facebook.presto.verifier.prestoaction.JdbcUrlSelector;
import com.facebook.presto.verifier.prestoaction.PrestoAction;
import com.facebook.presto.verifier.prestoaction.PrestoActionConfig;
import com.facebook.presto.verifier.prestoaction.PrestoExceptionClassifier;
import com.facebook.presto.verifier.prestoaction.QueryActions;
import com.facebook.presto.verifier.prestoaction.QueryActionsConfig;
import com.facebook.presto.verifier.resolver.FailureResolverManagerFactory;
import com.facebook.presto.verifier.resolver.FailureResolverModule;
import com.facebook.presto.verifier.retry.RetryConfig;
import com.facebook.presto.verifier.rewrite.QueryRewriteConfig;
import com.facebook.presto.verifier.rewrite.QueryRewriterFactory;
import com.facebook.presto.verifier.rewrite.VerificationQueryRewriterFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import org.testng.annotations.AfterClass;

import java.util.Optional;

import static com.facebook.presto.sql.parser.IdentifierSymbol.AT_SIGN;
import static com.facebook.presto.sql.parser.IdentifierSymbol.COLON;
import static com.facebook.presto.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE;
import static com.facebook.presto.verifier.VerifierTestUtil.CATALOG;
import static com.facebook.presto.verifier.VerifierTestUtil.SCHEMA;
import static com.facebook.presto.verifier.VerifierTestUtil.createChecksumValidator;
import static com.facebook.presto.verifier.VerifierTestUtil.createTypeManager;
import static com.facebook.presto.verifier.VerifierTestUtil.setupPresto;

public abstract class AbstractVerificationTest
{
    protected static final String SUITE = "test-suite";
    protected static final String NAME = "test-query";
    protected static final String TEST_ID = "test-id";
    protected static final QueryConfiguration QUERY_CONFIGURATION = new QueryConfiguration(CATALOG, SCHEMA, Optional.of("user"), Optional.empty(), Optional.empty());
    protected static final ParsingOptions PARSING_OPTIONS = ParsingOptions.builder().setDecimalLiteralTreatment(AS_DOUBLE).build();
    protected static final String CONTROL_TABLE_PREFIX = "tmp_verifier_c";
    protected static final String TEST_TABLE_PREFIX = "tmp_verifier_t";

    private final StandaloneQueryRunner queryRunner;

    private final Injector injector;
    private final SqlParser sqlParser = new SqlParser(new SqlParserOptions().allowIdentifierSymbol(COLON, AT_SIGN));
    private final PrestoExceptionClassifier exceptionClassifier = PrestoExceptionClassifier.defaultBuilder().build();
    private final DeterminismAnalyzerConfig determinismAnalyzerConfig = new DeterminismAnalyzerConfig().setMaxAnalysisRuns(3).setRunTeardown(true);
    private final FailureResolverManagerFactory failureResolverManagerFactory;

    public AbstractVerificationTest()
            throws Exception
    {
        this.queryRunner = setupPresto();
        this.injector = new Bootstrap(ImmutableList.of(FailureResolverModule.BUILT_IN))
                .setRequiredConfigurationProperties(ImmutableMap.of("too-many-open-partitions.failure-resolver.enabled", "false"))
                .initialize();
        this.failureResolverManagerFactory = injector.getInstance(FailureResolverManagerFactory.class);
    }

    @AfterClass
    public void teardown()
    {
        try {
            injector.getInstance(LifeCycleManager.class).stop();
        }
        catch (Throwable ignored) {
        }
    }

    public StandaloneQueryRunner getQueryRunner()
    {
        return queryRunner;
    }

    public SqlParser getSqlParser()
    {
        return sqlParser;
    }

    protected SourceQuery getSourceQuery(String controlQuery, String testQuery)
    {
        return new SourceQuery(SUITE, NAME, controlQuery, testQuery, QUERY_CONFIGURATION, QUERY_CONFIGURATION);
    }

    protected Optional<VerifierQueryEvent> runExplain(String controlQuery, String testQuery)
    {
        return verify(getSourceQuery(controlQuery, testQuery), true, Optional.empty());
    }

    protected Optional<VerifierQueryEvent> runVerification(String controlQuery, String testQuery)
    {
        return verify(getSourceQuery(controlQuery, testQuery), false, Optional.empty());
    }

    protected Optional<VerifierQueryEvent> verify(SourceQuery sourceQuery, boolean explain)
    {
        return verify(sourceQuery, explain, Optional.empty());
    }

    protected Optional<VerifierQueryEvent> verify(SourceQuery sourceQuery, boolean explain, PrestoAction mockPrestoAction)
    {
        return verify(sourceQuery, explain, Optional.of(mockPrestoAction));
    }

    protected PrestoAction getPrestoAction(Optional<QueryConfiguration> queryConfiguration)
    {
        VerificationContext verificationContext = VerificationContext.create(NAME, SUITE);
        VerifierConfig verifierConfig = new VerifierConfig().setTestId(TEST_ID);
        RetryConfig retryConfig = new RetryConfig();
        PrestoActionConfig prestoActionConfig = new PrestoActionConfig()
                .setHosts(queryRunner.getServer().getAddress().getHost())
                .setJdbcPort(queryRunner.getServer().getAddress().getPort());
        QueryActionsConfig queryActionsConfig = new QueryActionsConfig();
        return new JdbcPrestoAction(
                exceptionClassifier,
                queryConfiguration.orElse(QUERY_CONFIGURATION),
                verificationContext,
                new JdbcUrlSelector(prestoActionConfig.getJdbcUrls()),
                prestoActionConfig,
                queryActionsConfig.getMetadataTimeout(),
                queryActionsConfig.getChecksumTimeout(),
                retryConfig,
                retryConfig,
                verifierConfig);
    }

    private Optional<VerifierQueryEvent> verify(SourceQuery sourceQuery, boolean explain, Optional<PrestoAction> mockPrestoAction)
    {
        VerifierConfig verifierConfig = new VerifierConfig().setTestId(TEST_ID).setExplain(explain);
        TypeManager typeManager = createTypeManager();
        PrestoAction prestoAction = mockPrestoAction.orElseGet(() -> getPrestoAction(Optional.of(sourceQuery.getControlConfiguration())));
        QueryRewriterFactory queryRewriterFactory = new VerificationQueryRewriterFactory(
                sqlParser,
                typeManager,
                new QueryRewriteConfig().setTablePrefix(CONTROL_TABLE_PREFIX),
                new QueryRewriteConfig().setTablePrefix(TEST_TABLE_PREFIX));

        VerificationFactory verificationFactory = new VerificationFactory(
                sqlParser,
                (source, context) -> new QueryActions(prestoAction, prestoAction, prestoAction),
                queryRewriterFactory,
                failureResolverManagerFactory,
                createChecksumValidator(verifierConfig),
                exceptionClassifier,
                verifierConfig,
                typeManager,
                determinismAnalyzerConfig);
        return verificationFactory.get(sourceQuery, Optional.empty()).run().getEvent();
    }
}
