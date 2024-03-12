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
import com.facebook.presto.verifier.prestoaction.DefaultClientInfoFactory;
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
import com.facebook.presto.verifier.source.SnapshotQueryConsumer;
import com.facebook.presto.verifier.source.SnapshotQuerySupplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import org.testng.annotations.AfterClass;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.sql.parser.IdentifierSymbol.AT_SIGN;
import static com.facebook.presto.sql.parser.IdentifierSymbol.COLON;
import static com.facebook.presto.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE;
import static com.facebook.presto.verifier.VerifierTestUtil.CATALOG;
import static com.facebook.presto.verifier.VerifierTestUtil.SCHEMA;
import static com.facebook.presto.verifier.VerifierTestUtil.createChecksumValidator;
import static com.facebook.presto.verifier.VerifierTestUtil.createTypeManager;
import static com.facebook.presto.verifier.VerifierTestUtil.setupPresto;
import static com.facebook.presto.verifier.source.AbstractJdbiSnapshotQuerySupplier.VERIFIER_SNAPSHOT_KEY_PATTERN;
import static java.lang.String.format;

public abstract class AbstractVerificationTest
{
    protected static final String SUITE = "test-suite";
    protected static final String NAME = "test-query";
    protected static final String TEST_ID = "test-id";
    protected static final QueryConfiguration QUERY_CONFIGURATION = new QueryConfiguration(CATALOG, SCHEMA, Optional.of("user"), Optional.empty(), Optional.empty(), true);
    protected static final ParsingOptions PARSING_OPTIONS = ParsingOptions.builder().setDecimalLiteralTreatment(AS_DOUBLE).build();
    protected static final String CONTROL_TABLE_PREFIX = "tmp_verifier_c";
    protected static final String TEST_TABLE_PREFIX = "tmp_verifier_t";

    protected static VerificationSettings concurrentControlAndTestSettings;
    protected static VerificationSettings skipControlSettings;
    protected static VerificationSettings saveSnapshotSettings;
    protected static VerificationSettings queryBankModeSettings;

    protected static VerificationSettings reuseTableSettings;

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

        concurrentControlAndTestSettings = new VerificationSettings();
        concurrentControlAndTestSettings.concurrentControlAndTest = Optional.of(true);
        skipControlSettings = new VerificationSettings();
        skipControlSettings.skipControl = Optional.of(true);
        saveSnapshotSettings = new VerificationSettings();
        saveSnapshotSettings.runningMode = Optional.of("query-bank");
        saveSnapshotSettings.saveSnapshot = Optional.of(true);
        queryBankModeSettings = new VerificationSettings();
        queryBankModeSettings.runningMode = Optional.of("query-bank");
        reuseTableSettings = new VerificationSettings();
        reuseTableSettings.reuseTable = Optional.of(true);
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
        return new SourceQuery(SUITE, NAME, controlQuery, testQuery, Optional.empty(), Optional.empty(), QUERY_CONFIGURATION, QUERY_CONFIGURATION);
    }

    protected SourceQuery getSourceQuery(String controlQuery, String testQuery, String controlQueryId, String testQueryId)
    {
        return new SourceQuery(SUITE, NAME, controlQuery, testQuery, Optional.of(controlQueryId), Optional.of(testQueryId), QUERY_CONFIGURATION, QUERY_CONFIGURATION);
    }

    protected Optional<VerifierQueryEvent> runExplain(String controlQuery, String testQuery)
    {
        return verify(getSourceQuery(controlQuery, testQuery), true, Optional.empty(), Optional.empty());
    }

    protected Optional<VerifierQueryEvent> runExplain(String controlQuery, String testQuery, VerificationSettings settings)
    {
        return verify(getSourceQuery(controlQuery, testQuery), true, Optional.empty(), Optional.of(settings));
    }

    protected Optional<VerifierQueryEvent> runVerification(String controlQuery, String testQuery)
    {
        return verify(getSourceQuery(controlQuery, testQuery), false, Optional.empty(), Optional.empty());
    }

    protected Optional<VerifierQueryEvent> runVerification(String controlQuery, String testQuery, VerificationSettings settings)
    {
        return verify(getSourceQuery(controlQuery, testQuery), false, Optional.empty(), Optional.of(settings));
    }

    protected Optional<VerifierQueryEvent> runVerification(String controlQuery, String testQuery, String controlQueryId, String testQueryId, VerificationSettings settings)
    {
        return verify(getSourceQuery(controlQuery, testQuery, controlQueryId, testQueryId), false, Optional.empty(), Optional.of(settings));
    }

    protected Optional<VerifierQueryEvent> verify(SourceQuery sourceQuery, boolean explain)
    {
        return verify(sourceQuery, explain, Optional.empty(), Optional.empty());
    }

    protected Optional<VerifierQueryEvent> verify(SourceQuery sourceQuery, boolean explain, PrestoAction mockPrestoAction)
    {
        return verify(sourceQuery, explain, Optional.of(mockPrestoAction), Optional.empty());
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
                new DefaultClientInfoFactory(verifierConfig));
    }

    private Optional<VerifierQueryEvent> verify(
            SourceQuery sourceQuery,
            boolean explain,
            Optional<PrestoAction> mockPrestoAction,
            Optional<VerificationSettings> verificationSettings)
    {
        VerifierConfig verifierConfig = new VerifierConfig().setTestId(TEST_ID).setExplain(explain);
        verificationSettings.ifPresent(settings -> {
            settings.concurrentControlAndTest.ifPresent(verifierConfig::setConcurrentControlAndTest);
            settings.skipControl.ifPresent(verifierConfig::setSkipControl);
            settings.runningMode.ifPresent(verifierConfig::setRunningMode);
            settings.saveSnapshot.ifPresent(verifierConfig::setSaveSnapshot);
            settings.functionSubstitutes.ifPresent(verifierConfig::setFunctionSubstitutes);
        });
        QueryRewriteConfig controlRewriteConfig = new QueryRewriteConfig().setTablePrefix(CONTROL_TABLE_PREFIX);
        QueryRewriteConfig testRewriteConfig = new QueryRewriteConfig().setTablePrefix(TEST_TABLE_PREFIX);
        verificationSettings.ifPresent(settings -> {
            settings.reuseTable.ifPresent(controlRewriteConfig::setReuseTable);
            settings.reuseTable.ifPresent(testRewriteConfig::setReuseTable);
        });
        TypeManager typeManager = createTypeManager();
        PrestoAction prestoAction = mockPrestoAction.orElseGet(() -> getPrestoAction(Optional.of(sourceQuery.getControlConfiguration())));
        QueryRewriterFactory queryRewriterFactory = new VerificationQueryRewriterFactory(
                sqlParser,
                typeManager,
                controlRewriteConfig,
                testRewriteConfig,
                verifierConfig);

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
        return verificationFactory.get(sourceQuery, Optional.empty(),
                MockSnapshotSupplierAndConsumer.getMockSnapshotSupplierAndConsumer(),
                MockSnapshotSupplierAndConsumer.getMockSnapshotSupplierAndConsumer().get()
                ).run().getEvent();
    }

    public static class VerificationSettings
    {
        public VerificationSettings()
        {
            concurrentControlAndTest = Optional.empty();
            skipControl = Optional.empty();
            runningMode = Optional.empty();
            saveSnapshot = Optional.empty();
            functionSubstitutes = Optional.empty();
            reuseTable = Optional.empty();
        }

        Optional<Boolean> concurrentControlAndTest;
        Optional<Boolean> skipControl;
        Optional<String> runningMode;
        Optional<Boolean> saveSnapshot;
        Optional<String> functionSubstitutes;
        Optional<Boolean> reuseTable;
    }

    public static class MockSnapshotSupplierAndConsumer
            implements SnapshotQuerySupplier, SnapshotQueryConsumer
    {
        private static MockSnapshotSupplierAndConsumer mockSnapshotSupplierAndConsumer = new MockSnapshotSupplierAndConsumer();
        private Map<String, SnapshotQuery> snapshots = new HashMap<>();
        @Override
        public Map<String, SnapshotQuery> get()
        {
            return snapshots;
        }

        @Override
        public void accept(SnapshotQuery snapshot)
        {
            String key = format(VERIFIER_SNAPSHOT_KEY_PATTERN, snapshot.getSuite(), snapshot.getName(), snapshot.isExplain());
            snapshots.put(key, snapshot);
        }

        public static MockSnapshotSupplierAndConsumer getMockSnapshotSupplierAndConsumer()
        {
            return mockSnapshotSupplierAndConsumer;
        }
    }
}
