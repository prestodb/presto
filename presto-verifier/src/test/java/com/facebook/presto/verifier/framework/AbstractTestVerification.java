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
import com.facebook.presto.sql.parser.SqlParserOptions;
import com.facebook.presto.tests.StandaloneQueryRunner;
import com.facebook.presto.verifier.checksum.ChecksumValidator;
import com.facebook.presto.verifier.event.VerifierQueryEvent;
import com.facebook.presto.verifier.prestoaction.JdbcPrestoAction;
import com.facebook.presto.verifier.prestoaction.PrestoAction;
import com.facebook.presto.verifier.prestoaction.PrestoClusterConfig;
import com.facebook.presto.verifier.prestoaction.PrestoExceptionClassifier;
import com.facebook.presto.verifier.resolver.ChecksumExceededTimeLimitFailureResolver;
import com.facebook.presto.verifier.resolver.ExceededGlobalMemoryLimitFailureResolver;
import com.facebook.presto.verifier.resolver.ExceededTimeLimitFailureResolver;
import com.facebook.presto.verifier.resolver.FailureResolverManagerFactory;
import com.facebook.presto.verifier.resolver.VerifierLimitationFailureResolver;
import com.facebook.presto.verifier.retry.RetryConfig;
import com.facebook.presto.verifier.rewrite.QueryRewriteConfig;
import com.facebook.presto.verifier.rewrite.QueryRewriterFactory;
import com.facebook.presto.verifier.rewrite.VerificationQueryRewriterFactory;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.AfterClass;

import java.util.Optional;
import java.util.regex.Pattern;

import static com.facebook.airlift.testing.Closeables.closeQuietly;
import static com.facebook.presto.sql.parser.IdentifierSymbol.AT_SIGN;
import static com.facebook.presto.sql.parser.IdentifierSymbol.COLON;
import static com.facebook.presto.verifier.VerifierTestUtil.CATALOG;
import static com.facebook.presto.verifier.VerifierTestUtil.SCHEMA;
import static com.facebook.presto.verifier.VerifierTestUtil.createChecksumValidator;
import static com.facebook.presto.verifier.VerifierTestUtil.createTypeManager;
import static com.facebook.presto.verifier.VerifierTestUtil.setupPresto;
import static java.util.regex.Pattern.DOTALL;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class AbstractTestVerification
{
    protected static final String SUITE = "test-suite";
    protected static final String NAME = "test-query";
    protected static final String TEST_ID = "test-id";

    private final StandaloneQueryRunner queryRunner;

    public AbstractTestVerification()
            throws Exception
    {
        this.queryRunner = setupPresto();
    }

    @AfterClass
    public void destroy()
    {
        closeQuietly(queryRunner);
    }

    public StandaloneQueryRunner getQueryRunner()
    {
        return queryRunner;
    }

    protected Optional<VerifierQueryEvent> runVerification(String controlQuery, String testQuery)
    {
        return runVerification(controlQuery, testQuery, new DeterminismAnalyzerConfig());
    }

    protected Optional<VerifierQueryEvent> runVerification(String controlQuery, String testQuery, DeterminismAnalyzerConfig determinismAnalyzerConfig)
    {
        return runVerification(controlQuery, testQuery, Optional.empty(), determinismAnalyzerConfig);
    }

    protected Optional<VerifierQueryEvent> runVerification(String controlQuery, String testQuery, Optional<PrestoAction> mockPrestoAction, DeterminismAnalyzerConfig determinismAnalyzerConfig)
    {
        QueryConfiguration configuration = new QueryConfiguration(CATALOG, SCHEMA, Optional.of("user"), Optional.empty(), Optional.empty());
        VerificationContext verificationContext = VerificationContext.create();
        VerifierConfig verifierConfig = new VerifierConfig().setTestId(TEST_ID);
        RetryConfig retryConfig = new RetryConfig();
        TypeManager typeManager = createTypeManager();
        PrestoAction prestoAction = mockPrestoAction.orElseGet(() -> new JdbcPrestoAction(
                PrestoExceptionClassifier.createDefault(),
                configuration,
                verificationContext,
                new PrestoClusterConfig()
                        .setHost(queryRunner.getServer().getAddress().getHost())
                        .setJdbcPort(queryRunner.getServer().getAddress().getPort()),
                retryConfig,
                retryConfig));
        SqlParser sqlParser = new SqlParser(new SqlParserOptions().allowIdentifierSymbol(COLON, AT_SIGN));
        QueryRewriterFactory queryRewriterFactory = new VerificationQueryRewriterFactory(
                sqlParser,
                typeManager,
                new QueryRewriteConfig().setTablePrefix("tmp_verifier_c"),
                new QueryRewriteConfig().setTablePrefix("tmp_verifier_t"));
        ChecksumValidator checksumValidator = createChecksumValidator(verifierConfig);
        SourceQuery sourceQuery = new SourceQuery(SUITE, NAME, controlQuery, testQuery, configuration, configuration);
        FailureResolverManagerFactory failureResolverManagerFactory = new FailureResolverManagerFactory(
                ImmutableSet.of(
                        new ExceededGlobalMemoryLimitFailureResolver(),
                        new ExceededTimeLimitFailureResolver(),
                        new ChecksumExceededTimeLimitFailureResolver(),
                        new VerifierLimitationFailureResolver()),
                ImmutableSet.of());
        return new VerificationFactory(
                sqlParser,
                (source, context) -> prestoAction,
                queryRewriterFactory,
                failureResolverManagerFactory,
                path -> {throw new UnsupportedOperationException();},
                checksumValidator,
                verifierConfig,
                typeManager,
                determinismAnalyzerConfig).get(sourceQuery, Optional.empty()).run().getEvent();
    }

    protected void assertEvent(
            VerifierQueryEvent event,
            VerifierQueryEvent.EventStatus expectedStatus,
            Optional<DeterminismAnalysis> expectedDeterminismAnalysis,
            Optional<String> expectedErrorCode,
            Optional<String> expectedErrorMessageRegex)
    {
        assertEquals(event.getSuite(), SUITE);
        assertEquals(event.getTestId(), TEST_ID);
        assertEquals(event.getName(), NAME);
        assertEquals(event.getStatus(), expectedStatus.name());
        assertEquals(event.getDeterminismAnalysis(), expectedDeterminismAnalysis.map(DeterminismAnalysis::name).orElse(null));
        assertEquals(event.getErrorCode(), expectedErrorCode.orElse(null));
        if (event.getErrorMessage() == null) {
            assertFalse(expectedErrorMessageRegex.isPresent());
        }
        else {
            assertTrue(expectedErrorMessageRegex.isPresent());
            assertTrue(Pattern.compile(expectedErrorMessageRegex.get(), DOTALL).matcher(event.getErrorMessage()).matches());
        }
    }
}
