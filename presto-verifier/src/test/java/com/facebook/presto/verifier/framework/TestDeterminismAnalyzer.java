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
import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.parser.SqlParserOptions;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.verifier.checksum.ChecksumValidator;
import com.facebook.presto.verifier.prestoaction.JdbcPrestoAction;
import com.facebook.presto.verifier.prestoaction.JdbcUrlSelector;
import com.facebook.presto.verifier.prestoaction.PrestoAction;
import com.facebook.presto.verifier.prestoaction.PrestoActionConfig;
import com.facebook.presto.verifier.prestoaction.PrestoExceptionClassifier;
import com.facebook.presto.verifier.prestoaction.QueryActionsConfig;
import com.facebook.presto.verifier.retry.RetryConfig;
import com.facebook.presto.verifier.rewrite.QueryRewriter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.sql.parser.IdentifierSymbol.AT_SIGN;
import static com.facebook.presto.sql.parser.IdentifierSymbol.COLON;
import static com.facebook.presto.verifier.VerifierTestUtil.CATALOG;
import static com.facebook.presto.verifier.VerifierTestUtil.SCHEMA;
import static com.facebook.presto.verifier.VerifierTestUtil.createChecksumValidator;
import static com.facebook.presto.verifier.VerifierTestUtil.createTypeManager;
import static com.facebook.presto.verifier.framework.ClusterType.CONTROL;
import static com.facebook.presto.verifier.framework.ClusterType.TEST;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestDeterminismAnalyzer
{
    private static final String SUITE = "test-suite";
    private static final String NAME = "test-query";
    private static final SqlParser sqlParser = new SqlParser(new SqlParserOptions().allowIdentifierSymbol(COLON, AT_SIGN));

    @Test
    public void testMutableCatalog()
    {
        DeterminismAnalyzer determinismAnalyzer = createDeterminismAnalyzer("mysql");
        assertFalse(isMutableCatalogReferenced(determinismAnalyzer, "SELECT * FROM x join y on x.t = y.t"));
        assertTrue(isMutableCatalogReferenced(determinismAnalyzer, "SELECT * FROM x join mysql.default.y on x.t = y.t"));
    }

    private static boolean isMutableCatalogReferenced(DeterminismAnalyzer determinismAnalyzer, String query)
    {
        return determinismAnalyzer.isNonDeterministicCatalogReferenced(sqlParser.createStatement(query, ParsingOptions.builder().build()));
    }

    private static DeterminismAnalyzer createDeterminismAnalyzer(String mutableCatalogPattern)
    {
        QueryConfiguration configuration = new QueryConfiguration(CATALOG, SCHEMA, Optional.of("user"), Optional.empty(), Optional.empty());
        VerificationContext verificationContext = VerificationContext.create(SUITE, NAME);
        VerifierConfig verifierConfig = new VerifierConfig().setTestId("test-id");
        RetryConfig retryConfig = new RetryConfig();
        QueryActionsConfig queryActionsConfig = new QueryActionsConfig();
        TypeManager typeManager = createTypeManager();
        PrestoAction prestoAction = new JdbcPrestoAction(
                PrestoExceptionClassifier.defaultBuilder().build(),
                configuration,
                verificationContext,
                new JdbcUrlSelector(ImmutableList.of()),
                new PrestoActionConfig(),
                queryActionsConfig.getMetadataTimeout(),
                queryActionsConfig.getChecksumTimeout(),
                retryConfig,
                retryConfig,
                verifierConfig);
        QueryRewriter queryRewriter = new QueryRewriter(
                sqlParser,
                typeManager,
                prestoAction,
                ImmutableMap.of(CONTROL, QualifiedName.of("tmp_verifier_c"), TEST, QualifiedName.of("tmp_verifier_t")),
                ImmutableMap.of());
        ChecksumValidator checksumValidator = createChecksumValidator(verifierConfig);
        SourceQuery sourceQuery = new SourceQuery("test", "", "", "", configuration, configuration);

        return new DeterminismAnalyzer(
                sourceQuery,
                prestoAction,
                queryRewriter,
                checksumValidator,
                typeManager,
                new DeterminismAnalyzerConfig().setNonDeterministicCatalogs(mutableCatalogPattern));
    }
}
