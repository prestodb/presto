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
import com.facebook.presto.spi.ErrorCodeSupplier;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.parser.SqlParserOptions;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.verifier.checksum.ChecksumValidator;
import com.facebook.presto.verifier.checksum.FloatingPointColumnValidator;
import com.facebook.presto.verifier.checksum.OrderableArrayColumnValidator;
import com.facebook.presto.verifier.checksum.SimpleColumnValidator;
import com.facebook.presto.verifier.resolver.ExceededGlobalMemoryLimitFailureResolver;
import com.facebook.presto.verifier.resolver.ExceededTimeLimitFailureResolver;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_PARTITION_DROPPED_DURING_QUERY;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.sql.parser.IdentifierSymbol.AT_SIGN;
import static com.facebook.presto.sql.parser.IdentifierSymbol.COLON;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;

public class TestVerificationManager
{
    private static class MockPrestoAction
            implements PrestoAction
    {
        private final ErrorCodeSupplier errorCode;

        public MockPrestoAction(ErrorCodeSupplier errorCode)
        {
            this.errorCode = requireNonNull(errorCode, "errorCode is null");
        }

        @Override
        public QueryStats execute(
                Statement statement,
                QueryConfiguration configuration,
                QueryOrigin queryOrigin,
                VerificationContext context)
        {
            throw QueryException.forPresto(new RuntimeException(), Optional.of(errorCode), false, Optional.empty(), queryOrigin);
        }

        @Override
        public <R> QueryResult<R> execute(
                Statement statement,
                QueryConfiguration configuration,
                QueryOrigin queryOrigin,
                VerificationContext context,
                ResultSetConverter<R> converter)
        {
            throw QueryException.forPresto(new RuntimeException(), Optional.of(errorCode), false, Optional.empty(), queryOrigin);
        }
    }

    private static final String SUITE = "test-suite";
    private static final String NAME = "test-query";
    private static final SqlParser SQL_PARSER = new SqlParser(new SqlParserOptions().allowIdentifierSymbol(AT_SIGN, COLON));
    private static final QueryConfiguration QUERY_CONFIGURATION = new QueryConfiguration("test", "di", Optional.of("user"), Optional.empty(), Optional.empty());
    private static final SourceQuery SOURCE_QUERY = new SourceQuery(
            SUITE,
            NAME,
            "SELECT 1",
            "SELECT 2",
            QUERY_CONFIGURATION,
            QUERY_CONFIGURATION);
    private static final VerifierConfig VERIFIER_CONFIG = new VerifierConfig().setTestId("test");

    @Test
    public void testFailureRequeued()
    {
        VerificationManager manager = getVerificationManager(ImmutableList.of(SOURCE_QUERY), new MockPrestoAction(HIVE_PARTITION_DROPPED_DURING_QUERY), VERIFIER_CONFIG);
        manager.start();
        assertEquals(manager.getQueriesSubmitted().get(), 3);
    }

    @Test
    public void testFailureNotRequeued()
    {
        VerificationManager manager = getVerificationManager(ImmutableList.of(SOURCE_QUERY), new MockPrestoAction(GENERIC_INTERNAL_ERROR), VERIFIER_CONFIG);
        manager.start();
        assertEquals(manager.getQueriesSubmitted().get(), 1);
    }

    @Test
    public void testFailureRequeueDisabled()
    {
        VerificationManager manager = getVerificationManager(
                ImmutableList.of(SOURCE_QUERY),
                new MockPrestoAction(HIVE_PARTITION_DROPPED_DURING_QUERY),
                new VerifierConfig().setTestId("test").setVerificationResubmissionLimit(0));
        manager.start();
        assertEquals(manager.getQueriesSubmitted().get(), 1);
    }

    private static VerificationManager getVerificationManager(List<SourceQuery> sourceQueries, PrestoAction prestoAction, VerifierConfig config)
    {
        return new VerificationManager(
                () -> sourceQueries,
                new VerificationFactory(
                        SQL_PARSER,
                        prestoAction,
                        new QueryRewriter(SQL_PARSER, prestoAction, ImmutableList.of(), config),
                        new ChecksumValidator(new SimpleColumnValidator(), new FloatingPointColumnValidator(config), new OrderableArrayColumnValidator()),
                        ImmutableList.of(new ExceededGlobalMemoryLimitFailureResolver(), new ExceededTimeLimitFailureResolver()),
                        config),
                SQL_PARSER,
                ImmutableSet.of(),
                ImmutableList.of(),
                new QueryConfigurationOverridesConfig(),
                new QueryConfigurationOverridesConfig(),
                config);
    }
}
