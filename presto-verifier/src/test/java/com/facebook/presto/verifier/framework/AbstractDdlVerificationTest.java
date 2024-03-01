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

import com.facebook.presto.sql.tree.ShowCreate;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.verifier.event.VerifierQueryEvent;
import com.facebook.presto.verifier.framework.DdlMatchResult.MatchType;
import com.facebook.presto.verifier.prestoaction.PrestoAction;
import com.facebook.presto.verifier.prestoaction.QueryActionStats;
import com.google.common.collect.ImmutableMap;

import java.sql.SQLException;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.verifier.event.VerifierQueryEvent.EventStatus;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;

public class AbstractDdlVerificationTest
        extends AbstractVerificationTest
{
    public AbstractDdlVerificationTest()
            throws Exception
    {
    }

    protected void assertEvent(
            VerifierQueryEvent event,
            EventStatus expectedStatus,
            Optional<MatchType> expectedMatchType,
            boolean hasSetupQuery)
    {
        assertEquals(event.getSuite(), SUITE);
        assertEquals(event.getTestId(), TEST_ID);
        assertEquals(event.getName(), NAME);
        assertEquals(event.getStatus(), expectedStatus.name());
        expectedMatchType.ifPresent(matchType -> assertEquals(event.getMatchType(), matchType.name()));

        int setupQueryCount = hasSetupQuery ? 1 : 0;
        assertEquals(event.getControlQueryInfo().getSetupQueries().size(), setupQueryCount);
        assertEquals(event.getTestQueryInfo().getSetupQueries().size(), setupQueryCount);
        assertEquals(event.getControlQueryInfo().getTeardownQueries().size(), 1);
        assertEquals(event.getTestQueryInfo().getTeardownQueries().size(), 1);
    }

    protected class MockPrestoAction
            implements PrestoAction
    {
        private final PrestoAction prestoAction = getPrestoAction(Optional.empty());

        private final Map<Integer, String> showCreateResultByInvocation;
        private int invocationCount;

        public MockPrestoAction(Map<Integer, String> showCreateResultByInvocation)
        {
            this.showCreateResultByInvocation = ImmutableMap.copyOf(showCreateResultByInvocation);
        }

        @Override
        public QueryActionStats execute(Statement statement, QueryStage queryStage)
        {
            return prestoAction.execute(statement, queryStage);
        }

        @Override
        @SuppressWarnings("unchecked")
        public <R> QueryResult<R> execute(Statement statement, QueryStage queryStage, ResultSetConverter<R> converter)
        {
            if (statement instanceof ShowCreate) {
                invocationCount += 1;
                if (showCreateResultByInvocation.containsKey(invocationCount)) {
                    return (QueryResult<R>) prestoAction.execute(
                            getSqlParser().createStatement(format("SELECT '%s'", showCreateResultByInvocation.get(invocationCount).replaceAll("'", "''")), PARSING_OPTIONS),
                            queryStage,
                            resultSet -> {
                                try {
                                    return Optional.of(resultSet.getString(1));
                                }
                                catch (SQLException e) {
                                    throw new RuntimeException(e);
                                }
                            });
                }
            }
            return prestoAction.execute(statement, queryStage, converter);
        }
    }
}
