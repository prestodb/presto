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
package com.facebook.presto.connector.system;

import com.facebook.presto.annotation.UsedByGeneratedCode;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.procedure.Procedure;
import com.facebook.presto.spi.procedure.Procedure.Argument;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.lang.invoke.MethodHandle;
import java.util.NoSuchElementException;

import static com.facebook.presto.spi.StandardErrorCode.ADMINISTRATIVELY_KILLED;
import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.type.StandardTypes.VARCHAR;
import static com.facebook.presto.util.Reflection.methodHandle;
import static java.util.Objects.requireNonNull;

public class KillQueryProcedure
{
    private static final MethodHandle KILL_QUERY = methodHandle(KillQueryProcedure.class, "killQuery", String.class);

    private final QueryManager queryManager;

    @Inject
    public KillQueryProcedure(QueryManager queryManager)
    {
        this.queryManager = requireNonNull(queryManager, "queryManager is null");
    }

    @UsedByGeneratedCode
    public void killQuery(String queryId)
    {
        try {
            QueryInfo queryInfo = queryManager.getQueryInfo(QueryId.valueOf(queryId));

            // If the query was already done, we don't need to do kill it.
            if (queryInfo.getState().isDone()) {
                throw new PrestoException(NOT_SUPPORTED, "The query to kill was not running");
            }

            // Kill the query
            queryManager.failQuery(new QueryId(queryId), new PrestoException(ADMINISTRATIVELY_KILLED, "Killed via kill_query procedure"));

            // Verify if the query was killed successfully.
            if (queryInfo.getState() != QueryState.FAILED || queryInfo.getErrorCode() != StandardErrorCode.ADMINISTRATIVELY_KILLED.toErrorCode()) {
                throw new PrestoException(NOT_SUPPORTED, "The query to kill was not killed");
            }
        }
        catch (NoSuchElementException e) {
            // The query was not killed because it does not exist.
            throw new PrestoException(NOT_FOUND, "The query to kill was not found");
        }
    }

    public Procedure getProcedure()
    {
        return new Procedure(
                "runtime",
                "kill_query",
                ImmutableList.of(new Argument("query_id", VARCHAR)),
                KILL_QUERY.bindTo(this));
    }
}
