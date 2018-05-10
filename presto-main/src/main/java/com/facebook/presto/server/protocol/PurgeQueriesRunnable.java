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
package com.facebook.presto.server.protocol;

import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.spi.QueryId;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;

import java.util.Optional;
import java.util.concurrent.ConcurrentMap;

class PurgeQueriesRunnable
        implements Runnable
{
    private static final Logger log = Logger.get(PurgeQueriesRunnable.class);

    private final ConcurrentMap<QueryId, Query> queries;
    private final QueryManager queryManager;

    public PurgeQueriesRunnable(ConcurrentMap<QueryId, Query> queries, QueryManager queryManager)
    {
        this.queries = queries;
        this.queryManager = queryManager;
    }

    @Override
    public void run()
    {
        try {
            // Queries are added to the query manager before being recorded in queryIds set.
            // Therefore, we take a snapshot if queryIds before getting the live queries
            // from the query manager.  Then we remove only the queries in the snapshot and
            // not live queries set.  If we did this in the other order, a query could be
            // registered between fetching the live queries and inspecting the queryIds set.
            for (QueryId queryId : ImmutableSet.copyOf(queries.keySet())) {
                Query query = queries.get(queryId);
                if (!query.isSubmissionFinished()) {
                    continue;
                }
                Optional<QueryState> state = queryManager.getQueryState(queryId);

                // free up resources if the query completed
                if (!state.isPresent() || state.get() == QueryState.FAILED) {
                    query.dispose();
                }

                // forget about this query if the query manager is no longer tracking it
                if (!state.isPresent()) {
                    queries.remove(queryId);
                }
            }
        }
        catch (Throwable e) {
            log.warn(e, "Error removing old queries");
        }
    }
}
