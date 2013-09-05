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
package com.facebook.presto.server;

import com.facebook.presto.TaskSource;
import com.facebook.presto.UnpartitionedPagePartitionFunction;
import com.facebook.presto.client.FailureInfo;
import com.facebook.presto.execution.LocationFactory;
import com.facebook.presto.execution.QueryId;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.execution.QueryStats;
import com.facebook.presto.execution.StageId;
import com.facebook.presto.execution.StageInfo;
import com.facebook.presto.execution.StageState;
import com.facebook.presto.execution.StageStats;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import io.airlift.units.Duration;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.net.URI;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import static com.facebook.presto.OutputBuffers.INITIAL_EMPTY_OUTPUT_BUFFERS;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_VARBINARY;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;

@ThreadSafe
public class MockQueryManager
        implements QueryManager
{
    public static final List<TupleInfo> TUPLE_INFOS = ImmutableList.of(SINGLE_VARBINARY);

    private final MockTaskManager mockTaskManager;
    private final LocationFactory locationFactory;
    private final AtomicInteger nextQueryId = new AtomicInteger();
    private final ConcurrentMap<QueryId, SimpleQuery> queries = new ConcurrentHashMap<>();

    @Inject
    public MockQueryManager(MockTaskManager mockTaskManager, LocationFactory locationFactory)
    {
        Preconditions.checkNotNull(mockTaskManager, "mockTaskManager is null");
        Preconditions.checkNotNull(locationFactory, "locationFactory is null");
        this.mockTaskManager = mockTaskManager;
        this.locationFactory = locationFactory;
    }

    @Override
    public List<QueryInfo> getAllQueryInfo()
    {
        return ImmutableList.copyOf(filter(transform(queries.values(), new Function<SimpleQuery, QueryInfo>()
        {
            @Override
            public QueryInfo apply(SimpleQuery queryWorker)
            {
                try {
                    return queryWorker.getQueryInfo();
                }
                catch (RuntimeException ignored) {
                    return null;
                }
            }
        }), Predicates.notNull()));
    }

    @Override
    public Duration waitForStateChange(QueryId queryId, QueryState currentState, Duration maxWait)
            throws InterruptedException
    {
        return maxWait;
    }

    @Override
    public QueryInfo getQueryInfo(QueryId queryId)
    {
        Preconditions.checkNotNull(queryId, "queryId is null");

        SimpleQuery query = queries.get(queryId);
        if (query == null) {
            throw new NoSuchElementException();
        }
        return query.getQueryInfo();
    }

    @Override
    public QueryInfo createQuery(Session session, String query)
    {
        Preconditions.checkNotNull(query, "query is null");

        TaskId outputTaskId = new TaskId(String.valueOf(nextQueryId.getAndIncrement()), "0", "0");

        mockTaskManager.updateTask(session,
                outputTaskId,
                null,
                ImmutableList.<TaskSource>of(),
                INITIAL_EMPTY_OUTPUT_BUFFERS.withBuffer("out", new UnpartitionedPagePartitionFunction()).withNoMoreBufferIds());

        SimpleQuery simpleQuery = new SimpleQuery(outputTaskId, locationFactory.createQueryLocation(outputTaskId.getQueryId()), mockTaskManager, locationFactory);
        queries.put(outputTaskId.getQueryId(), simpleQuery);
        return simpleQuery.getQueryInfo();
    }

    @Override
    public void cancelQuery(QueryId queryId)
    {
        queries.remove(queryId);
    }

    @Override
    public void cancelStage(StageId stageId)
    {
        // mock queries don't have stages
    }

    private static class SimpleQuery
    {
        private final TaskId outputTaskId;
        private final URI self;
        private final MockTaskManager mockTaskManager;
        private final LocationFactory locationFactory;

        private SimpleQuery(TaskId outputTaskId, URI self, MockTaskManager mockTaskManager, LocationFactory locationFactory)
        {
            this.outputTaskId = outputTaskId;
            this.self = self;
            this.mockTaskManager = mockTaskManager;
            this.locationFactory = locationFactory;
        }

        private QueryInfo getQueryInfo()
        {
            TaskInfo outputTask = mockTaskManager.getTaskInfo(outputTaskId, false);

            QueryState state;
            switch (outputTask.getState()) {
                case PLANNED:
                case RUNNING:
                    state = QueryState.RUNNING;
                    break;
                case FINISHED:
                    state = QueryState.FINISHED;
                    break;
                case CANCELED:
                    state = QueryState.CANCELED;
                    break;
                case FAILED:
                    state = QueryState.FAILED;
                    break;
                default:
                    throw new IllegalStateException("Unknown task state " + outputTask.getState());
            }
            return new QueryInfo(outputTaskId.getQueryId(),
                    new Session("user", "test", "test_catalog", "test_schema", null, null),
                    state,
                    self,
                    ImmutableList.of("out"),
                    "query",
                    new QueryStats(),
                    new StageInfo(outputTaskId.getStageId(),
                            StageState.FINISHED,
                            locationFactory.createStageLocation(outputTaskId.getStageId()),
                            null,
                            TUPLE_INFOS,
                            new StageStats(),
                            ImmutableList.<TaskInfo>of(outputTask),
                            ImmutableList.<StageInfo>of(),
                            ImmutableList.<FailureInfo>of()),
                    null,
                    null);
        }
    }
}
