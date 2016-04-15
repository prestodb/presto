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
package com.facebook.presto.execution;

import com.facebook.presto.Session;
import com.facebook.presto.execution.resourceGroups.ResourceGroupSelector;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.sql.tree.Statement;
import com.google.common.collect.ImmutableList;
import org.weakref.jmx.MBeanExporter;
import org.weakref.jmx.ObjectNames;

import javax.annotation.PreDestroy;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;

import static com.facebook.presto.execution.QueuedExecution.createQueuedExecution;
import static com.facebook.presto.spi.StandardErrorCode.USER_ERROR;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class SqlQueryQueueManager
        implements QueryQueueManager
{
    private final ConcurrentMap<QueryQueueDefinition, QueryQueue> queryQueues = new ConcurrentHashMap<>();
    private final List<ResourceGroupSelector> selectors;
    private final MBeanExporter mbeanExporter;

    @Inject
    public SqlQueryQueueManager(List<? extends ResourceGroupSelector> selectors, MBeanExporter mbeanExporter)
    {
        this.mbeanExporter = requireNonNull(mbeanExporter, "mbeanExporter is null");
        this.selectors = ImmutableList.copyOf(selectors);
    }

    @Override
    public boolean submit(Statement statement, QueryExecution queryExecution, Executor executor, SqlQueryManagerStats stats)
    {
        List<QueryQueue> queues = selectQueues(statement, queryExecution.getSession(), executor);

        for (QueryQueue queue : queues) {
            if (!queue.reserve(queryExecution)) {
                // Reject query if we couldn't acquire a permit to enter the queue.
                // The permits will be released when this query fails.
                return false;
            }
        }
        queues.get(0).enqueue(createQueuedExecution(queryExecution, queues.subList(1, queues.size()), executor, stats));
        return true;
    }

    // Queues returned have already been created and added queryQueues
    private List<QueryQueue> selectQueues(Statement statement, Session session, Executor executor)
    {
        for (ResourceGroupSelector selector : selectors) {
            Optional<List<QueryQueueDefinition>> queues = selector.match(statement, session.toSessionRepresentation());
            if (queues.isPresent()) {
                return getOrCreateQueues(session, executor, queues.get());
            }
        }
        throw new PrestoException(USER_ERROR, "Query did not match any queuing rule");
    }

    private List<QueryQueue> getOrCreateQueues(Session session, Executor executor, List<QueryQueueDefinition> definitions)
    {
        ImmutableList.Builder<QueryQueue> queues = ImmutableList.builder();
        for (QueryQueueDefinition definition : definitions) {
            String expandedName = definition.getName();
            if (!queryQueues.containsKey(definition)) {
                QueryQueue queue = new QueryQueue(executor, definition.getMaxQueued(), definition.getMaxConcurrent());
                if (queryQueues.putIfAbsent(definition, queue) == null) {
                    // Export the mbean, after checking for races
                    String objectName = ObjectNames.builder(QueryQueue.class, definition.getName()).withProperty("expansion", expandedName).build();
                    mbeanExporter.export(objectName, queue);
                }
            }
            queues.add(queryQueues.get(definition));
        }
        return queues.build();
    }

    @PreDestroy
    public void destroy()
    {
        for (QueryQueueDefinition definition : queryQueues.keySet()) {
            String objectName = ObjectNames.builder(QueryQueue.class, definition.getName()).withProperty("expansion", definition.getName()).build();
            mbeanExporter.unexport(objectName);
        }
    }
}
