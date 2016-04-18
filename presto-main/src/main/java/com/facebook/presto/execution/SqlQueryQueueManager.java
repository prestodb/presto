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
import com.google.common.collect.ImmutableMap;
import org.weakref.jmx.MBeanExporter;
import org.weakref.jmx.ObjectNames;

import javax.annotation.PreDestroy;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.Map;
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
    private final Map<String, ResourceGroupSelector> selectors;
    private final MBeanExporter mbeanExporter;
    private Executor executor;
    @Inject
    public SqlQueryQueueManager(Map<String, ? extends ResourceGroupSelector> selectors, MBeanExporter mbeanExporter)
    {
        this.mbeanExporter = requireNonNull(mbeanExporter, "mbeanExporter is null");
        this.selectors = ImmutableMap.copyOf(selectors);
        this.executor = null;
    }

    @Override
    public void setExecutor(Executor executor)
    {
        if (this.executor == null) {
            this.executor = executor;
            createAllQueues();
        }
    }

    @Override
    public boolean submit(Statement statement, QueryExecution queryExecution, SqlQueryManagerStats stats)
    {
        QueryQueue queue = selectQueue(statement, queryExecution.getSession(), executor);
        if (!queue.reserve(queryExecution)) {
            // Reject query if we couldn't acquire a permit to enter the queue.
            // The permits will be released when this query fails.
            return false;
        }
        queue.enqueue(createQueuedExecution(queryExecution, ImmutableList.of(), executor, stats));
        return true;
    }

    // Queues returned have already been created and added queryQueues
    private QueryQueue selectQueue(Statement statement, Session session, Executor executor)
    {
        ResourceGroupSelector selector = selectors.get(session.getSource().get());
        if (selector != null) {
            Optional<QueryQueueDefinition> queueDefinition = selector.match(statement, session.toSessionRepresentation());
            if (queueDefinition.isPresent()) {
                QueryQueue queue = queryQueues.get(queueDefinition.get());
                if (queue != null) {
                    return queue;
                }
            }
        }
        throw new PrestoException(USER_ERROR, "Query did not match any queuing rule ");
    }

    private void createAllQueues()
    {
        for (ResourceGroupSelector selector : selectors.values()) {
            QueryQueueDefinition definition = selector.getQueryQueueDefinition();
            if (!queryQueues.containsKey(definition)) {
                QueryQueue queue = new QueryQueue(executor, definition.getMaxQueued(), definition.getMaxConcurrent());
                if (queryQueues.putIfAbsent(definition, queue) == null) {
                    // Export the mbean, after checking for races
                    String objectName = ObjectNames.builder(
                            QueryQueue.class, definition.getName()).withProperty(
                            "expansion", definition.getName()).build();
                    mbeanExporter.export(objectName, queue);
                }
            }
        }
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
