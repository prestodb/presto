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
package com.facebook.presto.execution.resourceGroups;

import com.facebook.presto.Session;
import com.facebook.presto.SessionRepresentation;
import com.facebook.presto.execution.QueryExecution;
import com.facebook.presto.execution.QueryQueueDefinition;
import com.facebook.presto.execution.QueryQueueManager;
import com.facebook.presto.execution.SqlQueryManagerStats;
import com.facebook.presto.execution.resourceGroups.ResourceGroup.RootResourceGroup;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.sql.tree.Statement;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.spi.StandardErrorCode.USER_ERROR;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class ResourceGroupManager
        implements QueryQueueManager
{
    private static final Logger log = Logger.get(ResourceGroupManager.class);
    private final ScheduledExecutorService refreshExecutor = Executors.newSingleThreadScheduledExecutor();
    private final List<RootResourceGroup> rootGroups = new CopyOnWriteArrayList<>();
    private final ConcurrentMap<ResourceGroupId, ResourceGroup> groups = new ConcurrentHashMap<>();
    private final List<ResourceGroupSelector> selectors;
    private final ResourceGroupConfigurationManager configurationManager;
    private final AtomicBoolean started = new AtomicBoolean();

    @Inject
    public ResourceGroupManager(List<? extends ResourceGroupSelector> selectors, ResourceGroupConfigurationManager configurationManager)
    {
        this.selectors = ImmutableList.copyOf(selectors);
        this.configurationManager = requireNonNull(configurationManager, "configurationManager is null");
    }

    @Override
    public boolean submit(Statement statement, QueryExecution queryExecution, Executor executor, SqlQueryManagerStats stats)
    {
        List<QueryQueueDefinition> queues = selectQueues(statement, queryExecution.getSession());

        queues = new ArrayList<>(queues);
        Collections.reverse(queues);
        ResourceGroupId groupId = new ResourceGroupId(queues.get(0).getExpandedTemplate(queryExecution.getSession()));
        createGroupIfNecessary(groupId, queryExecution.getSession().toSessionRepresentation(), executor);
        for (QueryQueueDefinition queue : queues.subList(1, queues.size())) {
            groupId = new ResourceGroupId(groupId, queue.getExpandedTemplate(queryExecution.getSession()));
            createGroupIfNecessary(groupId, queryExecution.getSession().toSessionRepresentation(), executor);
        }

        return groups.get(groupId).add(queryExecution);
    }

    @PreDestroy
    public void destroy()
    {
        refreshExecutor.shutdownNow();
    }

    @PostConstruct
    public void start()
    {
        if (started.compareAndSet(false, true)) {
            refreshExecutor.scheduleWithFixedDelay(this::refreshAndStartQueries, 1, 1, TimeUnit.MILLISECONDS);
        }
    }

    private void refreshAndStartQueries()
    {
        for (RootResourceGroup group : rootGroups) {
            try {
                group.processQueuedQueries();
            }
            catch (RuntimeException e) {
                log.error(e, "Exception while processing queued queries for %s", group);
            }
        }
    }

    private synchronized void createGroupIfNecessary(ResourceGroupId id, SessionRepresentation session, Executor executor)
    {
        if (!groups.containsKey(id)) {
            ResourceGroup group;
            if (id.getParent().isPresent()) {
                ResourceGroup parent = groups.get(id.getParent().get());
                requireNonNull(parent, "parent is null");
                group = parent.getOrCreateSubGroup(id.getLastSegment());
            }
            else {
                RootResourceGroup root = new RootResourceGroup(id.getSegments().get(0), executor);
                group = root;
                rootGroups.add(root);
            }
            configurationManager.configure(group, session);
            checkState(groups.put(id, group) == null, "Unexpected existing resource group");
        }
    }

    // Queues returned have already been created and added queryQueues
    private List<QueryQueueDefinition> selectQueues(Statement statement, Session session)
    {
        for (ResourceGroupSelector selector : selectors) {
            Optional<List<QueryQueueDefinition>> queues = selector.match(statement, session.toSessionRepresentation());
            if (queues.isPresent()) {
                List<QueryQueueDefinition> definitions = queues.get();
                checkState(!definitions.isEmpty(), selector + " returned empty list of queue definitions");
                return definitions;
            }
        }
        throw new PrestoException(USER_ERROR, "Query did not match any selection rule");
    }
}
