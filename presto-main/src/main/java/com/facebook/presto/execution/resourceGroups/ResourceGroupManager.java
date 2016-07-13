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
import com.facebook.presto.execution.QueryExecution;
import com.facebook.presto.execution.QueryQueueManager;
import com.facebook.presto.execution.resourceGroups.ResourceGroup.RootResourceGroup;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.sql.tree.Statement;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import org.weakref.jmx.JmxException;
import org.weakref.jmx.MBeanExporter;
import org.weakref.jmx.ObjectNames;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.facebook.presto.SystemSessionProperties.getQueryPriority;
import static com.facebook.presto.spi.StandardErrorCode.QUERY_REJECTED;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

@ThreadSafe
public class ResourceGroupManager
        implements QueryQueueManager
{
    private static final Logger log = Logger.get(ResourceGroupManager.class);
    private final ScheduledExecutorService refreshExecutor = newSingleThreadScheduledExecutor(daemonThreadsNamed("ResourceGroupManager"));
    private final List<RootResourceGroup> rootGroups = new CopyOnWriteArrayList<>();
    private final ConcurrentMap<ResourceGroupId, ResourceGroup> groups = new ConcurrentHashMap<>();
    private final List<ResourceGroupSelector> selectors;
    private final ResourceGroupConfigurationManager configurationManager;
    private final MBeanExporter exporter;
    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicLong lastCpuQuotaGenerationNanos = new AtomicLong(System.nanoTime());

    @Inject
    public ResourceGroupManager(List<? extends ResourceGroupSelector> selectors, ResourceGroupConfigurationManager configurationManager, MBeanExporter exporter)
    {
        this.exporter = requireNonNull(exporter, "exporter is null");
        this.selectors = ImmutableList.copyOf(selectors);
        this.configurationManager = requireNonNull(configurationManager, "configurationManager is null");
    }

    public ResourceGroupInfo getResourceGroupInfo(ResourceGroupId id)
    {
        checkArgument(groups.containsKey(id), "Group %s does not exist", id);
        return groups.get(id).getInfo();
    }

    @Override
    public void submit(Statement statement, QueryExecution queryExecution, Executor executor)
    {
        ResourceGroupId group;
        try {
            group = selectGroup(statement, queryExecution.getSession());
        }
        catch (PrestoException e) {
            queryExecution.fail(e);
            return;
        }
        createGroupIfNecessary(group, queryExecution.getSession(), executor);
        groups.get(group).run(queryExecution);
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
        long nanoTime = System.nanoTime();
        long elapsedSeconds = NANOSECONDS.toSeconds(nanoTime - lastCpuQuotaGenerationNanos.get());
        if (elapsedSeconds > 0) {
            // Only advance our clock on second boundaries to avoid calling generateCpuQuota() too frequently, and because it would be a no-op for zero seconds.
            lastCpuQuotaGenerationNanos.addAndGet(elapsedSeconds * 1_000_000_000L);
        }
        else if (elapsedSeconds < 0) {
            // nano time has overflowed
            lastCpuQuotaGenerationNanos.set(nanoTime);
        }
        for (RootResourceGroup group : rootGroups) {
            try {
                if (elapsedSeconds > 0) {
                    group.generateCpuQuota(elapsedSeconds);
                }
            }
            catch (RuntimeException e) {
                log.error(e, "Exception while generation cpu quota for %s", group);
            }
            try {
                group.processQueuedQueries();
            }
            catch (RuntimeException e) {
                log.error(e, "Exception while processing queued queries for %s", group);
            }
        }
    }

    private synchronized void createGroupIfNecessary(ResourceGroupId id, Session session, Executor executor)
    {
        SelectionContext context = new SelectionContext(session.getIdentity().getPrincipal().isPresent(), session.getUser(), session.getSource(), getQueryPriority(session));
        if (!groups.containsKey(id)) {
            ResourceGroup group;
            if (id.getParent().isPresent()) {
                createGroupIfNecessary(id.getParent().get(), session, executor);
                ResourceGroup parent = groups.get(id.getParent().get());
                requireNonNull(parent, "parent is null");
                group = parent.getOrCreateSubGroup(id.getLastSegment());
            }
            else {
                RootResourceGroup root = new RootResourceGroup(id.getSegments().get(0), this::exportGroup, executor);
                group = root;
                rootGroups.add(root);
            }
            configurationManager.configure(group, context);
            checkState(groups.put(id, group) == null, "Unexpected existing resource group");
        }
    }

    private void exportGroup(ResourceGroup group, Boolean export)
    {
        String objectName = ObjectNames.builder(ResourceGroup.class, group.getId().toString()).build();
        try {
            if (export) {
                exporter.export(objectName, group);
            }
            else {
                exporter.unexport(objectName);
            }
        }
        catch (JmxException e) {
            log.error(e, "Error %s resource group %s", export ? "exporting" : "unexporting", group.getId());
        }
    }

    private ResourceGroupId selectGroup(Statement statement, Session session)
    {
        SelectionContext context = new SelectionContext(session.getIdentity().getPrincipal().isPresent(), session.getUser(), session.getSource(), getQueryPriority(session));
        for (ResourceGroupSelector selector : selectors) {
            Optional<ResourceGroupId> group = selector.match(statement, context);
            if (group.isPresent()) {
                return group.get();
            }
        }
        throw new PrestoException(QUERY_REJECTED, "Query did not match any selection rule");
    }
}
