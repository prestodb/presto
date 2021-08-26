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
package com.facebook.presto.sql.planner;

import com.facebook.presto.Session;
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.common.Page;
import com.facebook.presto.execution.Lifespan;
import com.facebook.presto.execution.scheduler.ExecutionWriterTarget;
import com.facebook.presto.metadata.ConnectorMetadataUpdaterManager;
import com.facebook.presto.metadata.InsertTableHandle;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSmallFragmentCoalescingPlan;
import com.facebook.presto.spi.PageSinkContext;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.split.PageSinkManager;
import com.facebook.presto.split.PageSourceProvider;
import com.facebook.presto.split.SplitManager;
import com.facebook.presto.split.SplitSource;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.sun.management.ThreadMXBean;
import io.airlift.slice.Slice;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.facebook.airlift.concurrent.MoreFutures.getFutureValue;
import static com.facebook.presto.spi.connector.ConnectorSplitManager.SplitSchedulingStrategy.UNGROUPED_SCHEDULING;
import static com.facebook.presto.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Stream.concat;

public class SmallFragmentCoalescer
{
    private static final ThreadMXBean THREAD_MX_BEAN = (ThreadMXBean) ManagementFactory.getThreadMXBean();

    private final Session session;
    private final Metadata metadata;
    private final ExecutionWriterTarget target;
    private final PageSourceProvider pageSourceProvider;
    private final PageSinkManager pageSinkManager;
    private final ConnectorMetadataUpdaterManager metadataUpdaterManager;
    private final SplitManager splitManager;

    private final AtomicLong cpuNanosecondsConsumed = new AtomicLong();

    @VisibleForTesting
    public SmallFragmentCoalescer()
    {
        this.session = null;
        this.metadata = null;
        this.target = null;
        this.pageSinkManager = null;
        this.pageSourceProvider = null;
        this.metadataUpdaterManager = null;
        this.splitManager = null;
    }

    public SmallFragmentCoalescer(
            Session session,
            Metadata metadata,
            ExecutionWriterTarget target,
            PageSourceProvider pageSourceProvider,
            PageSinkManager pageSinkManager,
            ConnectorMetadataUpdaterManager metadataUpdaterManager,
            SplitManager splitManager)
    {
        this.session = requireNonNull(session, "session is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.target = requireNonNull(target, "target is null");
        this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
        this.pageSinkManager = requireNonNull(pageSinkManager, "pageSinkManager is null");
        this.metadataUpdaterManager = requireNonNull(metadataUpdaterManager, "metadataUpdaterManager is null");
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
    }

    public SmallFragmentCoalescingResult coalesceSmallFragments(Collection<Slice> fragments)
    {
        requireNonNull(fragments, "fragments is null");
        if (target instanceof ExecutionWriterTarget.InsertHandle) {
            InsertTableHandle handle = ((ExecutionWriterTarget.InsertHandle) target).getHandle();
            ConnectorSmallFragmentCoalescingPlan plan = metadata.createSmallFragmentCoalescingPlan(session, handle, fragments);
            List<ConnectorSmallFragmentCoalescingPlan.SubPlan> plans = plan.getSubPlans().collect(Collectors.toList());
            ThreadFactory threadFactory = new ThreadFactoryBuilder()
                    .setNameFormat(session.getQueryId().getId() + " fragment coalescing page producer")
                    .setDaemon(true)
                    .build();
            int pageProducingParallelism = SystemSessionProperties.getSmallFragmentCoalescingScanParallelism(session);
            ListeningExecutorService pageProducerExecutor = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(pageProducingParallelism, threadFactory));
            try {
                Collection<Slice> resultFragments = concat(
                        plans.stream().flatMap(subPlan -> executeCoalescingSubPlan(session, handle, subPlan, pageProducerExecutor)),
                        plan.getRetainedFragments().stream())
                        .collect(toImmutableList());
                return new SmallFragmentCoalescingResult(resultFragments, plan.getDeprecatedFragments(), cpuNanosecondsConsumed.get());
            }
            finally {
                pageProducerExecutor.shutdownNow();
            }
        }
        else {
            return new SmallFragmentCoalescingResult(ImmutableSet.copyOf(fragments), ImmutableSet.of(), 1);
        }
    }

    private Stream<Slice> executeCoalescingSubPlan(Session session, InsertTableHandle insertTableHandle, ConnectorSmallFragmentCoalescingPlan.SubPlan subPlan, ListeningExecutorService pageProducerExecutor)
    {
        PageSinkContext.Builder pageSinkContextBuilder = PageSinkContext.builder().setCommitRequired(false);
        metadataUpdaterManager.getMetadataUpdater(insertTableHandle.getConnectorId()).ifPresent(pageSinkContextBuilder::setConnectorMetadataUpdater);
        ConnectorPageSink pageSink = pageSinkManager.createPageSink(session, insertTableHandle, pageSinkContextBuilder.build());
        TableHandle tableHandle = new TableHandle(insertTableHandle.getConnectorId(), subPlan.getConnectorTableHandle(), insertTableHandle.getTransactionHandle(), Optional.of(subPlan.getTableLayoutHandle()));
        BlockingQueue<Page> pages = new LinkedBlockingQueue<>(32);
        ListenableFuture<List<Long>> pageProducersFuture = createSmallFragmentPageProducers(pages, session, tableHandle, subPlan, pageProducerExecutor);
        try {
            while (!pageProducersFuture.isDone() || !pages.isEmpty()) {
                Page p = pages.poll(1, TimeUnit.SECONDS);
                if (p == null) {
                    continue;
                }
                pageSink.appendPage(p);
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        cpuNanosecondsConsumed.addAndGet(getFutureValue(pageProducersFuture).stream().reduce(Long::sum).orElse(0L));
        Stream<Slice> fragments = getFutureValue(pageSink.finish()).stream();
        return fragments;
    }

    private ListenableFuture<List<Long>> createSmallFragmentPageProducers(BlockingQueue<Page> pages, Session session, TableHandle tableHandle, ConnectorSmallFragmentCoalescingPlan.SubPlan subPlan, ListeningExecutorService es)
    {
        List<ListenableFuture<Long>> producerFutures = new ArrayList<>();
        try (SplitSource splitSource = splitManager.getSplits(session, tableHandle, UNGROUPED_SCHEDULING, WarningCollector.NOOP)) {
            while (!splitSource.isFinished()) {
                SplitSource.SplitBatch splitBatch = getFutureValue(splitSource.getNextBatch(NOT_PARTITIONED, Lifespan.taskWide(), 1000), PrestoException.class);
                for (Split split : splitBatch.getSplits()) {
                    ListenableFuture<Long> producerFuture = es.submit(() -> {
                        long startThreadCpuTime = THREAD_MX_BEAN.getCurrentThreadCpuTime();
                        try (ConnectorPageSource source = pageSourceProvider.createPageSource(session, split, tableHandle, subPlan.getColumns())) {
                            while (!source.isFinished()) {
                                Page page = source.getNextPage();
                                if (page == null) {
                                    continue;
                                }
                                try {
                                    pages.put(page.getLoadedPage());
                                }
                                catch (InterruptedException e) {
                                    break;
                                }
                            }
                        }
                        return THREAD_MX_BEAN.getCurrentThreadCpuTime() - startThreadCpuTime;
                    });
                    producerFutures.add(producerFuture);
                }
            }
        }
        return Futures.allAsList(producerFutures);
    }

    public static class SmallFragmentCoalescingResult
    {
        public final Collection<Slice> fragments;
        // Replaced and need to be deleted
        public final Collection<Slice> deprecatedFragments;
        public final long cpuNanosecondsConsumed;

        public SmallFragmentCoalescingResult(Collection<Slice> fragments, Collection<Slice> deprecatedFragments, long cpuNanosecondsConsumed)
        {
            this.fragments = requireNonNull(fragments, "fragments is null");
            this.deprecatedFragments = requireNonNull(deprecatedFragments, "deprecatedFragments is null");
            this.cpuNanosecondsConsumed = cpuNanosecondsConsumed;
        }
    }
}
