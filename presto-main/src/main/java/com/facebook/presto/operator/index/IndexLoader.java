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
package com.facebook.presto.operator.index;

import com.facebook.presto.ScheduledSplit;
import com.facebook.presto.TaskSource;
import com.facebook.presto.operator.Driver;
import com.facebook.presto.operator.DriverFactory;
import com.facebook.presto.operator.LookupSource;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.operator.PageBuilder;
import com.facebook.presto.operator.PagesIndex;
import com.facebook.presto.operator.PipelineContext;
import com.facebook.presto.operator.TaskContext;
import com.facebook.presto.operator.index.PagesIndexBuilderOperator.PagesIndexBuilderOperatorFactory;
import com.facebook.presto.operator.index.UnloadedIndexKeyRecordSet.UnloadedIndexKeyRecordCursor;
import com.facebook.presto.spi.block.BlockCursor;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListenableFuture;

import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

@ThreadSafe
public class IndexLoader
{
    private final BlockingQueue<UpdateRequest> updateRequests = new LinkedBlockingQueue<>();

    private final List<Type> indexTypes;
    private final List<Type> outputTypes;
    private final DriverFactory driverFactory;
    private final PlanNodeId sourcePlanNodeId;

    private final AtomicReference<TaskContext> taskContextReference = new AtomicReference<>();
    private final Supplier<IndexSnapshotBuilder> indexSnapshotBuilderSupplier;

    private volatile IndexSnapshot indexSnapshot;

    public IndexLoader(final List<Integer> indexChannels,
            List<Type> types,
            final int snapshotOperatorId,
            DriverFactory driverFactory,
            final PagesIndexBuilderOperatorFactory pagesIndexOutput,
            final int expectedPositions)
    {
        checkArgument(!indexChannels.isEmpty(), "indexChannels must not be empty");
        this.outputTypes = ImmutableList.copyOf(checkNotNull(types, "types is null"));
        this.driverFactory = checkNotNull(driverFactory, "driverFactory is null");
        this.sourcePlanNodeId = Iterables.getOnlyElement(driverFactory.getSourceIds());

        ImmutableList.Builder<Type> typeBuilder = ImmutableList.builder();
        for (Integer outputIndexChannel : indexChannels) {
            typeBuilder.add(types.get(outputIndexChannel));
        }
        indexTypes = typeBuilder.build();

        indexSnapshotBuilderSupplier = Suppliers.memoize(new Supplier<IndexSnapshotBuilder>()
        {
            @Override
            public IndexSnapshotBuilder get()
            {
                TaskContext taskContext = taskContextReference.get();
                checkState(taskContext != null, "Task context must be set before index can be built");
                PipelineContext pipelineContext = taskContext.addPipelineContext(false, false);
                return new IndexSnapshotBuilder(pipelineContext, pagesIndexOutput, indexChannels, snapshotOperatorId, expectedPositions);
            }
        });

        indexSnapshot = new IndexSnapshot(new EmptyLookupSource(types.size()), new EmptyLookupSource(indexChannels.size()));
    }

    // This is a ghetto way to acquire a TaskContext at runtime (unavailable at planning)
    public void setContext(TaskContext taskContext)
    {
        taskContextReference.compareAndSet(null, taskContext);
    }

    public int getChannelCount()
    {
        return outputTypes.size();
    }

    public List<Type> getOutputTypes()
    {
        return outputTypes;
    }

    public IndexSnapshot getIndexSnapshot()
    {
        return indexSnapshot;
    }

    public IndexSnapshot getIndexSnapshotForKeys(BlockCursor[] indexCursors)
    {
        UpdateRequest updateRequest = new UpdateRequest(indexCursors);
        updateRequests.add(updateRequest);

        synchronized (this) {
            if (!updateRequest.isFinished()) {
                List<UpdateRequest> requests = new ArrayList<>(32);
                updateRequests.drainTo(requests);

                batchLoadRequests(requests);

                for (UpdateRequest request : requests) {
                    request.finished();
                }
            }
        }

        return indexSnapshot;
    }

    private synchronized void batchLoadRequests(List<UpdateRequest> requests)
    {
        IndexSnapshotBuilder indexSnapshotBuilder = indexSnapshotBuilderSupplier.get();

        UnloadedIndexKeyRecordSet unloadedKeysRecordSet = new UnloadedIndexKeyRecordSet(indexSnapshot, indexTypes, requests);

        // Drive index lookup to produce the output (landing in indexOutput)
        Driver driver = driverFactory.createDriver(indexSnapshotBuilder.getPipelineContext().addDriverContext());
        driver.updateSource(new TaskSource(sourcePlanNodeId, ImmutableSet.of(new ScheduledSplit(0, new IndexSplit(unloadedKeysRecordSet))), true));
        while (!driver.isFinished()) {
            ListenableFuture<?> process = driver.process();
            checkState(process.isDone(), "Driver should never block");
        }

        indexSnapshot = indexSnapshotBuilder.buildIndexSnapshot(unloadedKeysRecordSet.cursor());
    }

    @NotThreadSafe
    private static class IndexSnapshotBuilder
    {
        private final PipelineContext pipelineContext;
        private final List<Integer> indexChannels;

        private final PagesIndex pagesIndex;

        private final PagesIndex missingKeysIndex;
        private final List<Integer> missingKeysChannels;

        private LookupSource missingKeys;

        public IndexSnapshotBuilder(PipelineContext pipelineContext,
                PagesIndexBuilderOperatorFactory pagesIndexOutput,
                List<Integer> indexChannels,
                int snapshotOperatorId,
                int expectedPositions)
        {
            this.pipelineContext = pipelineContext;
            this.indexChannels = checkNotNull(indexChannels, "indexChannels is null");

            // create operator context to track this the memory of the index
            OperatorContext snapshotOperatorContext = pipelineContext.addDriverContext().addOperatorContext(snapshotOperatorId, IndexLoader.class.getSimpleName());

            this.pagesIndex = new PagesIndex(pagesIndexOutput.getTypes(), expectedPositions, snapshotOperatorContext);
            pagesIndexOutput.setPagesIndex(pagesIndex);

            ImmutableList.Builder<Type> missingKeysTypes = ImmutableList.builder();
            ImmutableList.Builder<Integer> missingKeysChannels = ImmutableList.builder();
            for (int i = 0; i < indexChannels.size(); i++) {
                Integer outputIndexChannel = indexChannels.get(i);
                missingKeysTypes.add(pagesIndexOutput.getTypes().get(outputIndexChannel));
                missingKeysChannels.add(i);
            }
            this.missingKeysIndex = new PagesIndex(missingKeysTypes.build(), expectedPositions, snapshotOperatorContext);
            this.missingKeysChannels = missingKeysChannels.build();
            this.missingKeys = new EmptyLookupSource(indexChannels.size());
        }

        public PipelineContext getPipelineContext()
        {
            return pipelineContext;
        }

        public IndexSnapshot buildIndexSnapshot(UnloadedIndexKeyRecordCursor unloadedKeyRecordCursor)
        {
            // Create lookup source with new data
            LookupSource lookupSource = pagesIndex.createLookupSource(indexChannels);

            // Build a page containing the keys that produced no output rows, so in future requests can skip these keys
            PageBuilder missingKeysPageBuilder = new PageBuilder(missingKeysIndex.getTypes());
            while (unloadedKeyRecordCursor.advanceNextPosition()) {
                BlockCursor[] cursors = unloadedKeyRecordCursor.asBlockCursors();
                if (lookupSource.getJoinPosition(cursors) < 0) {
                    for (int i = 0; i < cursors.length; i++) {
                        cursors[i].appendTo(missingKeysPageBuilder.getBlockBuilder(i));
                    }
                }
            }

            // only update missing keys if we have new missing keys
            if (!missingKeysPageBuilder.isEmpty()) {
                missingKeysIndex.addPage(missingKeysPageBuilder.build());
                missingKeys = missingKeysIndex.createLookupSource(missingKeysChannels);
            }

            return new IndexSnapshot(lookupSource, missingKeys);
        }
    }

    private static class EmptyLookupSource
            implements LookupSource
    {
        private final int channelCount;

        private EmptyLookupSource(int channelCount)
        {
            this.channelCount = channelCount;
        }

        @Override
        public int getChannelCount()
        {
            return channelCount;
        }

        @Override
        public long getJoinPosition(BlockCursor... cursors)
        {
            return IndexSnapshot.UNLOADED_INDEX_KEY;
        }

        @Override
        public long getNextJoinPosition(long currentPosition)
        {
            return IndexSnapshot.UNLOADED_INDEX_KEY;
        }

        @Override
        public void appendTo(long position, PageBuilder pageBuilder, int outputChannelOffset)
        {
            throw new UnsupportedOperationException();
        }
    }
}
