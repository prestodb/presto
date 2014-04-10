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
import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.block.uncompressed.UncompressedBlock;
import com.facebook.presto.block.uncompressed.UncompressedBooleanBlockCursor;
import com.facebook.presto.block.uncompressed.UncompressedDoubleBlockCursor;
import com.facebook.presto.block.uncompressed.UncompressedLongBlockCursor;
import com.facebook.presto.block.uncompressed.UncompressedSliceBlockCursor;
import com.facebook.presto.operator.Driver;
import com.facebook.presto.operator.DriverFactory;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.operator.Page;
import com.facebook.presto.operator.PagesIndex;
import com.facebook.presto.operator.PipelineContext;
import com.facebook.presto.operator.TaskContext;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.Slice;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.operator.index.PagesIndexBuilderOperator.PagesIndexSupplier;
import static com.facebook.presto.operator.index.UnloadedIndexKeyRecordSet.UnloadedIndexKeyRecordCursor;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

@ThreadSafe
public class IndexLoader
{
    private final BlockingQueue<UpdateRequest> updateRequests = new LinkedBlockingQueue<>();
    private final Context context = new Context();

    private final int[] outputIndexChannels;
    private final List<TupleInfo.Type> indexTypes;
    private final List<TupleInfo> indexTupleInfos;
    private final List<TupleInfo> outputTupleInfos;
    private final int snapshotOperatorId;
    private final DriverFactory driverFactory;
    private final PagesIndexSupplier indexOutput;

    // TODO: add eviction policy to index
    private volatile IndexSnapshot snapshot = IndexSnapshot.EMPTY_SNAPSHOT;

    public IndexLoader(List<Integer> outputIndexChannels, List<TupleInfo> outputTupleInfos, int snapshotOperatorId, DriverFactory driverFactory, PagesIndexSupplier indexOutput)
    {
        checkArgument(!outputIndexChannels.isEmpty(), "outputIndexChannels must not be empty");
        this.outputIndexChannels = Ints.toArray(checkNotNull(outputIndexChannels, "outputIndexChannels is null"));
        this.outputTupleInfos = ImmutableList.copyOf(checkNotNull(outputTupleInfos, "outputTupleInfos is null"));
        this.snapshotOperatorId = snapshotOperatorId;
        this.driverFactory = checkNotNull(driverFactory, "driverFactory is null");
        this.indexOutput = checkNotNull(indexOutput, "indexOutput is null");

        ImmutableList.Builder<TupleInfo.Type> typeBuilder = ImmutableList.builder();
        ImmutableList.Builder<TupleInfo> tupleInfoBuilder = ImmutableList.builder();
        for (Integer outputIndexChannel : outputIndexChannels) {
            typeBuilder.add(outputTupleInfos.get(outputIndexChannel).getType());
            tupleInfoBuilder.add(outputTupleInfos.get(outputIndexChannel));
        }
        indexTypes = typeBuilder.build();
        indexTupleInfos = tupleInfoBuilder.build();
    }

    // This is a ghetto way to acquire a TaskContext at runtime (unavailable at planning)
    public void setContext(TaskContext taskContext)
    {
        context.setContext(taskContext, snapshotOperatorId);
    }

    private static class Context
    {
        @GuardedBy("this")
        private PipelineContext pipelineContext;

        @GuardedBy("this")
        // This OperatorContext is used SOLELY for tracking IndexSnapshot memory consumption
        private OperatorContext snapshotOperatorContext;

        public synchronized void setContext(TaskContext taskContext, int snapshotOperatorId)
        {
            if (snapshotOperatorContext == null) {
                pipelineContext = taskContext.addPipelineContext(false, false);
                snapshotOperatorContext = pipelineContext.addDriverContext().addOperatorContext(snapshotOperatorId, IndexLoader.class.getSimpleName());
            }
        }

        public synchronized boolean isInitialized()
        {
            return snapshotOperatorContext != null;
        }

        public synchronized PipelineContext getPipelineContext()
        {
            return pipelineContext;
        }

        public synchronized OperatorContext getSnapshotOperatorContext()
        {
            return snapshotOperatorContext;
        }
    }

    public int getChannelCount()
    {
        return outputTupleInfos.size();
    }

    public List<TupleInfo> getOutputTupleInfos()
    {
        return outputTupleInfos;
    }

    public IndexSnapshotReader getReader()
    {
        return new IndexSnapshotReader(snapshot);
    }

    public IndexSnapshotReader getUpdatedReader(BlockCursor[] indexCursors)
    {
        Page page = derivePage(indexCursors);
        updateSnapshot(page);
        return new IndexSnapshotReader(snapshot);
    }

    // Total HACK to get more values to process from a cursor
    private Block deriveBlock(BlockCursor cursor)
    {
        checkArgument(cursor.isValid(), "cursor expected to be in a valid state");
        checkArgument(cursor instanceof UncompressedLongBlockCursor ||
                cursor instanceof UncompressedDoubleBlockCursor ||
                cursor instanceof UncompressedBooleanBlockCursor ||
                cursor instanceof UncompressedSliceBlockCursor);

        Slice rawSlice = cursor.getRawSlice();
        int rawOffset = cursor.getRawOffset();
        Slice slice = rawSlice.slice(rawOffset, rawSlice.length() - rawOffset);
        int positionCount = cursor.getRemainingPositions() + 1;
        return new UncompressedBlock(positionCount, cursor.getTupleInfo(), slice);
    }

    private Page derivePage(BlockCursor[] cursors)
    {
        Block[] blocks = new Block[cursors.length];
        for (int i = 0; i < cursors.length; i++) {
            blocks[i] = deriveBlock(cursors[i]);
        }
        return new Page(blocks);
    }

    private void updateSnapshot(Page page)
    {
        UpdateRequest updateRequest = new UpdateRequest(page);
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
    }

    private synchronized void batchLoadRequests(List<UpdateRequest> requests)
    {
        checkState(context.isInitialized(), "Context not yet initialized for IndexLoader!");

        List<Page> pages = Lists.transform(requests, UpdateRequest.pageGetter());
        UnloadedIndexKeyRecordSet unloadedIndexKeys = new UnloadedIndexKeyRecordSet(snapshot, indexTypes, context.getPipelineContext(), pages);

        // Drive index lookup to produce the output (landing in indexOutput)
        Driver driver = driverFactory.createDriver(context.getPipelineContext().addDriverContext());
        PlanNodeId sourcePlanNodeId = Iterables.getOnlyElement(driver.getSourceIds());
        driver.updateSource(new TaskSource(sourcePlanNodeId, ImmutableSet.of(new ScheduledSplit(0, new IndexSplit(unloadedIndexKeys))), true));
        while (!driver.isFinished()) {
            ListenableFuture<?> process = driver.process();
            checkState(process.isDone(), "Driver should never block");
        }
        // Note: the pagesIndex memory reservation will be removed when the above Driver finishes,
        // but it will be accounted for again in our index snapshot.
        PagesIndex pagesIndex = Futures.getUnchecked(indexOutput.getPagesIndex());

        // Construct the new IndexSnapshot
        IndexSnapshot.Builder builder = new IndexSnapshot.Builder(snapshot, outputIndexChannels, indexTupleInfos, context.getSnapshotOperatorContext())
                .addFragment(pagesIndex);

        // Make sure that every key is recorded in the snapshot, even those that produced no output rows
        UnloadedIndexKeyRecordCursor cursor = unloadedIndexKeys.cursor();
        while (cursor.advanceNextPosition()) {
            builder.ensureKey(cursor.asBlockCursors());
        }

        snapshot = builder.build();
    }

    private static class UpdateRequest
    {
        private final Page page;
        private final AtomicBoolean finished = new AtomicBoolean();

        private UpdateRequest(Page page)
        {
            this.page = checkNotNull(page, "page is null");
        }

        private Page getPage()
        {
            return page;
        }

        public void finished()
        {
            finished.set(true);
        }

        public boolean isFinished()
        {
            return finished.get();
        }

        public static Function<UpdateRequest, Page> pageGetter()
        {
            return new Function<UpdateRequest, Page>()
            {
                @Override
                public Page apply(UpdateRequest updateRequest)
                {
                    return updateRequest.getPage();
                }
            };
        }
    }
}
