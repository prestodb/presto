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
package com.facebook.presto.operator;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.PageBuilder;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.Type;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.List;

import static com.facebook.presto.common.block.PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

/**
 * This page builder creates pages with dictionary blocks:
 * normal dictionary blocks for the probe side and the original blocks for the build side.
 * <p>
 * TODO use dictionary blocks (probably extended kind) to avoid data copying for build side
 */
public class LookupJoinPageBuilder
{
    private final IntArrayList probeIndexBuilder = new IntArrayList();
    private final PageBuilder buildPageBuilder;
    private final int buildOutputChannelCount;
    private int estimatedProbeBlockBytes;
    private int previousPosition = -1;
    private int estimatedProbeBytesPerRow = -1;
    private boolean isSequentialProbeIndices = true;

    public LookupJoinPageBuilder(List<Type> buildTypes)
    {
        this.buildPageBuilder = new PageBuilder(requireNonNull(buildTypes, "buildTypes is null"));
        this.buildOutputChannelCount = buildTypes.size();
    }

    public boolean isFull()
    {
        return estimatedProbeBlockBytes + buildPageBuilder.getSizeInBytes() >= DEFAULT_MAX_PAGE_SIZE_IN_BYTES || buildPageBuilder.isFull();
    }

    public boolean isEmpty()
    {
        return probeIndexBuilder.isEmpty() && buildPageBuilder.isEmpty();
    }

    public void reset()
    {
        // be aware that probeIndexBuilder will not clear its capacity
        probeIndexBuilder.clear();
        buildPageBuilder.reset();
        estimatedProbeBlockBytes = 0;
        estimatedProbeBytesPerRow = -1;
        previousPosition = -1;
        isSequentialProbeIndices = true;
    }

    /**
     * append the index for the probe and copy the row for the build
     */
    public void appendRow(JoinProbe probe, LookupSource lookupSource, long joinPosition)
    {
        // probe side
        appendProbeIndex(probe);

        // build side
        buildPageBuilder.declarePosition();
        lookupSource.appendTo(joinPosition, buildPageBuilder, 0);
    }

    /**
     * append the index for the probe and append nulls for the build
     */
    public void appendNullForBuild(JoinProbe probe)
    {
        // probe side
        appendProbeIndex(probe);

        // build side
        buildPageBuilder.declarePosition();
        for (int i = 0; i < buildOutputChannelCount; i++) {
            buildPageBuilder.getBlockBuilder(i).appendNull();
        }
    }

    public Page build(JoinProbe probe)
    {
        int outputPositions = probeIndexBuilder.size();
        verify(buildPageBuilder.getPositionCount() == outputPositions);

        int[] probeOutputChannels = probe.getOutputChannels();
        Block[] blocks = new Block[probeOutputChannels.length + buildOutputChannelCount];
        Page probePage = probe.getPage();
        if (!isSequentialProbeIndices || outputPositions == 0) {
            int[] probeIndices = probeIndexBuilder.toIntArray();
            for (int i = 0; i < probeOutputChannels.length; i++) {
                blocks[i] = probePage.getBlock(probeOutputChannels[i]).getPositions(probeIndices, 0, outputPositions);
            }
        }
        else {
            // probeIndices are sequential without holes
            int startRegion = probeIndexBuilder.getInt(0);
            verify(previousPosition - startRegion == outputPositions - 1);
            // probeIndices are a simple covering of the block, output the probe block directly
            boolean outputProbeBlocksDirectly = startRegion == 0 && outputPositions == probePage.getPositionCount();

            for (int i = 0; i < probeOutputChannels.length; i++) {
                Block block = probePage.getBlock(probeOutputChannels[i]);
                if (!outputProbeBlocksDirectly) {
                    // only a subregion of the block should be output
                    block = block.getRegion(startRegion, outputPositions);
                }
                blocks[i] = block;
            }
        }

        int offset = probeOutputChannels.length;
        for (int i = 0; i < buildOutputChannelCount; i++) {
            blocks[offset + i] = buildPageBuilder.getBlockBuilder(i).build();
            verify(blocks[offset + i].getPositionCount() == outputPositions);
        }
        return new Page(outputPositions, blocks);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("estimatedSize", estimatedProbeBlockBytes + buildPageBuilder.getSizeInBytes())
                .add("positionCount", buildPageBuilder.getPositionCount())
                .toString();
    }

    private void appendProbeIndex(JoinProbe probe)
    {
        int position = probe.getPosition();
        // positions to be appended should be in ascending order
        verify(position >= 0 && previousPosition <= position);
        isSequentialProbeIndices &= position == previousPosition + 1 || previousPosition == -1;

        // Update probe indices and size
        probeIndexBuilder.add(position);
        estimatedProbeBlockBytes += Integer.BYTES;

        // Update memory usage for probe side.
        //
        // The size of the probe cannot be easily calculated given
        // (1) the structure of Block is recursive,
        // (2) an inner block can serve as multiple views (e.g., in a dictionary block).
        //     Without a dedup at the granularity of rows, we cannot tell if we are overcounting, and
        // (3) even we are able to dedup magically, calling getRegionSizeInBytes can be expensive.
        //     For example, consider a dictionary block inside an array block;
        //     calling getRegionSizeInBytes(p, 1) of the array block can lead to calling getRegionSizeInBytes with an arbitrary length for the dictionary block,
        //     which is very expensive.
        //
        // To workaround the memory accounting complexity yet having a relatively reasonable estimation, we use sizeInBytes / positionCount as the size for each row.
        // It can be shown that the output page is bounded within range [buildPageBuilder.getSizeInBytes(), buildPageBuilder.getSizeInBytes + probe.getPage().getSizeInBytes()].
        //
        // This is under the assumption that the position of a probe is non-decreasing.
        // if position > previousPosition, we know it is a new row to append and we accumulate the estimated row size (sizeInBytes / positionCount);
        // otherwise we do not count because we know it is duplicated with the previous appended row.
        // So in the worst case, we can only accumulate up to the sizeInBytes of the probe page.
        //
        // On the other hand, we do not want to produce a page that is too small if the build size is too small (e.g., the build side is with all nulls).
        // That means we only appended a few small rows in the probe and reached the probe end.
        // But that is going to happen anyway because we have to flush the page whenever we reach the probe end.
        // So with or without precise memory accounting, the output page is small anyway.

        if (previousPosition != position) {
            previousPosition = position;
            if (estimatedProbeBytesPerRow < 0) {
                estimatedProbeBytesPerRow = computeEstimatedProbeBytesPerRow(probe);
            }
            estimatedProbeBlockBytes += estimatedProbeBytesPerRow;
        }
    }

    private static int computeEstimatedProbeBytesPerRow(JoinProbe probe)
    {
        Page probePage = probe.getPage();
        if (probePage.getPositionCount() == 0) {
            return 0;
        }
        // Estimate the size of an average probe page row
        int estimatedBytesPerRow = 0;
        for (int index : probe.getOutputChannels()) {
            estimatedBytesPerRow += probePage.getBlock(index).getSizeInBytes() / probePage.getPositionCount();
        }
        return estimatedBytesPerRow;
    }
}
