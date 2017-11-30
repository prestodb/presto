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

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.List;

import static com.facebook.presto.spi.block.PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

/**
 * This page builder creates pages with dictionary blocks:
 * normal dictionary blocks for the probe side and the original blocks for the build side.
 *
 * TODO use dictionary blocks (probably extended kind) to avoid data copying for build side
 */
public class DictionaryLookupJoinPageBuilder
        implements LookupJoinPageBuilder
{
    private final IntArrayList probeIndexBuilder = new IntArrayList();
    private final PageBuilder buildPageBuilder;
    private final int buildOutputChannelCount;
    private int probeBlockBytes;
    private boolean isSequentialProbeIndices = true;

    public DictionaryLookupJoinPageBuilder(List<Type> buildTypes)
    {
        this.buildPageBuilder = new PageBuilder(requireNonNull(buildTypes, "buildTypes is null"));
        this.buildOutputChannelCount = buildTypes.size();
    }

    public boolean isFull()
    {
        return probeBlockBytes + buildPageBuilder.getSizeInBytes() >= DEFAULT_MAX_PAGE_SIZE_IN_BYTES || buildPageBuilder.isFull();
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
        probeBlockBytes = 0;
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
        Block[] blocks = new Block[probe.getOutputChannelCount() + buildOutputChannelCount];
        int[] probeIndices = probeIndexBuilder.toIntArray();
        int length = probeIndices.length;
        verify(buildPageBuilder.getPositionCount() == length);

        int[] probeOutputChannels = probe.getOutputChannels();
        for (int i = 0; i < probe.getOutputChannelCount(); i++) {
            Block probeBlock = probe.getPage().getBlock(probeOutputChannels[i]);
            if (!isSequentialProbeIndices || length == 0) {
                blocks[i] = probeBlock.getPositions(probeIndices);
            }
            else if (length == probeBlock.getPositionCount()) {
                // probeIndices are a simple covering of the block
                verify(probeIndices[0] == 0);
                verify(probeIndices[length - 1] == length - 1);
                blocks[i] = probeBlock;
            }
            else {
                // probeIndices are sequential without holes
                verify(probeIndices[length - 1] - probeIndices[0] == length - 1);
                blocks[i] = probeBlock.getRegion(probeIndices[0], length);
            }
        }

        Page buildPage = buildPageBuilder.build();
        int offset = probe.getOutputChannelCount();
        for (int i = 0; i < buildOutputChannelCount; i++) {
            blocks[offset + i] = buildPage.getBlock(i);
        }
        return new Page(buildPageBuilder.getPositionCount(), blocks);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("estimatedSize", probeBlockBytes + buildPageBuilder.getSizeInBytes())
                .add("positionCount", buildPageBuilder.getPositionCount())
                .toString();
    }

    private void appendProbeIndex(JoinProbe probe)
    {
        int position = probe.getPosition();
        verify(position >= 0);
        int previousPosition = probeIndexBuilder.isEmpty() ? -1 : probeIndexBuilder.get(probeIndexBuilder.size() - 1);
        // positions to be appended should be in ascending order
        verify(previousPosition <= position);
        isSequentialProbeIndices &= position == previousPosition + 1 || previousPosition == -1;

        probeIndexBuilder.add(position);

        // update memory usage
        if (previousPosition == position) {
            return;
        }
        for (int index : probe.getOutputChannels()) {
            // be aware that getRegionSizeInBytes could be expensive
            probeBlockBytes += probe.getPage().getBlock(index).getRegionSizeInBytes(position, 1);
        }
    }
}
