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

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.DictionaryBlock;
import com.facebook.presto.spi.block.LazyBlock;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.SizeOf;
import io.airlift.slice.Slice;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.slice.Slices.wrappedIntArray;
import static java.util.Arrays.copyOf;

public class GenericPageProcessor
        implements PageProcessor
{
    private final FilterFunction filterFunction;
    private final List<ProjectionFunction> projections;

    private final Block[] inputDictionaries;
    private final Block[] outputDictionaries;

    public GenericPageProcessor(FilterFunction filterFunction, Iterable<? extends ProjectionFunction> projections)
    {
        this.filterFunction = filterFunction;
        this.projections = ImmutableList.copyOf(projections);

        this.inputDictionaries = new Block[this.projections.size()];
        this.outputDictionaries = new Block[this.projections.size()];
    }

    @Override
    public int process(ConnectorSession session, Page page, int start, int end, PageBuilder pageBuilder)
    {
        int position = start;
        Block[] inputBlocks = page.getBlocks();

        for (; position < end && !pageBuilder.isFull(); position++) {
            if (filterFunction.filter(position, page.getBlocks())) {
                pageBuilder.declarePosition();
                for (int i = 0; i < projections.size(); i++) {
                    // todo: if the projection function increases the size of the data significantly, this could cause the servers to OOM
                    projections.get(i).project(position, inputBlocks, pageBuilder.getBlockBuilder(i));
                }
            }
        }
        return position;
    }

    @Override
    public Page processColumnar(ConnectorSession session, Page page, List<? extends Type> types)
    {
        int[] selectedPositions = filterPage(page);
        if (selectedPositions.length == 0) {
            return null;
        }

        if (projections.isEmpty()) {
            return new Page(selectedPositions.length);
        }

        PageBuilder pageBuilder = new PageBuilder(types);
        Block[] inputBlocks = page.getBlocks();

        for (int projectionIndex = 0; projectionIndex < projections.size(); projectionIndex++) {
            ProjectionFunction projection = projections.get(projectionIndex);
            projectColumnar(selectedPositions, pageBuilder.getBlockBuilder(projectionIndex), inputBlocks, projection);
        }
        pageBuilder.declarePositions(selectedPositions.length);
        return pageBuilder.build();
    }

    @Override
    public Page processColumnarDictionary(ConnectorSession session, Page page, List<? extends Type> types)
    {
        Page inputPage = getNonLazyPage(page);
        int[] selectedPositions = filterPage(inputPage);
        Map<UUID, UUID> dictionarySourceIds = new HashMap<>();

        if (selectedPositions.length == 0) {
            return null;
        }

        if (projections.isEmpty()) {
            return new Page(selectedPositions.length);
        }

        PageBuilder pageBuilder = new PageBuilder(types);
        Block[] inputBlocks = page.getBlocks();
        Block[] outputBlocks = new Block[projections.size()];

        for (int projectionIndex = 0; projectionIndex < projections.size(); projectionIndex++) {
            ProjectionFunction projection = projections.get(projectionIndex);

            if (canDictionaryProcess(projection, inputPage)) {
                outputBlocks[projectionIndex] = projectColumnarDictionary(inputPage, selectedPositions, projection, dictionarySourceIds);
            }
            else {
                outputBlocks[projectionIndex] = projectColumnar(selectedPositions, pageBuilder.getBlockBuilder(projectionIndex), inputBlocks, projection).build();
            }
        }

        for (Block block : outputBlocks) {
            verify(block.getPositionCount() == selectedPositions.length);
        }
        return new Page(selectedPositions.length, outputBlocks);
    }

    private Block projectColumnarDictionary(Page inputPage, int[] selectedPositions, ProjectionFunction projection, Map<UUID, UUID> dictionarySourceIds)
    {
        Block outputDictionary = projectDictionary(projection, inputPage);
        int[] outputIds = filterIds(projection, inputPage, selectedPositions);

        int inputChannel = getOnlyElement(projection.getInputChannels());
        DictionaryBlock dictionaryBlock = (DictionaryBlock) inputPage.getBlock(inputChannel);

        UUID sourceId = dictionarySourceIds.get(dictionaryBlock.getDictionarySourceId());
        if (sourceId == null) {
            sourceId = UUID.randomUUID();
            dictionarySourceIds.put(dictionaryBlock.getDictionarySourceId(), sourceId);
        }

        return new DictionaryBlock(selectedPositions.length, outputDictionary, wrappedIntArray(outputIds), false, sourceId);
    }

    private static BlockBuilder projectColumnar(int[] selectedPositions, BlockBuilder blockBuilder, Block[] inputBlocks, ProjectionFunction projection)
    {
        for (int position : selectedPositions) {
            projection.project(position, inputBlocks, blockBuilder);
        }
        return blockBuilder;
    }

    private static int[] filterIds(ProjectionFunction projection, Page page, int[] selectedPositions)
    {
        Slice ids = ((DictionaryBlock) page.getBlock(getOnlyElement(projection.getInputChannels()))).getIds();

        int[] outputIds = new int[selectedPositions.length];
        for (int pos = 0; pos < selectedPositions.length; pos++) {
            outputIds[pos] = ids.getInt(selectedPositions[pos] * SizeOf.SIZE_OF_INT);
        }
        return outputIds;
    }

    private Block projectDictionary(ProjectionFunction projection, Page page)
    {
        int inputChannel = getOnlyElement(projection.getInputChannels());
        Block dictionary = ((DictionaryBlock) page.getBlock(inputChannel)).getDictionary();

        int projectionIndex = projections.indexOf(projection);
        if (inputDictionaries[projectionIndex] == dictionary) {
            return outputDictionaries[projectionIndex];
        }

        BlockBuilder dictionaryBuilder = projection.getType().createBlockBuilder(new BlockBuilderStatus(), dictionary.getPositionCount());
        Block[] blocks = new Block[page.getChannelCount()];
        blocks[inputChannel] = dictionary;

        for (int i = 0; i < dictionary.getPositionCount(); i++) {
            projection.project(i, blocks, dictionaryBuilder);
        }

        inputDictionaries[projectionIndex] = dictionary;
        outputDictionaries[projectionIndex] = dictionaryBuilder.build();

        return outputDictionaries[projectionIndex];
    }

    private static boolean canDictionaryProcess(ProjectionFunction projection, Page inputPage)
    {
        Set<Integer> inputChannels = projection.getInputChannels();
        return projection.isDeterministic()
                && inputChannels.size() == 1
                && (inputPage.getBlock(getOnlyElement(inputChannels)) instanceof DictionaryBlock);
    }

    private Page getNonLazyPage(Page page)
    {
        ImmutableSet.Builder<Integer> builder = ImmutableSet.builder();
        for (ProjectionFunction projection : projections) {
            builder.addAll(projection.getInputChannels());
        }
        Set<Integer> inputChannels = builder.build();

        if (inputChannels.isEmpty()) {
            return page;
        }

        Block[] blocks = page.getBlocks();
        for (int inputChannel : inputChannels) {
            Block block = page.getBlock(inputChannel);
            if (block instanceof LazyBlock) {
                blocks[inputChannel] = ((LazyBlock) block).getBlock();
            }
        }
        return new Page(blocks);
    }

    private int[] filterPage(Page page)
    {
        int[] selected = new int[page.getPositionCount()];
        int index = 0;
        for (int position = 0; position < page.getPositionCount(); position++) {
            if (filterFunction.filter(position, page.getBlocks())) {
                selected[index] = position;
                index++;
            }
        }
        return copyOf(selected, index);
    }
}
