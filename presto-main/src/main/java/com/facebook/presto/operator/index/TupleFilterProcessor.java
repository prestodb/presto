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

import com.facebook.presto.operator.PageProcessor;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Filters out rows that do not match the values from the specified tuple
 */
public class TupleFilterProcessor
        implements PageProcessor
{
    private final Page tuplePage;
    private final List<Type> outputTypes;
    private final int[] outputTupleChannels;

    public TupleFilterProcessor(Page tuplePage, List<Type> outputTypes, int[] outputTupleChannels)
    {
        requireNonNull(tuplePage, "tuplePage is null");
        checkArgument(tuplePage.getPositionCount() == 1, "tuplePage should only have one position");
        checkArgument(tuplePage.getChannelCount() > 0, "tuplePage must have at least one channel");
        requireNonNull(outputTypes, "outputTypes is null");
        requireNonNull(outputTupleChannels, "outputTupleChannels is null");
        checkArgument(tuplePage.getChannelCount() == outputTupleChannels.length, "tuplePage and outputTupleChannels have different number of channels");
        checkArgument(outputTypes.size() >= outputTupleChannels.length, "Must have at least as many output channels as those used for filtering");

        this.tuplePage = tuplePage;
        this.outputTypes = ImmutableList.copyOf(outputTypes);
        this.outputTupleChannels = outputTupleChannels;
    }

    @Override
    public int process(ConnectorSession session, Page page, int start, int end, PageBuilder pageBuilder)
    {
        // TODO: generate bytecode for this in the future
        for (int position = start; position < end; position++) {
            if (matches(position, page)) {
                pageBuilder.declarePosition();
                for (int i = 0; i < outputTypes.size(); i++) {
                    Type type = outputTypes.get(i);
                    Block block = page.getBlock(i);
                    BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(i);
                    type.appendTo(block, position, blockBuilder);
                }
            }
        }

        return end;
    }

    @Override
    public Page processColumnar(ConnectorSession session, Page page, List<? extends Type> types)
    {
        PageBuilder pageBuilder = new PageBuilder(types);
        int positionCount = page.getPositionCount();

        int[] selectedPositions = new int[positionCount];
        int selectedCount = 0;
        for (int i = 0; i < positionCount; i++) {
            if (matches(i, page)) {
                selectedPositions[selectedCount++] = i;
            }
        }

        for (int i = 0; i < outputTypes.size(); i++) {
            Type type = outputTypes.get(i);
            Block block = page.getBlock(i);
            BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(i);
            for (int position : selectedPositions) {
                type.appendTo(block, position, blockBuilder);
            }
        }
        pageBuilder.declarePositions(selectedCount);
        return pageBuilder.build();
    }

    @Override
    public Page processColumnarDictionary(ConnectorSession session, Page page, List<? extends Type> types)
    {
        return processColumnar(session, page, types);
    }

    private boolean matches(int position, Page page)
    {
        for (int i = 0; i < outputTupleChannels.length; i++) {
            Type type = outputTypes.get(outputTupleChannels[i]);
            Block outputBlock = page.getBlock(outputTupleChannels[i]);
            Block singleTupleBlock = tuplePage.getBlock(i);
            if (!type.equalTo(singleTupleBlock, 0, outputBlock, position)) {
                return false;
            }
        }
        return true;
    }
}
