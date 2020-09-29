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

import com.facebook.presto.common.NotSupportedException;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.PageBuilder;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.type.TypeUtils;
import com.google.common.collect.ImmutableList;
import org.openjdk.jol.info.ClassLayout;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static com.facebook.presto.common.function.OperatorType.IS_DISTINCT_FROM;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.util.Failures.internalError;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class SimplePagesHashStrategy
        implements PagesHashStrategy
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(SimplePagesHashStrategy.class).instanceSize();
    private final List<Type> types;
    private final List<Integer> outputChannels;
    private final List<List<Block>> channels;
    private final List<Integer> hashChannels;
    private final List<Block> precomputedHashChannel;
    private final Optional<Integer> sortChannel;
    private final boolean groupByUsesEqualTo;
    private final List<MethodHandle> distinctFromMethodHandles;

    public SimplePagesHashStrategy(
            List<Type> types,
            List<Integer> outputChannels,
            List<List<Block>> channels,
            List<Integer> hashChannels,
            OptionalInt precomputedHashChannel,
            Optional<Integer> sortChannel,
            FunctionAndTypeManager functionAndTypeManager,
            boolean groupByUsesEqualTo)
    {
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        this.outputChannels = ImmutableList.copyOf(requireNonNull(outputChannels, "outputChannels is null"));
        this.channels = ImmutableList.copyOf(requireNonNull(channels, "channels is null"));

        checkArgument(types.size() == channels.size(), "Expected types and channels to be the same length");
        this.hashChannels = ImmutableList.copyOf(requireNonNull(hashChannels, "hashChannels is null"));
        if (precomputedHashChannel.isPresent()) {
            this.precomputedHashChannel = channels.get(precomputedHashChannel.getAsInt());
        }
        else {
            this.precomputedHashChannel = null;
        }
        this.sortChannel = requireNonNull(sortChannel, "sortChannel is null");
        requireNonNull(functionAndTypeManager, "functionManager is null");
        this.groupByUsesEqualTo = groupByUsesEqualTo;
        ImmutableList.Builder<MethodHandle> distinctFromMethodHandlesBuilder = ImmutableList.builder();
        for (Type type : types) {
            distinctFromMethodHandlesBuilder.add(
                    functionAndTypeManager.getBuiltInScalarFunctionImplementation(functionAndTypeManager.resolveOperator(IS_DISTINCT_FROM, fromTypes(type, type))).getMethodHandle());
        }
        distinctFromMethodHandles = distinctFromMethodHandlesBuilder.build();
    }

    @Override
    public int getChannelCount()
    {
        return outputChannels.size();
    }

    @Override
    public long getSizeInBytes()
    {
        return INSTANCE_SIZE + channels.stream()
                .flatMap(List::stream)
                .mapToLong(Block::getRetainedSizeInBytes)
                .sum();
    }

    @Override
    public void appendTo(int blockIndex, int position, PageBuilder pageBuilder, int outputChannelOffset)
    {
        for (int outputIndex : outputChannels) {
            Type type = types.get(outputIndex);
            List<Block> channel = channels.get(outputIndex);
            Block block = channel.get(blockIndex);
            type.appendTo(block, position, pageBuilder.getBlockBuilder(outputChannelOffset));
            outputChannelOffset++;
        }
    }

    @Override
    public long hashPosition(int blockIndex, int position)
    {
        if (precomputedHashChannel != null) {
            return BIGINT.getLong(precomputedHashChannel.get(blockIndex), position);
        }
        long result = 0;
        for (int hashChannel : hashChannels) {
            Type type = types.get(hashChannel);
            Block block = channels.get(hashChannel).get(blockIndex);
            result = result * 31 + TypeUtils.hashPosition(type, block, position);
        }
        return result;
    }

    @Override
    public long hashRow(int position, Page page)
    {
        long result = 0;
        for (int i = 0; i < hashChannels.size(); i++) {
            int hashChannel = hashChannels.get(i);
            Type type = types.get(hashChannel);
            Block block = page.getBlock(i);
            result = result * 31 + TypeUtils.hashPosition(type, block, position);
        }
        return result;
    }

    @Override
    public boolean rowEqualsRow(int leftPosition, Page leftPage, int rightPosition, Page rightPage)
    {
        for (int i = 0; i < hashChannels.size(); i++) {
            int hashChannel = hashChannels.get(i);
            Type type = types.get(hashChannel);
            Block leftBlock = leftPage.getBlock(i);
            Block rightBlock = rightPage.getBlock(i);
            if (!TypeUtils.positionEqualsPosition(type, leftBlock, leftPosition, rightBlock, rightPosition)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean positionEqualsRow(int leftBlockIndex, int leftPosition, int rightPosition, Page rightPage)
    {
        for (int i = 0; i < hashChannels.size(); i++) {
            int hashChannel = hashChannels.get(i);
            Type type = types.get(hashChannel);
            Block leftBlock = channels.get(hashChannel).get(leftBlockIndex);
            Block rightBlock = rightPage.getBlock(i);
            if (!TypeUtils.positionEqualsPosition(type, leftBlock, leftPosition, rightBlock, rightPosition)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean positionEqualsRowIgnoreNulls(int leftBlockIndex, int leftPosition, int rightPosition, Page rightPage)
    {
        for (int i = 0; i < hashChannels.size(); i++) {
            int hashChannel = hashChannels.get(i);
            Type type = types.get(hashChannel);
            Block leftBlock = channels.get(hashChannel).get(leftBlockIndex);
            Block rightBlock = rightPage.getBlock(i);
            try {
                if (!type.equalTo(leftBlock, leftPosition, rightBlock, rightPosition)) {
                    return false;
                }
            }
            catch (NotSupportedException e) {
                throw new PrestoException(NOT_SUPPORTED, e.getMessage(), e);
            }
        }
        return true;
    }

    @Override
    public boolean positionEqualsRow(int leftBlockIndex, int leftPosition, int rightPosition, Page page, int[] rightHashChannels)
    {
        for (int i = 0; i < hashChannels.size(); i++) {
            int hashChannel = hashChannels.get(i);
            Type type = types.get(hashChannel);
            Block leftBlock = channels.get(hashChannel).get(leftBlockIndex);
            Block rightBlock = page.getBlock(rightHashChannels[i]);
            if (!TypeUtils.positionEqualsPosition(type, leftBlock, leftPosition, rightBlock, rightPosition)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean positionNotDistinctFromRow(int leftBlockIndex, int leftPosition, int rightPosition, Page page, int[] rightChannels)
    {
        if (groupByUsesEqualTo) {
            return positionEqualsRow(leftBlockIndex, leftPosition, rightPosition, page, rightChannels);
        }
        for (int i = 0; i < hashChannels.size(); i++) {
            int hashChannel = hashChannels.get(i);
            Block leftBlock = channels.get(hashChannel).get(leftBlockIndex);
            Block rightBlock = page.getBlock(rightChannels[i]);
            MethodHandle methodHandle = distinctFromMethodHandles.get(hashChannel);
            try {
                if (!(boolean) methodHandle.invokeExact(leftBlock, leftPosition, rightBlock, rightPosition)) {
                    return false;
                }
            }
            catch (Throwable t) {
                throw internalError(t);
            }
        }
        return true;
    }

    @Override
    public boolean positionEqualsPosition(int leftBlockIndex, int leftPosition, int rightBlockIndex, int rightPosition)
    {
        for (int hashChannel : hashChannels) {
            Type type = types.get(hashChannel);
            List<Block> channel = channels.get(hashChannel);
            Block leftBlock = channel.get(leftBlockIndex);
            Block rightBlock = channel.get(rightBlockIndex);
            if (!TypeUtils.positionEqualsPosition(type, leftBlock, leftPosition, rightBlock, rightPosition)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean positionEqualsPositionIgnoreNulls(int leftBlockIndex, int leftPosition, int rightBlockIndex, int rightPosition)
    {
        for (int hashChannel : hashChannels) {
            Type type = types.get(hashChannel);
            List<Block> channel = channels.get(hashChannel);
            Block leftBlock = channel.get(leftBlockIndex);
            Block rightBlock = channel.get(rightBlockIndex);
            try {
                if (!type.equalTo(leftBlock, leftPosition, rightBlock, rightPosition)) {
                    return false;
                }
            }
            catch (NotSupportedException e) {
                throw new PrestoException(NOT_SUPPORTED, e.getMessage(), e);
            }
        }
        return true;
    }

    @Override
    public boolean isPositionNull(int blockIndex, int blockPosition)
    {
        for (int hashChannel : hashChannels) {
            if (isChannelPositionNull(hashChannel, blockIndex, blockPosition)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public int compareSortChannelPositions(int leftBlockIndex, int leftBlockPosition, int rightBlockIndex, int rightBlockPosition)
    {
        int channel = getSortChannel();

        Block leftBlock = channels.get(channel).get(leftBlockIndex);
        Block rightBlock = channels.get(channel).get(rightBlockIndex);

        return types.get(channel).compareTo(leftBlock, leftBlockPosition, rightBlock, rightBlockPosition);
    }

    @Override
    public boolean isSortChannelPositionNull(int blockIndex, int blockPosition)
    {
        return isChannelPositionNull(getSortChannel(), blockIndex, blockPosition);
    }

    private boolean isChannelPositionNull(int channelIndex, int blockIndex, int blockPosition)
    {
        List<Block> channel = channels.get(channelIndex);
        Block block = channel.get(blockIndex);
        return block.isNull(blockPosition);
    }

    private int getSortChannel()
    {
        return sortChannel.get();
    }
}
