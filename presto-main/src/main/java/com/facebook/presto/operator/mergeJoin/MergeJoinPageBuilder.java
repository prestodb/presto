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
package com.facebook.presto.operator.mergeJoin;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.PageBuilder;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.Type;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class MergeJoinPageBuilder
{
    private final List<Type> types;
    private final PageBuilder pageBuilder;
    private final long maxPositionCount = 1024;
    private final List<Integer> leftChannels;
    private final List<Integer> rightChannels;

    MergeJoinPageBuilder(
            List<Type> leftTypes,
            List<Integer> leftOutputChannels,
            List<Type> rightTypes,
            List<Integer> rightOutputChannels)
    {
        this.leftChannels = requireNonNull(leftOutputChannels, "leftOutputChannels is null");
        requireNonNull(rightOutputChannels, "rightOutputChannels is null");

        List<Type> leftOutputTypes = leftOutputChannels.stream().map(leftTypes::get).collect(toImmutableList());
        List<Type> rightOutputTypes = rightOutputChannels.stream().map(rightTypes::get).collect(toImmutableList());
        this.rightChannels = IntStream.range(0, rightOutputTypes.size()).boxed().collect(Collectors.toList());

        this.types = Stream.concat(leftOutputTypes.stream(), rightOutputTypes.stream()).collect(toImmutableList());
        this.pageBuilder = new PageBuilder(this.types);
    }

    public boolean isEmpty()
    {
        return pageBuilder.isEmpty();
    }

    public boolean isFull()
    {
        return pageBuilder.getPositionCount() >= maxPositionCount;
    }

    public Page buildAndReset()
    {
        Page page = pageBuilder.build();
        pageBuilder.reset();
        return page;
    }

    /**
     * Append left channels and right channels to page builder at their current position respectively
     * If one of the pages is empty, it will append null for its channels
     */
    public void appendRow(Optional<Page> left, int leftPostion, Optional<Page> right, int rightPosition)
    {
        checkArgument(left.isPresent() || right.isPresent(), "left and right pages cannot be empty at the the same time");
        pageBuilder.declarePosition();
        appendPosition(left, leftPostion, leftChannels, 0);
        appendPosition(right, rightPosition, rightChannels, leftChannels.size());
    }

    private void appendPosition(Optional<Page> page, int position, List<Integer> channels, int channelOffset)
    {
        for (int i = 0; i < channels.size(); i++) {
            int offset = i + channelOffset;
            BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(offset);
            if (page.isPresent()) {
                types.get(offset).appendTo(page.get().getBlock(channels.get(i)), position, blockBuilder);
            }
            else {
                blockBuilder.appendNull();
            }
        }
    }
}
