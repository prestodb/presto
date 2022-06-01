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
import com.facebook.presto.common.type.Type;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

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

    public Page build()
    {
        return pageBuilder.build();
    }

    public void reset()
    {
        pageBuilder.reset();
    }

    public void appendRow(Page left, int leftPosition, Page right, int rightPosition)
    {
        pageBuilder.declarePosition();
        appendRowPosition(left, leftPosition, leftChannels, 0);
        appendRowPosition(right, rightPosition, rightChannels, leftChannels.size());
    }

    /**
     * append the left position for the left side and append nulls for the right side
     */
    public void appendRowWithNullRight(Page left, int leftPosition)
    {
        pageBuilder.declarePosition();
        appendRowPosition(left, leftPosition, leftChannels, 0);
        appendNull(rightChannels, leftChannels.size());
    }

    /**
     * append the right position for the right side and append nulls for the left side
     */
    public void appendRowWithNullLeft(Page right, int rightPosition)
    {
        pageBuilder.declarePosition();
        appendNull(leftChannels, 0);
        appendRowPosition(right, rightPosition, rightChannels, leftChannels.size());
    }

    private void appendRowPosition(Page page, int position, List<Integer> channels, int channelOffset)
    {
        for (int i = 0; i < channels.size(); i++) {
            int offset = i + channelOffset;
            types.get(offset).appendTo(page.getBlock(channels.get(i)), position, pageBuilder.getBlockBuilder(offset));
        }
    }

    private void appendNull(List<Integer> channels, int channelOffset)
    {
        for (int i = 0; i < channels.size(); i++) {
            pageBuilder.getBlockBuilder(i + channelOffset).appendNull();
        }
    }
}
