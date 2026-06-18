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
import com.facebook.presto.common.block.Block;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * This class must be public as it is accessed via join compiler reflection.
 */
public final class JoinUtils
{
    private JoinUtils() {}

    public static List<Page> channelsToPages(List<List<Block>> channels)
    {
        if (channels.isEmpty()) {
            return ImmutableList.of();
        }

        int pagesCount = channels.get(0).size();
        ImmutableList.Builder<Page> pagesBuilder = ImmutableList.builderWithExpectedSize(pagesCount);
        for (int pageIndex = 0; pageIndex < pagesCount; ++pageIndex) {
            Block[] blocks = new Block[channels.size()];
            for (int channelIndex = 0; channelIndex < channels.size(); ++channelIndex) {
                blocks[channelIndex] = channels.get(channelIndex).get(pageIndex);
            }
            pagesBuilder.add(new Page(blocks));
        }
        return pagesBuilder.build();
    }
}
