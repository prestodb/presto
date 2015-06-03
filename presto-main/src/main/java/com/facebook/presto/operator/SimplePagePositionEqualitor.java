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
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.TypeUtils;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class SimplePagePositionEqualitor
        implements PagePositionEqualitor
{
    private final List<Type> types;
    private final List<Integer> channels;

    /**
     * @param types - types of the Page channels to equate
     * @param channels - Page channels to equate
     */
    public SimplePagePositionEqualitor(List<Type> types, List<Integer> channels)
    {
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        this.channels = ImmutableList.copyOf(requireNonNull(channels, "channels is null"));
        checkArgument(channels.size() == types.size(), "Must have the same number of channels as types");
    }

    @Override
    public int hashRow(int position, Page page)
    {
        int result = 0;
        for (int i = 0; i < channels.size(); i++) {
            Type type = types.get(i);
            int channel = channels.get(i);
            Block block = page.getBlock(channel);
            result = result * 31 + TypeUtils.hashPosition(type, block, position);
        }
        return result;
    }

    @Override
    public boolean rowEqualsRow(int leftPosition, Page leftPage, int rightPosition, Page rightPage)
    {
        for (int i = 0; i < channels.size(); i++) {
            Type type = types.get(i);
            int channel = channels.get(i);
            Block leftBlock = leftPage.getBlock(channel);
            Block rightBlock = rightPage.getBlock(channel);
            if (!TypeUtils.positionEqualsPosition(type, leftBlock, leftPosition, rightBlock, rightPosition)) {
                return false;
            }
        }
        return true;
    }
}
