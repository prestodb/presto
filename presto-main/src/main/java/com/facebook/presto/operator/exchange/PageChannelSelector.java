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
package com.facebook.presto.operator.exchange;

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;

import java.util.function.Function;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class PageChannelSelector
        implements Function<Page, Page>
{
    private final int[] channels;

    public PageChannelSelector(int... channels)
    {
        checkArgument(IntStream.of(channels).allMatch(channel -> channel >= 0), "channels must be positive");
        this.channels = requireNonNull(channels, "channels is null").clone();
    }

    @Override
    public Page apply(Page page)
    {
        requireNonNull(page, "page is null");

        Block[] blocks = new Block[channels.length];
        for (int i = 0; i < channels.length; i++) {
            int channel = channels[i];
            blocks[i] = page.getBlock(channel);
        }
        return new Page(page.getPositionCount(), blocks);
    }
}
