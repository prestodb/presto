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
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import java.util.function.LongConsumer;

import static java.util.Objects.requireNonNull;

class RandomExchanger
        implements Consumer<Page>
{
    private final List<Consumer<PageReference>> buffers;
    private final LongConsumer memoryTracker;

    public RandomExchanger(List<Consumer<PageReference>> buffers, LongConsumer memoryTracker)
    {
        this.buffers = ImmutableList.copyOf(requireNonNull(buffers, "buffers is null"));
        this.memoryTracker = requireNonNull(memoryTracker, "memoryTracker is null");
    }

    @Override
    public void accept(Page page)
    {
        memoryTracker.accept(page.getSizeInBytes());

        PageReference pageReference = new PageReference(page, 1, () -> memoryTracker.accept(-page.getSizeInBytes()));

        int randomIndex = ThreadLocalRandom.current().nextInt(buffers.size());
        buffers.get(randomIndex).accept(pageReference);
    }
}
