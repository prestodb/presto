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

package com.facebook.presto.plugin.blackhole;

import com.facebook.presto.spi.FixedPageSource;
import com.facebook.presto.spi.Page;
import com.google.common.base.Throwables;
import io.airlift.units.Duration;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

class DelayPageSource
        extends FixedPageSource
{
    private final long pageProcessingDelayInMillis;

    public DelayPageSource(Iterable<Page> pages, Duration pageProcessingDelay)
    {
        super(pages);
        this.pageProcessingDelayInMillis = requireNonNull(pageProcessingDelay, "pageProcessingDelay is null").toMillis();
    }

    @Override
    public Page getNextPage()
    {
        if (isFinished()) {
            return null;
        }

        if (pageProcessingDelayInMillis > 0) {
            try {
                MILLISECONDS.sleep(pageProcessingDelayInMillis);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw Throwables.propagate(e);
            }
        }

        return super.getNextPage();
    }
}
