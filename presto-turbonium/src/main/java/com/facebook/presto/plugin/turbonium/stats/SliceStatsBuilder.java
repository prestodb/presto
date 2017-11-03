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
package com.facebook.presto.plugin.turbonium.stats;

import io.airlift.slice.Slice;

import java.util.Optional;

public class SliceStatsBuilder
        extends AbstractStatsBuilder<Slice>
{
    private Slice min;
    private Slice max;

    @Override
    protected void setIfMin(Slice value)
    {
        if (min == null || value.compareTo(min) < 0) {
            min = value;
        }
    }

    @Override
    protected void setIfMax(Slice value)
    {
        if (max == null || value.compareTo(max) > 0) {
            max = value;
        }
    }

    @Override
    protected Optional<Slice> getMin()
    {
        return Optional.ofNullable(min);
    }

    @Override
    protected Optional<Slice> getMax()
    {
        return Optional.ofNullable(max);
    }

    @Override
    protected Optional<Slice> getDelta()
    {
        return Optional.empty();
    }
}
