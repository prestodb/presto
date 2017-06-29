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

import java.util.Optional;

public class LongStatsBuilder
        extends AbstractStatsBuilder<Long>
{
    private Long min;
    private Long max;

    @Override
    protected void setIfMin(Long value)
    {
        if (min == null || min > value) {
            min = value;
        }
    }

    @Override
    protected void setIfMax(Long value)
    {
        if (max == null || max < value) {
            max = value;
        }
    }

    @Override
    protected Optional<Long> getMin()
    {
        return Optional.ofNullable(min);
    }

    @Override
    protected Optional<Long> getMax()
    {
        return Optional.ofNullable(max);
    }

    @Override
    protected Optional<Long> getDelta()
    {
        if (min == null && max == null) {
            return Optional.empty();
        }
        long delta = max - min;
        if (delta < 0) {
            return Optional.empty();
        }
        else {
            return Optional.of(delta);
        }
    }
}
