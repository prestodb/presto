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

public class IntStatsBuilder
        extends AbstractStatsBuilder<Integer>
{
    private Integer min;
    private Integer max;

    @Override
    protected void setIfMin(Integer value)
    {
        if (min == null || min > value) {
            min = value;
        }
    }

    @Override
    protected void setIfMax(Integer value)
    {
        if (max == null || max < value) {
            max = value;
        }
    }

    @Override
    protected Optional<Integer> getMin()
    {
        return Optional.ofNullable(min);
    }

    @Override
    protected Optional<Integer> getMax()
    {
        return Optional.ofNullable(max);
    }

    @Override
    protected Optional<Integer> getDelta()
    {
        if (min == null && max == null) {
            return Optional.empty();
        }
        int delta = max - min;
        if (delta < 0) {
            return Optional.empty();
        }
        else {
            return Optional.of(delta);
        }
    }
}
