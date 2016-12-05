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
package com.facebook.presto.orc.metadata.statistics;

import static com.google.common.base.MoreObjects.toStringHelper;

public class DateStatistics
        implements RangeStatistics<Integer>
{
    private final Integer minimum;
    private final Integer maximum;

    public DateStatistics(Integer minimum, Integer maximum)
    {
        this.minimum = minimum;
        this.maximum = maximum;
    }

    @Override
    public Integer getMin()
    {
        return minimum;
    }

    @Override
    public Integer getMax()
    {
        return maximum;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("min", minimum)
                .add("max", maximum)
                .toString();
    }
}
