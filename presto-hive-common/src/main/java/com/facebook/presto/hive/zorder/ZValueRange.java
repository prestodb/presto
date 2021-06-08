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
package com.facebook.presto.hive.zorder;

import java.util.Optional;

/**
 * The ZValueRange class contains Optional Integer minimum and maximum values. If existing, they are both inclusive.
 * <p/>
 * Examples of converting queries into ZValueRange variables:
 * (1) "column == 5"                    -> <code> new ZValueRange(Optional.of(5), Optional.of(5)); </code>
 * (2) "column >= 0 and column <= 9"    -> <code> new ZValueRange(Optional.of(0), Optional.of(9)); </code>
 * (3) "column >= 1"                    -> <code> new ZValueRange(Optional.of(1), Optional.empty()); </code>
 * (4) no range for the column          -> <code> new ZValueRange(Optional.empty(), Optional.empty()); </code>
 */
public class ZValueRange
{
    private final Integer minimumValue;
    private final Integer maximumValue;

    public ZValueRange(Optional<Integer> minimumValue, Optional<Integer> maximumValue)
    {
        this.minimumValue = minimumValue.orElse(null);
        this.maximumValue = maximumValue.orElse(null);
    }

    public Integer getMinimumValue()
    {
        return minimumValue;
    }

    public Integer getMaximumValue()
    {
        return maximumValue;
    }
}
