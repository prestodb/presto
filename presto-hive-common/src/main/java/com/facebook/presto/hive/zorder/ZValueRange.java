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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * The ZValueRange class contains Optional Integer minimum and maximum value lists. If existing, they are all inclusive.
 * <p/>
 * Examples of converting queries into ZValueRange variables:
 * (1) "column == 5"                    -> <code> new ZValueRange(ImmutableList.of(Optional.of(5)), ImmutableList.of(Optional.of(5))); </code>
 * (2) "column >= 0 AND column <= 9"    -> <code> new ZValueRange(ImmutableList.of(Optional.of(0)), ImmutableList.of(Optional.of(9))); </code>
 * (3) "column >= 1"                    -> <code> new ZValueRange(ImmutableList.of(Optional.of(1)), ImmutableList.of(Optional.empty())); </code>
 * (4) no range for column              -> <code> new ZValueRange(ImmutableList.of(Optional.empty()), ImmutableList.of(Optional.empty())); </code>
 * (5) "column <= 5 OR column >= 10"    -> <code> new ZValueRange(ImmutableList.of(Optional.empty(), Optional.of(10)), ImmutableList.of(Optional.of(5), Optional.empty())); </code>
 */
public class ZValueRange
{
    private final List<Integer> minimumValues;
    private final List<Integer> maximumValues;
    public final int length;

    public ZValueRange(List<Optional<Integer>> minimumValues, List<Optional<Integer>> maximumValues)
    {
        this.minimumValues = new ArrayList<>();
        for (Optional<Integer> optionalInteger : minimumValues) {
            this.minimumValues.add(optionalInteger.orElse(null));
        }

        this.maximumValues = new ArrayList<>();
        for (Optional<Integer> optionalInteger : maximumValues) {
            this.maximumValues.add(optionalInteger.orElse(null));
        }

        length = minimumValues.size();
    }

    public List<Integer> getMinimumValues()
    {
        return minimumValues;
    }

    public List<Integer> getMaximumValues()
    {
        return maximumValues;
    }
}
