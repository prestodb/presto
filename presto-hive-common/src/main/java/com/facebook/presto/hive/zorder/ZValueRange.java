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

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

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

    /**
     * Class constructor initializing <code>minimumValues</code> and <code>maximumValues</code> from optional minimum/maximum value ranges.
     * <p/>
     * A value can be set to <code>Optional.empty()</code>, or <code>null</code>, if the query is unbounded in any dimension.
     */
    public ZValueRange(List<Optional<Integer>> minimumValues, List<Optional<Integer>> maximumValues)
    {
        this.minimumValues = minimumValues.stream().map(optionalInteger -> optionalInteger.orElse(null)).collect(Collectors.toList());
        this.maximumValues = maximumValues.stream().map(optionalInteger -> optionalInteger.orElse(null)).collect(Collectors.toList());
    }

    /**
     * Sets null minimum values to the default minimum value based on <code>maximumBits</code> before returning the list.
     * The default minimum value will be <code>-(1 << maximumBits)</code>.
     * @param maximumBits the maximum amount of bits the values can have, not including the sign bit
     * @return a list of non-null minimum range values
     */
    public List<Integer> getMinimumValues(int maximumBits)
    {
        return minimumValues.stream().map(x -> (x == null) ? -(1 << maximumBits) : x).collect(Collectors.toList());
    }

    /**
     * Sets null maximum values to the default maximum value based on <code>maximumBits</code> before returning the list.
     * The default maximum value will be <code>(1 << maximumBits) - 1</code>.
     * @param maximumBits the maximum amount of bits the values can have, not including the sign bit
     * @return a list of non-null maximum range values
     */
    public List<Integer> getMaximumValues(int maximumBits)
    {
        return maximumValues.stream().map(x -> (x == null) ? (1 << maximumBits) - 1 : x).collect(Collectors.toList());
    }
}
