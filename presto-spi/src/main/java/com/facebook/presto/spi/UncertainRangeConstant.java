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
package com.facebook.presto.spi;

import com.facebook.presto.spi.InConstantRangePredicate.Type;

/**
 * UncertainRangeConstant represents a range of possible values.
 */
public interface UncertainRangeConstant
{
    /**
     * Type of lower limit and upper limit of range.
     * If LONG, getLowerLimit() and getUpperLimit() return Long value.
     * If STRING, getLowerLimit() and getUpperLimit() return String value.
     */
    Type getType();

    public Object getLowerLimit();

    public Object getUpperLimit();

    /**
     * Returns true if this range has lower limit.
     * If true, you can call getLowerLimit() returns non-null value.
     * If false, lower limit is the -infinite.
     */
    boolean hasLowerLimit();

    /**
     * Returns true if this range has upper limit.
     * If true, you can call getUpperLimit() returns non-null value.
     * If false, upper limit is the infinite.
     */
    boolean hasUpperLimit();

    /**
     * Returns true if the lower limit of this range includes the lower limit value itself.
     */
    boolean includesLowerBoundary();

    /**
     * Returns true if the upper limit of this range includes the upper limit value itself.
     */
    boolean includesUpperBoundary();

    /**
     * Copys this range with or without including lower boundary.
     */
    UncertainRangeConstant withLowerBoundary(boolean include);

    /**
     * Copys this range with or without including upper boundary.
     */
    UncertainRangeConstant withUpperBoundary(boolean include);

    /**
     * Returns lower bound of this range.
     *
     * For example, if this range represents [0, 1), returns (-infinite, 0)
     * For example, if this range represents (1, 2], returns (-infinite, 1]
     */
    UncertainRangeConstant lowerBound();

    /**
     * Returns upper bound of this range.
     *
     * For example, if this range represents [0, 1), returns [1, infinite)
     * For example, if this range represents (1, 2], returns (2, infinite)
     */
    UncertainRangeConstant upperBound();

    /**
     * Returns intersection of this range and another range.
     */
    UncertainRangeConstant intersection(UncertainRangeConstant other);
}
