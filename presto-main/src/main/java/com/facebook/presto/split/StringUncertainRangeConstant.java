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
package com.facebook.presto.split;

import com.facebook.presto.spi.InConstantRangePredicate.Type;

public class StringUncertainRangeConstant
    extends AbstractUncertainRangeConstant<String>
{
    public StringUncertainRangeConstant(
            String lowerLimit, boolean includeLowerBoundary,
            String upperLimit, boolean includeUpperBoundary)
    {
        super(Type.STRING,
                lowerLimit, includeLowerBoundary,
                upperLimit, includeUpperBoundary);
    }

    protected StringUncertainRangeConstant clone() {
        return new StringUncertainRangeConstant(
                lowerLimit, includeLowerBoundary,
                upperLimit, includeUpperBoundary);
    }

    protected boolean valueLessThan(String o1, String o2) {
        return o1.compareTo(o2) < 0;
    }
}
