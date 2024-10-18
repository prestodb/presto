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
package com.facebook.presto.common.predicate.vector;

import com.facebook.presto.common.predicate.TupleDomainFilter;
import jdk.incubator.vector.DoubleVector;
import jdk.incubator.vector.VectorMask;

import static jdk.incubator.vector.VectorOperators.GE;
import static jdk.incubator.vector.VectorOperators.GT;
import static jdk.incubator.vector.VectorOperators.LE;
import static jdk.incubator.vector.VectorOperators.LT;
import static jdk.incubator.vector.VectorOperators.NE;

public class DoubleRangeVector
        extends TupleDomainFilter.DoubleRange
        implements TupleDomainFilterVector
{
    protected DoubleRangeVector(double lower, boolean lowerUnbounded, boolean lowerExclusive, double upper, boolean upperUnbounded, boolean upperExclusive, boolean nullAllowed)
    {
        super(lower, lowerUnbounded, lowerExclusive, upper, upperUnbounded, upperExclusive, nullAllowed);
    }

    public static DoubleRangeVector of(double lower, boolean lowerUnbounded, boolean lowerExclusive, double upper, boolean upperUnbounded, boolean upperExclusive, boolean nullAllowed)
    {
        return new DoubleRangeVector(lower, lowerUnbounded, lowerExclusive, upper, upperUnbounded, upperExclusive, nullAllowed);
    }

    public DoubleRangeVector(DoubleRange doubleRange)
    {
        super(doubleRange.getLower(),
                doubleRange.getLowerUnbounded(),
                doubleRange.getLowerExclusive(),
                doubleRange.getUpper(),
                doubleRange.getUpperUnbounded(),
                doubleRange.getUpperExclusive(),
                doubleRange.testNull());
    }

    @Override
    public VectorMask<Double> testDoubleVector(DoubleVector values)
    {
        VectorMask<Double> passing = values.compare(NE, Double.NaN);
        if (!lowerUnbounded) {
            if (lowerExclusive) {
                passing = values.compare(GT, lower);
            }
            else {
                passing = values.compare(GE, lower);
            }
        }
        if (!upperUnbounded) {
            if (upperExclusive) {
                passing = passing.and(values.compare(LT, upper));
            }
            else {
                passing = passing.and(values.compare(LE, upper));
            }
        }
        return passing;
    }
}
