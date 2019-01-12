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

package io.prestosql.spi.statistics;

import java.util.Objects;

import static java.lang.Double.NaN;
import static java.lang.Double.isInfinite;
import static java.lang.Double.isNaN;

public final class Estimate
{
    // todo eventually add some notion of statistic reliability
    //      Skipping for now as there hard to compute it properly and so far we do not have
    //      usecase for that.

    private static final Estimate UNKNOWN = new Estimate(NaN);
    private static final Estimate ZERO = new Estimate(0);

    private final double value;

    public static Estimate unknown()
    {
        return UNKNOWN;
    }

    public static Estimate zero()
    {
        return ZERO;
    }

    public static Estimate of(double value)
    {
        if (isNaN(value)) {
            throw new IllegalArgumentException("value is NaN");
        }
        if (isInfinite(value)) {
            throw new IllegalArgumentException("value is infinite");
        }
        return new Estimate(value);
    }

    private Estimate(double value)
    {
        this.value = value;
    }

    public boolean isUnknown()
    {
        return isNaN(value);
    }

    public double getValue()
    {
        return value;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Estimate estimate = (Estimate) o;
        return Double.compare(estimate.value, value) == 0;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(value);
    }

    @Override
    public String toString()
    {
        return String.valueOf(value);
    }
}
