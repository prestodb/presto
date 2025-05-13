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

package com.facebook.presto.spi.statistics;

import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.openjdk.jol.info.ClassLayout;

import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.lang.Double.NaN;
import static java.lang.Double.isInfinite;
import static java.lang.Double.isNaN;

@ThriftStruct
public final class Estimate
{
    static final long ESTIMATE_SIZE = ClassLayout.parseClass(Estimate.class).instanceSize();

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

    public static Estimate estimateFromDouble(double value)
    {
        if (isNaN(value)) {
            return unknown();
        }
        return of(value);
    }

    @JsonCreator
    @ThriftConstructor
    public Estimate(@JsonProperty("value") double value)
    {
        this.value = value;
    }

    public boolean isUnknown()
    {
        return isNaN(value);
    }

    @JsonProperty
    @ThriftField(1)
    public double getValue()
    {
        return value;
    }

    /**
     * If the estimate is not an unknown value, maps the current estimate using
     * the given function.
     *
     * @param mapper mapping function
     * @return a new estimate with the mapped value
     */
    public Estimate map(Function<Double, Double> mapper)
    {
        if (!isUnknown()) {
            return Estimate.of(mapper.apply(value));
        }
        return this;
    }

    /**
     * If the estimate is not unknown, maps the existing value where the mapping
     * function should return a new estimate.
     *
     * @param mapper the mapping function
     * @return a new estimate with the mapped value
     */
    public Estimate flatMap(Function<Double, Estimate> mapper)
    {
        if (!isUnknown()) {
            return mapper.apply(value);
        }
        return this;
    }

    /**
     * If the estimate is unknown, run another function to generate an estimate
     *
     * @param supplier function to supply a new estimate
     * @return a new estimate
     */
    public Estimate or(Supplier<Estimate> supplier)
    {
        if (isUnknown()) {
            return supplier.get();
        }
        return this;
    }

    /**
     * If the estimate is unknown, run another function to generate a value
     *
     * @param supplier function to supply a new estimate
     * @return a new estimate
     */
    public double orElse(Supplier<Double> supplier)
    {
        if (isUnknown()) {
            return supplier.get();
        }
        return this.getValue();
    }

    public boolean fuzzyEquals(Estimate other, double tolerance)
    {
        if (equals(other)) {
            return true;
        }
        if (isUnknown() || other.isUnknown()) {
            return false;
        }
        return Math.copySign(value - other.value, 1.0) <= tolerance;
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
