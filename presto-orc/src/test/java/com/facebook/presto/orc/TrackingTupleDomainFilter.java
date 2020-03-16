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

package com.facebook.presto.orc;

import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

public interface TrackingTupleDomainFilter
        extends TupleDomainFilter
{
    class TestBigintRange
            extends BigintRange
            implements TrackingTupleDomainFilter
    {
        private final Consumer<TupleDomainFilter> onTestCallback;

        public static TestBigintRange of(BigintRange range, Consumer<TupleDomainFilter> onTestCallback)
        {
            return new TestBigintRange(range.getLower(), range.getUpper(), range.nullAllowed, onTestCallback);
        }

        private TestBigintRange(long lower, long upper, boolean nullAllowed, Consumer<TupleDomainFilter> onTestCallback)
        {
            super(lower, upper, nullAllowed);
            this.onTestCallback = requireNonNull(onTestCallback, "onTestCallback is null");
        }

        @Override
        public boolean testLong(long value)
        {
            onTestCallback.accept(this);
            return super.testLong(value);
        }
    }

    class TestDoubleRange
            extends DoubleRange
            implements TrackingTupleDomainFilter
    {
        private final Consumer<TupleDomainFilter> onTestCallback;

        public static TestDoubleRange of(DoubleRange range, Consumer<TupleDomainFilter> onTestCallback)
        {
            return new TestDoubleRange(range.getLower(), range.lowerUnbounded, range.lowerExclusive, range.getUpper(), range.upperUnbounded, range.upperExclusive, range.nullAllowed, onTestCallback);
        }

        private TestDoubleRange(double lower, boolean lowerUnbounded, boolean lowerExclusive, double upper, boolean upperUnbounded, boolean upperExclusive, boolean nullAllowed, Consumer<TupleDomainFilter> onTestCallback)
        {
            super(lower, lowerUnbounded, lowerExclusive, upper, upperUnbounded, upperExclusive, nullAllowed);
            this.onTestCallback = requireNonNull(onTestCallback, "onTestCallback is null");
        }

        @Override
        public boolean testDouble(double value)
        {
            onTestCallback.accept(this);
            return super.testDouble(value);
        }
    }
}
